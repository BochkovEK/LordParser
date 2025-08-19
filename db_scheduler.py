import logging
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
import psycopg2
from config import (
    DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT,
    DEFAULT_PAGES, DEFAULT_YEAR, DEFAULT_DEBUG, DEFAULT_URL
)
from parser.lordfilm_parser import LordFilmParser

# Настройка логгирования
logging.basicConfig(
    level=logging.DEBUG if DEFAULT_DEBUG else logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('MovieScheduler')


def get_db_connection():
    """Создание соединения с PostgreSQL"""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


def init_db():
    """Инициализация структуры базы данных"""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Создание таблицы фильмов
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS movies (
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                year INTEGER,
                country TEXT,
                description TEXT,
                imdb_rating FLOAT,
                kp_rating FLOAT,
                link TEXT,
                rating_avg FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(title, year)
            )
        ''')

        # Создание индексов для ускорения поиска
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_movies_title ON movies(title)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_movies_year ON movies(year)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_movies_imdb ON movies(imdb_rating DESC)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_movies_kp ON movies(kp_rating DESC)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_movies_avg ON movies(rating_avg DESC)')

        conn.commit()
        logger.info("Database initialized successfully")

    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        raise
    finally:
        if conn:
            conn.close()


def save_to_db(movies_data):
    """Сохранение данных в базу"""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        inserted_count = 0
        updated_count = 0

        for movie in movies_data:
            cursor.execute('''
                INSERT INTO movies 
                (title, year, country, description, imdb_rating, kp_rating, link, rating_avg, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (title, year) 
                DO UPDATE SET
                    country = EXCLUDED.country,
                    description = EXCLUDED.description,
                    imdb_rating = EXCLUDED.imdb_rating,
                    kp_rating = EXCLUDED.kp_rating,
                    link = EXCLUDED.link,
                    rating_avg = EXCLUDED.rating_avg,
                    updated_at = EXCLUDED.updated_at
                RETURNING (xmax = 0) AS inserted
            ''', (
                movie.get('title'),
                movie.get('year'),
                movie.get('country', ''),  # Добавьте парсинг страны если нужно
                movie.get('description', ''),  # Добавьте парсинг описания если нужно
                movie.get('rating_imdb'),
                movie.get('rating_kp'),
                movie.get('link', ''),
                movie.get('rating_avg'),
                datetime.now()
            ))

            # Подсчет вставленных/обновленных записей
            if cursor.fetchone()[0]:
                inserted_count += 1
            else:
                updated_count += 1

        conn.commit()
        logger.info(f"Saved to DB: {inserted_count} new, {updated_count} updated movies")

    except Exception as e:
        logger.error(f"Error saving to database: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def scheduled_parsing():
    """Задача для периодического выполнения"""
    parser = None
    try:
        logger.info("Starting scheduled parsing...")

        # Создаем экземпляр парсера
        parser = LordFilmParser(
            base_url=DEFAULT_URL,
            year=DEFAULT_YEAR,
            debug=DEFAULT_DEBUG
        )

        # Получаем данные
        movies_data = parser._fetch_movies(DEFAULT_PAGES)

        if movies_data:
            save_to_db(movies_data)
            logger.info(f"Parsing completed successfully. Processed {len(movies_data)} movies")
        else:
            logger.warning("No movies data received from parser")

    except Exception as e:
        logger.error(f"Error during parsing: {e}")
        raise
    finally:
        # Гарантированное закрытие парсера
        if parser:
            try:
                parser.cleanup()
            except Exception as cleanup_error:
                logger.error(f"Error during parser cleanup: {cleanup_error}")


def health_check():
    """Проверка здоровья системы"""
    try:
        # Проверка БД
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM movies')
        count = cursor.fetchone()[0]
        conn.close()

        logger.info(f"Health check: DB contains {count} movies")
        return True

    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return False


if __name__ == "__main__":
    # Инициализация БД
    init_db()

    # Настройка планировщика
    scheduler = BlockingScheduler()

    # Основная задача парсинга - каждые 6 часов
    scheduler.add_job(
        scheduled_parsing,
        trigger=IntervalTrigger(hours=6),
        next_run_time=datetime.now(),
        name="movie_parsing_job",
        max_instances=1
    )

    # Health check - каждые 30 минут
    scheduler.add_job(
        health_check,
        trigger=IntervalTrigger(minutes=30),
        name="health_check_job"
    )

    try:
        logger.info("Starting Movie Scheduler...")
        logger.info(f"Will parse {DEFAULT_PAGES} pages every 6 hours")
        logger.info(f"Target year: {DEFAULT_YEAR}")
        logger.info(f"Debug mode: {DEFAULT_DEBUG}")

        scheduler.start()

    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped by user")
    except Exception as e:
        logger.error(f"Scheduler error: {e}")