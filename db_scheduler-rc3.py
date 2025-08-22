import logging
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
import psycopg2
from config import (
    DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT,
    DEFAULT_PAGES, DEFAULT_YEAR, DEFAULT_DEBUG, DEFAULT_URL,
    PARSE_PAGES, YEAR_RANGE
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


def clear_database():
    """Полная очистка базы данных"""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Удаляем все данные из таблицы
        cursor.execute('TRUNCATE TABLE movies RESTART IDENTITY CASCADE')

        conn.commit()
        logger.info("Database cleared successfully")

    except Exception as e:
        logger.error(f"Error clearing database: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def parse_without_year():
    """Парсинг без указания года (все фильмы подряд)"""
    parser = None
    try:
        # Создаем экземпляр парсера без указания года
        parser = LordFilmParser(
            base_url=DEFAULT_URL,
            year=None,  # Без фильтра по году
            debug=DEFAULT_DEBUG
        )

        # Получаем данные для указанного количества страниц
        movies_data = parser._fetch_movies(PARSE_PAGES)

        logger.info(f"Parsing without year: found {len(movies_data) if movies_data else 0} movies")
        return movies_data

    except Exception as e:
        logger.error(f"Error parsing without year: {e}")
        return []
    finally:
        if parser:
            try:
                parser.cleanup()
            except Exception as cleanup_error:
                logger.error(f"Error during parser cleanup: {cleanup_error}")


def parse_year_range():
    """Парсинг для каждого года в диапазоне"""
    all_movies_data = []

    # Парсим диапазон годов
    start_year, end_year = map(int, YEAR_RANGE.split('-'))

    for year in range(start_year, end_year + 1):
        logger.info(f"Parsing year: {year}")

        parser = None
        try:
            # Создаем экземпляр парсера для конкретного года
            parser = LordFilmParser(
                base_url=DEFAULT_URL,
                year=year,
                debug=DEFAULT_DEBUG
            )

            # Получаем данные для указанного количества страниц
            year_movies = parser._fetch_movies(PARSE_PAGES)

            if year_movies:
                all_movies_data.extend(year_movies)
                logger.info(f"Year {year}: found {len(year_movies)} movies")
            else:
                logger.warning(f"Year {year}: no movies found")

        except Exception as e:
            logger.error(f"Error parsing year {year}: {e}")
        finally:
            if parser:
                try:
                    parser.cleanup()
                except Exception as cleanup_error:
                    logger.error(f"Error during parser cleanup for year {year}: {cleanup_error}")

        # Небольшая задержка между годами
        import time
        time.sleep(2)

    return all_movies_data


def save_to_db(movies_data):
    """Сохранение данных в базу"""
    if not movies_data:
        logger.warning("No movies data to save")
        return

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
                movie.get('country', ''),
                movie.get('description', ''),
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


# def full_parsing_job():
#     """Полная задача парсинга с очисткой базы"""
#     all_movies_data = []
#
#     try:
#         logger.info("Starting full parsing job...")
#         logger.info(f"Year range: {YEAR_RANGE}")
#         logger.info(f"Pages per year: {PARSE_PAGES}")
#
#         # 1. Очищаем базу
#         clear_database()
#
#         # 2. Парсим без указания года (все фильмы подряд)
#         logger.info("Parsing without year filter...")
#         movies_without_year = parse_without_year()
#         if movies_without_year:
#             all_movies_data.extend(movies_without_year)
#             logger.info(f"Found {len(movies_without_year)} movies without year filter")
#
#         # 3. Парсим по годам из диапазона
#         logger.info("Parsing year range...")
#         movies_by_year = parse_year_range()
#         if movies_by_year:
#             all_movies_data.extend(movies_by_year)
#             logger.info(f"Found {len(movies_by_year)} movies by year range")
#
#         # 4. Сохраняем в базу
#         if all_movies_data:
#             save_to_db(all_movies_data)
#             logger.info(f"Full parsing completed. Total movies: {len(all_movies_data)}")
#
#             # Статистика по годам
#             year_stats = {}
#             for movie in all_movies_data:
#                 year = movie.get('year')
#                 if year:
#                     year_stats[year] = year_stats.get(year, 0) + 1
#
#             logger.info("Movies by year:")
#             for year, count in sorted(year_stats.items()):
#                 logger.info(f"  {year}: {count} movies")
#
#         else:
#             logger.warning("No movies data received from parsing")
#
#     except Exception as e:
#         logger.error(f"Error during full parsing job: {e}")
#         raise

def full_parsing_job():
    """Полная задача парсинга с очисткой базы"""
    all_movies_data = []

    try:
        logger.info("Starting full parsing job...")

        # 1. Очищаем базу
        clear_database()

        # 2. Парсим без указания года
        logger.info("Parsing without year filter...")
        movies_without_year = parse_without_year()
        if movies_without_year:
            all_movies_data.extend(movies_without_year)
            logger.info(f"Found {len(movies_without_year)} movies without year filter")

        # 3. Парсим по годам из диапазона
        logger.info("Parsing year range...")
        movies_by_year = parse_year_range()
        if movies_by_year:
            all_movies_data.extend(movies_by_year)
            logger.info(f"Found {len(movies_by_year)} movies by year range")

        # 4. Сохраняем в базу (автоматически выбирается режим)
        if all_movies_data:
            logger.info(f"Starting save process for {len(all_movies_data)} movies...")
            save_to_db(all_movies_data, batch_size=100)  # ← Вот здесь магия!

            # Статистика
            year_stats = {}
            for movie in all_movies_data:
                year = movie.get('year')
                if year:
                    year_stats[year] = year_stats.get(year, 0) + 1

            logger.info("Movies by year:")
            for year, count in sorted(year_stats.items()):
                logger.info(f"  {year}: {count} movies")

        else:
            logger.warning("No movies data received from parsing")

    except Exception as e:
        logger.error(f"Error during full parsing job: {e}")
        raise


def health_check():
    """Проверка здоровья системы"""
    try:
        # Проверка БД
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM movies')
        count = cursor.fetchone()[0]

        # Статистика по годам
        cursor.execute('''
            SELECT year, COUNT(*) 
            FROM movies 
            WHERE year IS NOT NULL 
            GROUP BY year 
            ORDER BY year DESC
        ''')
        year_stats = cursor.fetchall()

        conn.close()

        logger.info(f"Health check: DB contains {count} movies")
        if year_stats:
            logger.info("Movies by year in DB:")
            for year, count in year_stats[:10]:  # Показываем топ-10 годов
                logger.info(f"  {year}: {count} movies")

        return True

    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return False


if __name__ == "__main__":
    # Инициализация БД
    init_db()

    # Настройка планировщика
    scheduler = BlockingScheduler()

    # Основная задача - полный парсинг каждые 24 часа
    scheduler.add_job(
        full_parsing_job,
        trigger=IntervalTrigger(hours=24),
        next_run_time=datetime.now(),
        name="full_parsing_job",
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
        logger.info(f"Year range: {YEAR_RANGE}")
        logger.info(f"Pages per parsing: {PARSE_PAGES}")
        logger.info("Full parsing will run every 24 hours")
        logger.info("Includes: 1) All movies without year filter 2) Movies by year range")

        scheduler.start()

    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped by user")
    except Exception as e:
        logger.error(f"Scheduler error: {e}")