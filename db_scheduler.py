import logging
import sys
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
import psycopg2
import time
from config import (
    DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT,
    DEFAULT_DEBUG, DEFAULT_URL,
    PARSE_PAGES, YEAR_RANGE
)
from parser.lordfilm_parser import LordFilmParser

# Настройка логгирования
logging.basicConfig(
    level=logging.DEBUG if DEFAULT_DEBUG else logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    # stream=sys.stdout  # Явно указываем вывод в stdout
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


def parse_without_year_generator():
    """Генератор для парсинга без указания года"""
    parser = None
    try:
        parser = LordFilmParser(
            base_url=DEFAULT_URL,
            year=None,
            debug=DEFAULT_DEBUG
        )

        # Получаем данные для указанного количества страниц
        movies_data = parser._fetch_movies(PARSE_PAGES)

        if movies_data:
            logger.info(f"Parsing without year: yielding {len(movies_data)} movies")
            for movie in movies_data:
                yield movie
        else:
            logger.info("No movies found without year filter")

    except Exception as e:
        logger.error(f"Error parsing without year: {e}")
        raise
    finally:
        if parser:
            try:
                parser.cleanup()
            except Exception as cleanup_error:
                logger.error(f"Error during parser cleanup: {cleanup_error}")


def parse_year_range_generator():
    """Генератор для парсинга диапазона годов"""
    start_year, end_year = map(int, YEAR_RANGE.split('-'))

    for year in range(start_year, end_year + 1):
        logger.info(f"Parsing year: {year}")

        parser = None
        try:
            parser = LordFilmParser(
                base_url=DEFAULT_URL,
                year=year,
                debug=DEFAULT_DEBUG
            )

            # Получаем данные для указанного количества страниц
            movies_data = parser._fetch_movies(PARSE_PAGES)

            if movies_data:
                logger.info(f"Year {year}: yielding {len(movies_data)} movies")
                for movie in movies_data:
                    yield movie
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
        time.sleep(2)


def save_single_batch(movies_batch):
    """Сохранение одной пачки фильмов в базу"""
    if not movies_batch:
        return 0, 0

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        inserted_count = 0
        updated_count = 0

        for movie in movies_batch:
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

            if cursor.fetchone()[0]:
                inserted_count += 1
            else:
                updated_count += 1

        conn.commit()
        return inserted_count, updated_count

    except Exception as e:
        logger.error(f"Error saving batch to database: {e}")
        if conn:
            conn.rollback()
        return 0, 0
    finally:
        if conn:
            conn.close()


def process_generator_with_batches(data_generator, batch_size=100, context=""):
    """Обработка генератора с сохранением пачками"""
    total_inserted = 0
    total_updated = 0
    current_batch = []
    batch_count = 0

    try:
        for movie in data_generator:
            current_batch.append(movie)

            # Когда накопили пачку - сохраняем
            if len(current_batch) >= batch_size:
                batch_count += 1
                inserted, updated = save_single_batch(current_batch)
                total_inserted += inserted
                total_updated += updated

                logger.info(
                    f"Batch {batch_count} ({context}): "
                    f"saved {inserted} new, {updated} updated movies "
                    f"(total: {total_inserted + total_updated})"
                )

                # Очищаем пачку и делаем небольшую паузу
                current_batch = []
                time.sleep(0.1)

        # Сохраняем последнюю неполную пачку
        if current_batch:
            batch_count += 1
            inserted, updated = save_single_batch(current_batch)
            total_inserted += inserted
            total_updated += updated

            logger.info(
                f"Final batch {batch_count} ({context}): "
                f"saved {inserted} new, {updated} updated movies"
            )

        logger.info(
            f"Completed {context}: {total_inserted} new, "
            f"{total_updated} updated movies total"
        )

        return total_inserted + total_updated

    except Exception as e:
        # Пытаемся сохранить то, что успели набрать
        if current_batch:
            try:
                inserted, updated = save_single_batch(current_batch)
                total_inserted += inserted
                total_updated += updated
                logger.info(f"Saved partial batch due to error: {inserted + updated} movies")
            except Exception as save_error:
                logger.error(f"Error saving final batch: {save_error}")

        raise


def full_parsing_job():
    """Полная задача парсинга с использованием генераторов"""
    total_saved = 0
    batch_size = 100

    try:
        logger.info("Starting full parsing job with generators...")

        # 1. Очищаем базу
        clear_database()

        # 2. Парсим без указания года
        logger.info("Processing movies without year filter...")
        saved = process_generator_with_batches(
            parse_without_year_generator(),
            batch_size=batch_size,
            context="without year"
        )
        total_saved += saved

        # 3. Парсим по годам из диапазона
        logger.info("Processing year range...")
        saved = process_generator_with_batches(
            parse_year_range_generator(),
            batch_size=batch_size,
            context="year range"
        )
        total_saved += saved

        logger.info(f"Full parsing completed. Total movies saved: {total_saved}")

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
            for year, count in year_stats[:10]:
                logger.info(f"  {year}: {count} movies")

        return True

    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return False


if __name__ == "__main__":
    # Инициализация БД
    init_db()

    # Немедленный запуск для тестирования
    # logger.info("=== STARTING IMMEDIATE TEST RUN ===")
    # full_parsing_job()
    # logger.info("=== TEST RUN COMPLETED ===")

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
        logger.info("Starting Movie Scheduler with Generators...")
        logger.info(f"Year range: {YEAR_RANGE}")
        logger.info(f"Pages per parsing: {PARSE_PAGES}")
        logger.info(f"Batch size: 100 movies")
        logger.info("Full parsing will run every 24 hours")

        scheduler.start()

    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped by user")
    except Exception as e:
        logger.error(f"Scheduler error: {e}")