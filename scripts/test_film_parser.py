#!/usr/bin/env python3
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.parser.film_parser import FilmParser
from src.database.connection import db_manager
from src.database.models import Film, ParsingSession


def main():
    print("🎬 Тест парсера фильмов")
    print("=" * 50)

    # Получаем тестовые URL из БД
    db_manager.connect()
    session = db_manager.get_session()

    try:
        # Берем несколько фильмов из БД для теста
        test_films = session.query(Film).filter(Film.title.is_(None)).limit(3).all()

        if not test_films:
            print("❌ Нет фильмов для теста (все уже имеют данные)")
            # Берем любые 3 фильма
            test_films = session.query(Film).limit(3).all()

        test_urls = [film.url for film in test_films]
        print(f"🔗 Тестовые URL: {len(test_urls)} фильмов")
        for url in test_urls:
            print(f"   - {url}")

        # Создаем тестовую сессию
        test_session = ParsingSession(
            session_type="test_films",
            years_parsed="test",
            pages_per_year=0,
            status="running"
        )
        session.add(test_session)
        session.commit()
        session_id = test_session.id

        print(f"📊 Сессия парсинга создана: ID {session_id}")

        # Тестируем парсер
        parser = FilmParser()

        # Парсим тестовые фильмы
        stats = parser.parse_films_batch(test_urls, session_id, batch_size=2)

        print(f"\n📈 Результаты парсинга:")
        print(f"   Обработано фильмов: {stats['films_processed']}")
        print(f"   Обновлено в БД: {stats['films_updated']}")
        print(f"   Ошибок: {stats['errors']}")

        # Обновляем сессию
        test_session.status = "completed"
        test_session.films_processed = stats['films_processed']
        test_session.existing_films_updated = stats['films_updated']
        session.commit()

        print("✅ Парсер фильмов работает корректно!")

    except Exception as e:
        print(f"❌ Ошибка: {e}")
    finally:
        session.close()


if __name__ == "__main__":
    main()