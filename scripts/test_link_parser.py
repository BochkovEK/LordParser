#!/usr/bin/env python3
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.parser.link_parser import LinkParser
from src.parser.url_generator import URLGenerator


def main():
    print("🎯 Тест парсера ссылок")
    print("=" * 50)

    # Генерируем тестовые URL
    generator = URLGenerator()
    test_urls = list(generator.generate_daily_urls())[:2]  # Первые 2 страницы

    print(f"🔗 Тестовые URL: {len(test_urls)} страниц")

    # Тестируем парсер
    parser = LinkParser()

    try:
        # Создаем тестовую сессию в БД
        from src.database.connection import db_manager
        from src.database.models import ParsingSession

        db_manager.connect()
        session = db_manager.get_session()

        test_session = ParsingSession(
            session_type="test",
            years_parsed="2025",
            pages_per_year=2,
            status="running"
        )
        session.add(test_session)
        session.commit()
        session_id = test_session.id

        print(f"📊 Сессия парсинга создана: ID {session_id}")

        # Парсим тестовые страницы
        stats = parser.parse_links_batch(test_urls, session_id, batch_size=10)

        print(f"\n📈 Результаты парсинга:")
        print(f"   Обработано страниц: {stats['pages_processed']}")
        print(f"   Найдено ссылок: {stats['links_found']}")
        print(f"   Новых фильмов: {stats['new_films']}")
        print(f"   Ошибок: {stats['errors']}")

        # Обновляем сессию
        test_session.status = "completed"
        test_session.links_found = stats['links_found']
        test_session.new_films_added = stats['new_films']
        session.commit()

        print("✅ Парсер ссылок работает корректно!")

    except Exception as e:
        print(f"❌ Ошибка: {e}")
    finally:
        parser.close()
        session.close()


if __name__ == "__main__":
    main()