#!/usr/bin/env python3
"""
Тестовый скрипт для FilmParser с выборкой URL из БД
Запуск: python scripts/test_film_parser.py
"""

import sys
import os
from typing import List, Dict, Any
from datetime import datetime, timedelta

# Добавляем корневую директорию в путь
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.parser.film_parser import FilmParser
from src.database.connection import db_manager
from src.database.models import Film, ParsingSession


def get_test_films_from_db(limit: int = 3, criteria: str = "without_details") -> List[Dict[str, Any]]:
    """
    Получает тестовые фильмы из БД с разными критериями

    Args:
        limit: количество фильмов для теста
        criteria: критерий выбора:
            - 'without_details': фильмы без распарсенных данных
            - 'recent': самые свежие добавленные
            - 'needs_update': фильмы, которые давно не обновлялись
            - 'with_errors': фильмы с ошибками в истории парсинга

    Returns:
        Список словарей с URL и информацией о фильмах
    """
    session = db_manager.get_session()

    try:
        query = session.query(Film).filter(Film.url.isnot(None))

        if criteria == "without_details":
            # Фильмы без основных метаданных (еще не парсились)
            query = query.filter(
                (Film.title.is_(None)) |
                (Film.year.is_(None)) |
                (Film.lf_rating.is_(None))
            )
            print("🎯 Критерий: фильмы без распарсенных данных")

        elif criteria == "recent":
            # Самые свежие добавленные фильмы
            query = query.order_by(Film.created_at.desc())
            print("🎯 Критерий: самые свежие фильмы")

        elif criteria == "needs_update":
            # Фильмы, которые давно не обновлялись (больше 30 дней)
            old_date = datetime.utcnow() - timedelta(days=30)
            query = query.filter(
                (Film.last_updated_at < old_date) |
                (Film.last_updated_at.is_(None))
            ).order_by(Film.last_updated_at.asc())
            print("🎯 Критерий: фильмы, требующие обновления")

        elif criteria == "with_errors":
            # Фильмы с ошибками в истории парсинга
            from src.database.models import ParsingHistory
            subquery = session.query(ParsingHistory.film_id).filter(
                ParsingHistory.success == False
            ).distinct()
            query = query.filter(Film.id.in_(subquery))
            print("🎯 Критерий: фильмы с ошибками парсинга")

        films = query.limit(limit).all()

        result = []
        for film in films:
            film_info = {
                'url': film.url,
                'id': film.id,
                'current_title': film.title,
                'current_year': film.year,
                'has_details': film.title is not None and film.year is not None,
                'last_updated': film.last_updated_at,
                'created': film.created_at
            }
            result.append(film_info)

        print(f"📊 Из БД получено {len(result)} фильмов")

        # Выводим информацию о выбранных фильмах
        for film_info in result:
            status = "✅ Есть данные" if film_info['has_details'] else "❌ Нет данных"
            print(f"   🎬 ID {film_info['id']}: {film_info['url']} ({status})")
            if film_info['current_title']:
                print(f"      📝 Текущее название: {film_info['current_title']}")

        return result

    except Exception as e:
        print(f"❌ Ошибка получения фильмов из БД: {e}")
        import traceback
        traceback.print_exc()
        return []
    finally:
        session.close()


def test_film_parser_with_db_urls():
    """Тестирует парсер с реальными URL из БД"""
    print("🎬 Тестирование FilmParser с данными из БД")
    print("=" * 50)

    # Получаем фильмы из БД (можно менять критерии)
    test_films = get_test_films_from_db(limit=3, criteria="without_details")

    if not test_films:
        print("❌ Не найдено фильмов в БД для тестирования")
        return

    test_film_urls = [film['url'] for film in test_films]

    parser = None
    db_session = None

    try:
        # Подключение к БД
        db_manager.connect()
        db_session = db_manager.get_session()

        # Создаем тестовую сессию парсинга
        test_session = ParsingSession(
            session_type="film_details_test",
            status="running"
        )
        db_session.add(test_session)
        db_session.commit()
        session_id = test_session.id
        print(f"✅ Сессия парсинга создана: ID {session_id}")

        # Инициализация парсера
        print("🚀 Инициализация FilmParser...")
        parser = FilmParser()
        parser.setup_driver()
        print("✅ WebDriver запущен")

        # Тестируем парсинг отдельных фильмов
        print(f"\n🔍 Парсим {len(test_films)} фильмов из БД...")

        for i, film_info in enumerate(test_films, 1):
            print(f"\n--- Фильм {i}/{len(test_films)} ---")
            print(f"📝 URL: {film_info['url']}")
            print(f"🆔 ID в БД: {film_info['id']}")

            if film_info['has_details']:
                print(f"📊 В БД уже есть: {film_info['current_title']} ({film_info['current_year']})")

            try:
                # Парсим метаданные
                film_data = parser.parse_film_details(film_info['url'])

                if 'error' in film_data:
                    print(f"❌ Ошибка парсинга: {film_data['error']}")
                    continue

                # Выводим результаты
                print(f"✅ Успешно распарсено:")
                print(f"   🎭 Название: {film_data.get('title', 'N/A')}")
                print(f"   🎬 Оригинал: {film_data.get('original_title', 'N/A')}")
                print(f"   📅 Год: {film_data.get('year', 'N/A')}")

                # Рейтинги
                print(f"   ⭐ Рейтинги:")
                print(
                    f"     LF: {film_data.get('lf_rating', 'N/A')} (👍{film_data.get('lf_likes', 'N/A')} 👎{film_data.get('lf_dislikes', 'N/A')})")
                print(f"     KP: {film_data.get('kp_rating', 'N/A')}")
                print(f"     IMDB: {film_data.get('imdb_rating', 'N/A')}")

                # Тестируем сохранение в БД
                print("💾 Сохраняем в БД...")
                success = parser.update_film_in_db(film_data, session_id)
                if success:
                    print("✅ Данные сохранены в БД")
                else:
                    print("❌ Ошибка сохранения в БД")

            except Exception as e:
                print(f"❌ Ошибка при парсинге: {e}")
                continue

        # Тестируем пакетную обработку
        print(f"\n📦 Тестируем пакетную обработку...")
        stats = parser.parse_films_batch(test_film_urls, session_id, batch_size=2)

        print(f"\n📊 Статистика пакетной обработки:")
        print(f"   Обработано фильмов: {stats['films_processed']}")
        print(f"   Обновлено в БД: {stats['films_updated']}")
        print(f"   Ошибок: {stats['errors']}")

        # Завершаем сессию
        test_session.status = "completed"
        test_session.films_processed = stats['films_processed']
        db_session.commit()

        print(f"\n✅ Тестирование завершено успешно!")

    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Очистка ресурсов
        if parser:
            parser.close()
            print("🔚 WebDriver закрыт")
        if db_session:
            db_session.close()
            print("🔚 Сессия БД закрыта")


def show_db_stats():
    """Показывает статистику БД"""
    print("\n📊 Статистика базы данных:")
    print("=" * 30)

    session = db_manager.get_session()
    try:
        # Общее количество фильмов
        total_films = session.query(Film).count()
        films_with_details = session.query(Film).filter(
            Film.title.isnot(None),
            Film.year.isnot(None)
        ).count()
        films_without_details = total_films - films_with_details

        print(f"🎬 Всего фильмов: {total_films}")
        print(f"✅ С распарсенными данными: {films_with_details}")
        print(f"❌ Без данных: {films_without_details}")

        # Фильмы по годам (топ 5)
        years_stats = session.query(Film.year).filter(
            Film.year.isnot(None)
        ).group_by(Film.year).order_by(Film.year.desc()).limit(5).all()

        if years_stats:
            print(f"📅 Последние годы: {[year[0] for year in years_stats]}")

    except Exception as e:
        print(f"❌ Ошибка получения статистики: {e}")
    finally:
        session.close()


if __name__ == "__main__":
    # Показываем статистику БД
    show_db_stats()

    # Основной тест
    test_film_parser_with_db_urls()