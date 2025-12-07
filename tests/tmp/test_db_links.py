#!/usr/bin/env python3
"""
Тестовый скрипт для проверки сохраненных ссылок в БД
"""
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.connection import db_manager
from src.database.models import Film, ParsingSession, ParsingHistory


def test_films_table():
    """Тестирует таблицу films"""
    print("🎯 Тест таблицы films")
    print("=" * 50)

    session = db_manager.get_session()

    try:
        # Получаем общее количество фильмов
        total_films = session.query(Film).count()
        print(f"📊 Всего фильмов в БД: {total_films}")

        if total_films > 0:
            # Получаем последние 10 фильмов
            recent_films = session.query(Film).order_by(Film.id.desc()).limit(10).all()

            print(f"\n📝 Последние {len(recent_films)} фильмов:")
            for i, film in enumerate(recent_films, 1):
                print(f"  {i}. ID: {film.id}")
                print(f"     URL: {film.url}")
                print(f"     Активен: {film.is_active}")
                print(f"     Первый раз: {film.first_seen_at}")
                print(f"     Обновлен: {film.last_updated_at}")
                print(f"     Создан: {film.created_at}")
                print()

        else:
            print("❌ В таблице films нет данных")

    except Exception as e:
        print(f"❌ Ошибка при чтении films: {e}")
    finally:
        session.close()


def test_parsing_sessions():
    """Тестирует таблицу parsing_sessions"""
    print("\n🎯 Тест таблицы parsing_sessions")
    print("=" * 50)

    session = db_manager.get_session()

    try:
        # Получаем все сессии
        sessions = session.query(ParsingSession).order_by(ParsingSession.id.desc()).all()

        print(f"📊 Всего сессий парсинга: {len(sessions)}")

        for i, sess in enumerate(sessions, 1):
            print(f"  {i}. ID: {sess.id}")
            print(f"     Тип: {sess.session_type}")
            print(f"     Годы: {sess.years_parsed}")
            print(f"     Страниц: {sess.pages_per_year}")
            print(f"     Статус: {sess.status}")
            print(f"     Найдено ссылок: {sess.links_found}")
            print(f"     Новых фильмов: {sess.new_films_added}")
            print(f"     Начало: {sess.started_at}")
            print(f"     Завершение: {sess.completed_at}")
            print()

    except Exception as e:
        print(f"❌ Ошибка при чтении parsing_sessions: {e}")
    finally:
        session.close()


def test_parsing_history():
    """Тестирует таблицу parsing_history"""
    print("\n🎯 Тест таблицы parsing_history")
    print("=" * 50)

    session = db_manager.get_session()

    try:
        # Получаем последние 10 записей истории
        history = session.query(ParsingHistory).order_by(ParsingHistory.id.desc()).limit(10).all()

        print(f"📊 Последние {len(history)} записей истории:")

        for i, record in enumerate(history, 1):
            print(f"  {i}. ID: {record.id}")
            print(f"     Фильм ID: {record.film_id}")
            print(f"     Сессия ID: {record.session_id}")
            print(f"     Тип: {record.parsing_type}")
            print(f"     Успех: {record.success}")
            print(f"     Время: {record.parsed_at}")

            if record.error_message:
                print(f"     Ошибка: {record.error_message}")
            print()

    except Exception as e:
        print(f"❌ Ошибка при чтении parsing_history: {e}")
    finally:
        session.close()


def test_film_stats():
    """Показывает статистику по фильмам"""
    print("\n🎯 Статистика по фильмам")
    print("=" * 50)

    session = db_manager.get_session()

    try:
        # Группировка по доменам
        from sqlalchemy import func

        # Извлекаем домены из URL
        films = session.query(Film.url).all()

        domains = {}
        for (url,) in films:
            if 'lordfilm' in url:
                # Извлекаем путь после домена
                parts = url.split('/filmy/')
                if len(parts) > 1:
                    category = parts[1].split('/')[0] if '/' in parts[1] else parts[1]
                    domains[category] = domains.get(category, 0) + 1

        print("📊 Распределение по категориям:")
        for category, count in sorted(domains.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"  {category}: {count} фильмов")

        # Активные/неактивные
        active_count = session.query(Film).filter(Film.is_active == True).count()
        inactive_count = session.query(Film).filter(Film.is_active == False).count()

        print(f"\n📊 Активность:")
        print(f"  Активных: {active_count}")
        print(f"  Неактивных: {inactive_count}")

    except Exception as e:
        print(f"❌ Ошибка при анализе статистики: {e}")
    finally:
        session.close()


def main():
    print("🔍 Тестирование базы данных")
    print("=" * 60)

    # Подключаемся к БД
    try:
        db_manager.connect()
        print("✅ Подключение к БД успешно")
    except Exception as e:
        print(f"❌ Ошибка подключения к БД: {e}")
        return

    # Запускаем тесты
    test_films_table()
    test_parsing_sessions()
    test_parsing_history()
    test_film_stats()

    print("✅ Все тесты завершены")


if __name__ == "__main__":
    main()