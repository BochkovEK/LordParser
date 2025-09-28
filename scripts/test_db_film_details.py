#!/usr/bin/env python3
"""
Тестовый скрипт для проверки детальной информации о фильмах в БД
"""
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.connection import db_manager
from src.database.models import Film, ParsingHistory


def test_films_with_details():
    """Показывает фильмы с заполненными деталями"""
    print("🎬 Тест детальной информации о фильмах")
    print("=" * 60)

    session = db_manager.get_session()

    try:
        # Фильмы с заполненными данными (отсортированы по последнему обновлению)
        detailed_films = session.query(Film).filter(
            Film.title.isnot(None)
        ).order_by(Film.last_updated_at.desc()).limit(10).all()

        print(f"📊 Фильмов с деталями в БД: {len(detailed_films)}")

        if detailed_films:
            print(f"\n🎭 Последние {len(detailed_films)} обработанных фильмов:")
            for i, film in enumerate(detailed_films, 1):
                print(f"\n  {i}. 🎬 {film.title}")
                print(f"     🔗 URL: {film.url}")
                print(f"     📅 Год: {film.year}")
                print(f"     🌍 Страна: {film.country}")
                print(f"     🔤 Оригинал: {film.original_title}")
                print(f"     🎭 Режиссер: {film.director}")

                if film.categories:
                    print(f"     📊 Категории: {', '.join(film.categories)}")

                # Рейтинги
                ratings = []
                if film.lf_rating:
                    ratings.append(f"LordFilm: {film.lf_rating} (👍{film.lf_likes or 0} 👎{film.lf_dislikes or 0})")
                if film.kp_rating:
                    ratings.append(f"КП: {film.kp_rating}")
                if film.imdb_rating:
                    ratings.append(f"IMDB: {film.imdb_rating}")

                if ratings:
                    print(f"     ⭐ Рейтинги: {' | '.join(ratings)}")

                if film.actors:
                    print(f"     🎭 Актеры: {', '.join(film.actors[:3])}...")

                if film.description:
                    desc = film.description[:100] + "..." if len(film.description) > 100 else film.description
                    print(f"     📝 Описание: {desc}")

                print(f"     ⏰ Обновлен: {film.last_updated_at}")

        else:
            print("❌ Нет фильмов с заполненными деталями")

    except Exception as e:
        print(f"❌ Ошибка при чтении фильмов: {e}")
    finally:
        session.close()


def test_films_without_details():
    """Показывает фильмы без детальной информации"""
    print("\n🎯 Фильмы без детальной информации")
    print("=" * 50)

    session = db_manager.get_session()

    try:
        # Фильмы без данных (только ссылки)
        empty_films = session.query(Film).filter(
            Film.title.is_(None)
        ).order_by(Film.first_seen_at.desc()).limit(10).all()

        print(f"📊 Фильмов без деталей: {len(empty_films)}")

        if empty_films:
            print(f"\n🔗 Последние {len(empty_films)} необработанных фильмов:")
            for i, film in enumerate(empty_films, 1):
                print(f"  {i}. {film.url}")
                print(f"     📅 Добавлен: {film.first_seen_at}")

        else:
            print("✅ Все фильмы обработаны!")

    except Exception as e:
        print(f"❌ Ошибка при чтении фильмов: {e}")
    finally:
        session.close()


def test_parsing_statistics():
    """Показывает статистику парсинга"""
    print("\n📈 Статистика парсинга")
    print("=" * 50)

    session = db_manager.get_session()

    try:
        # Общая статистика
        total_films = session.query(Film).count()
        films_with_details = session.query(Film).filter(Film.title.isnot(None)).count()
        films_without_details = total_films - films_with_details

        print(f"📊 Всего фильмов: {total_films}")
        print(f"✅ С деталями: {films_with_details} ({films_with_details / total_films * 100:.1f}%)")
        print(f"⏳ Без деталей: {films_without_details} ({films_without_details / total_films * 100:.1f}%)")

        # Статистика по рейтингам
        films_with_lf_rating = session.query(Film).filter(Film.lf_rating.isnot(None)).count()
        films_with_kp_rating = session.query(Film).filter(Film.kp_rating.isnot(None)).count()
        films_with_imdb_rating = session.query(Film).filter(Film.imdb_rating.isnot(None)).count()

        print(f"\n⭐ Рейтинги:")
        print(f"   LordFilm: {films_with_lf_rating} фильмов")
        print(f"   КиноПоиск: {films_with_kp_rating} фильмов")
        print(f"   IMDB: {films_with_imdb_rating} фильмов")

        # Последние сессии парсинга
        from src.database.models import ParsingSession
        recent_sessions = session.query(ParsingSession).order_by(
            ParsingSession.started_at.desc()
        ).limit(3).all()

        print(f"\n🕒 Последние сессии парсинга:")
        for sess in recent_sessions:
            status_icon = "✅" if sess.status == "completed" else "🔄" if sess.status == "running" else "❌"
            print(f"   {status_icon} {sess.session_type}: {sess.started_at}")
            if sess.links_found:
                print(f"      Ссылок: {sess.links_found}, Новых: {sess.new_films_added}")
            if sess.films_processed:
                print(f"      Фильмов: {sess.films_processed}, Обновлено: {sess.existing_films_updated}")

    except Exception as e:
        print(f"❌ Ошибка при анализе статистики: {e}")
    finally:
        session.close()


def test_recent_parsing_history():
    """Показывает последние записи истории парсинга"""
    print("\n📋 История парсинга")
    print("=" * 50)

    session = db_manager.get_session()

    try:
        recent_history = session.query(ParsingHistory).order_by(
            ParsingHistory.parsed_at.desc()
        ).limit(10).all()

        print(f"📊 Последние {len(recent_history)} записей:")

        for i, record in enumerate(recent_history, 1):
            film_title = record.film.title if record.film and record.film.title else "Unknown"
            status_icon = "✅" if record.success else "❌"

            print(f"  {i}. {status_icon} {record.parsing_type}: {film_title}")
            print(f"     🕒 {record.parsed_at}")

            if record.error_message:
                print(f"     💥 Ошибка: {record.error_message}")

    except Exception as e:
        print(f"❌ Ошибка при чтении истории: {e}")
    finally:
        session.close()


def main():
    print("🔍 Детальная проверка базы данных фильмов")
    print("=" * 70)

    # Подключаемся к БД
    try:
        db_manager.connect()
        print("✅ Подключение к БД успешно")
    except Exception as e:
        print(f"❌ Ошибка подключения к БД: {e}")
        return

    # Запускаем тесты
    test_films_with_details()
    test_films_without_details()
    test_parsing_statistics()
    test_recent_parsing_history()

    print(f"\n🎉 Проверка завершена!")


if __name__ == "__main__":
    main()