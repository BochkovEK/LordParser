#!/usr/bin/env python3
"""
Debug скрипт для анализа стратегий поиска LordFilm рейтинга
"""
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.parser.film_parser import FilmParser
from src.database.connection import db_manager
from src.database.models import Film


def debug_lf_ratings():
    """Анализирует стратегии поиска LordFilm рейтинга"""
    print("🔍 Анализ стратегий поиска LordFilm рейтинга")
    print("=" * 70)

    db_manager.connect()
    session = db_manager.get_session()

    try:
        # Берем фильмы для теста
        test_films = session.query(Film).filter(
            Film.title.isnot(None)
        ).order_by(Film.last_updated_at.desc()).limit(5).all()

        print(f"📊 Тестируем на {len(test_films)} фильмах:")

        parser = FilmParser()

        for i, film in enumerate(test_films, 1):
            print(f"\n🎬 {i}. {film.title}")
            print(f"🔗 {film.url}")
            print("-" * 50)

            # Используем стандартный метод парсинга из film_parser
            film_data = parser.parse_film_details(film.url)

            if 'error' in film_data:
                print(f"💥 Ошибка парсинга: {film_data['error']}")
                continue

            # Анализируем результаты
            lf_rating = film_data.get('lf_rating')
            lf_likes = film_data.get('lf_likes')
            lf_dislikes = film_data.get('lf_dislikes')

            if lf_rating is not None:
                print(f"✅ LordFilm рейтинг: {lf_rating}")
                print(f"   👍 Лайки: {lf_likes}")
                print(f"   👎 Дизлайки: {lf_dislikes}")
            else:
                print(f"❌ LordFilm рейтинг: не найден")

            # Дополнительная информация для дебага
            print(f"\n📊 Дополнительная информация:")
            print(f"   Название: {film_data.get('title')}")
            print(f"   КП рейтинг: {film_data.get('kp_rating')}")
            print(f"   IMDB рейтинг: {film_data.get('imdb_rating')}")

            # Если рейтинг не найден, можем добавить дополнительную диагностику
            if lf_rating is None:
                print(f"🔍 Диагностика: нужно проверить методы _extract_lf_rating в film_parser")

            print()

        # Статистика
        print("\n📈 СТАТИСТИКА:")
        successful = sum(1 for film in test_films if parser.parse_film_details(film.url).get('lf_rating') is not None)
        print(f"   Успешных поисков LordFilm рейтинга: {successful}/{len(test_films)}")

    except Exception as e:
        print(f"❌ Общая ошибка: {e}")
    finally:
        parser.close()
        session.close()


if __name__ == "__main__":
    debug_lf_ratings()