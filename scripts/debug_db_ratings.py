#!/usr/bin/env python3
"""
Простой отладочный скрипт для анализа рейтингов из БД
"""
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.connection import db_manager
from src.database.models import Film


def debug_ratings_from_db():
    """Анализирует рейтинги которые уже есть в БД"""
    print("🔍 Анализ рейтингов из базы данных")
    print("=" * 60)

    db_manager.connect()
    session = db_manager.get_session()

    try:
        # Берем фильмы у которых есть какие-то данные
        test_films = session.query(Film).filter(
            Film.title.isnot(None)
        ).order_by(Film.last_updated_at.desc()).limit(5).all()

        print(f"📊 Анализируем {len(test_films)} фильмов с данными:")

        for i, film in enumerate(test_films, 1):
            print(f"\n🎬 {i}. {film.title}")
            print("-" * 40)
            print(f"   🔗 URL: {film.url}")
            print(f"   📅 Год: {film.year}")

            # Рейтинги
            ratings = []
            if film.lf_rating:
                ratings.append(f"LordFilm: {film.lf_rating} (👍{film.lf_likes or 0} 👎{film.lf_dislikes or 0})")
            if film.kp_rating:
                ratings.append(f"КиноПоиск: {film.kp_rating}")
            if film.imdb_rating:
                ratings.append(f"IMDB: {film.imdb_rating}")

            ratings_text = ' | '.join(ratings) if ratings else 'не найдены'
            print(f"   ⭐ Рейтинги: {ratings_text}")

            # Дополнительная информация
            if film.director:
                print(f"   🎭 Режиссер: {film.director}")
            if film.country:
                print(f"   🌍 Страна: {film.country}")

        print(f"\n📈 Статистика по всем фильмам в БД:")

        total_films = session.query(Film).count()
        films_with_kp = session.query(Film).filter(Film.kp_rating.isnot(None)).count()
        films_with_imdb = session.query(Film).filter(Film.imdb_rating.isnot(None)).count()

        print(f"   Всего фильмов: {total_films}")
        print(f"   С рейтингом КП: {films_with_kp} ({films_with_kp / total_films * 100:.1f}%)")
        print(f"   С рейтингом IMDB: {films_with_imdb} ({films_with_imdb / total_films * 100:.1f}%)")

    except Exception as e:
        print(f"❌ Ошибка: {e}")
    finally:
        session.close()


if __name__ == "__main__":
    debug_ratings_from_db()