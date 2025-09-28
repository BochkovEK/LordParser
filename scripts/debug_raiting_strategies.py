#!/usr/bin/env python3
"""
Debug скрипт для анализа эффективности стратегий поиска рейтингов
"""
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.parser.film_parser import FilmParser
from src.database.connection import db_manager
from src.database.models import Film


def debug_rating_strategies():
    """Анализирует какие стратегии поиска рейтингов срабатывают"""
    print("🔍 Анализ эффективности стратегий поиска рейтингов")
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
        strategy_stats = {
            'kp': {'success': 0, 'strategies': {}},
            'imdb': {'success': 0, 'strategies': {}}
        }

        for i, film in enumerate(test_films, 1):
            print(f"\n🎬 {i}. {film.title}")
            print("-" * 50)

            try:
                # Парсим фильм
                film_data = parser.parse_film_details(film.url)

                # Анализируем KP рейтинг
                kp_result = film_data.get('kp_rating', {})
                if kp_result.get('rating'):
                    strategy = kp_result.get('strategy', 'unknown')
                    strategy_stats['kp']['strategies'][strategy] = strategy_stats['kp']['strategies'].get(strategy,
                                                                                                          0) + 1
                    strategy_stats['kp']['success'] += 1

                    print(f"⭐ КиноПоиск: {kp_result['rating']}")
                    print(f"   🎯 Стратегия: {kp_result.get('strategy', 'unknown')}")
                    print(f"   📝 Инфо: {kp_result.get('debug_info', '')}")
                else:
                    print(f"❌ КиноПоиск: не найден")
                    print(f"   📝 Причина: {kp_result.get('debug_info', '')}")

                # Анализируем IMDB рейтинг
                imdb_result = film_data.get('imdb_rating', {})
                if imdb_result.get('rating'):
                    strategy = imdb_result.get('strategy', 'unknown')
                    strategy_stats['imdb']['strategies'][strategy] = strategy_stats['imdb']['strategies'].get(strategy,
                                                                                                              0) + 1
                    strategy_stats['imdb']['success'] += 1

                    print(f"⭐ IMDB: {imdb_result['rating']}")
                    print(f"   🎯 Стратегия: {imdb_result.get('strategy', 'unknown')}")
                    print(f"   📝 Инфо: {imdb_result.get('debug_info', '')}")
                else:
                    print(f"❌ IMDB: не найден")
                    print(f"   📝 Причина: {imdb_result.get('debug_info', '')}")

                # LordFilm рейтинг (для сравнения)
                lf_rating = film_data.get('lf_rating')
                if lf_rating:
                    print(f"✅ LordFilm: {lf_rating} (для сравнения)")

            except Exception as e:
                print(f"💥 Ошибка парсинга: {e}")

            print()

        # Статистика по стратегиям
        print("\n📈 СТАТИСТИКА СТРАТЕГИЙ:")
        print("=" * 50)

        for rating_type, stats in strategy_stats.items():
            print(f"\n{rating_type.upper()}:")
            print(f"   Успешных поисков: {stats['success']}/{len(test_films)}")
            if stats['strategies']:
                print("   Рабочие стратегии:")
                for strategy, count in stats['strategies'].items():
                    print(f"     - {strategy}: {count} раз")
            else:
                print("   ❌ Ни одна стратегия не сработала")

    except Exception as e:
        print(f"❌ Общая ошибка: {e}")
    finally:
        parser.close()
        session.close()


if __name__ == "__main__":
    debug_rating_strategies()