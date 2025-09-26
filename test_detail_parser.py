#!/usr/bin/env python3
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from parser.lordfilm_parser import LordFilmParser


def test_movie_details_with_rating():
    """Тестирование парсинга детальной страницы с рейтингом"""
    if len(sys.argv) != 2:
        print("Использование: python test_detail_parser.py <URL>")
        print("Пример: python test_detail_parser.py 'https://mk.lordfilm17.ru/filmy/nikto-2-2025'")
        return

    url = sys.argv[1]

    print(f"🎬 Тестируем парсинг детальной страницы с рейтингом: {url}")

    # Создаем парсер с debug=True для подробного вывода
    parser = LordFilmParser(debug=True)

    try:
        # Парсим детальную информацию с рейтингом
        details = parser.parse_movie_details_with_rating(url)

        print("\n" + "=" * 70)
        print("✅ РЕЗУЛЬТАТЫ ПАРСИНГА ДЕТАЛЬНОЙ СТРАНИЦЫ С РЕЙТИНГОМ")
        print("=" * 70)

        # Выводим все поля
        for key, value in details.items():
            if isinstance(value, list):
                print(f"{key:25}: {', '.join(value)}")
            else:
                print(f"{key:25}: {value}")

        print("=" * 70)

        # Статистика
        print(f"📊 Найдено полей: {len(details)}")
        if 'rating' in details:
            print(f"⭐ Рейтинг: {details['rating']}")
            if 'votes' in details:
                print(f"🗳️  Голосов: {details['votes']}")
        else:
            print("❌ Рейтинг не найден")

    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
    finally:
        parser.cleanup()
        print("🧹 Парсер закрыт")


def test_only_rating():
    """Тестирование только парсинга рейтинга"""
    if len(sys.argv) != 2:
        print("Использование: python test_detail_parser.py <URL>")
        return

    url = sys.argv[1]
    print(f"⭐ Тестируем только парсинг рейтинга: {url}")

    parser = LordFilmParser(debug=True)

    try:
        # Сначала переходим на страницу
        parser.driver.get(url)

        # Парсим только рейтинг
        rating_data = parser._parse_rating_with_wait()

        print("\n" + "=" * 50)
        print("✅ РЕЗУЛЬТАТЫ ПАРСИНГА РЕЙТИНГА")
        print("=" * 50)

        for key, value in rating_data.items():
            print(f"{key:15}: {value}")

        print("=" * 50)

        if not rating_data:
            print("❌ Рейтинг не найден")
        elif 'rating' not in rating_data:
            print("⚠️  Данные найдены, но рейтинг не извлечен")

    except Exception as e:
        print(f"❌ Ошибка: {e}")
    finally:
        parser.cleanup()


if __name__ == "__main__":
    # Запуск полного теста
    test_movie_details_with_rating()

    # Если хотите тестировать только рейтинг, раскомментируйте:
    # test_only_rating()