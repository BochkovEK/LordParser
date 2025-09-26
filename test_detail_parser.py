#!/usr/bin/env python3
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from parser.lordfilm_parser import LordFilmParser


def test_movie_details():
    """Тестирование парсинга детальной страницы"""
    if len(sys.argv) != 2:
        print("Использование: python test_detail_parser.py <URL>")
        print("Пример: python test_detail_parser.py 'https://mk.lordfilm17.ru/filmy/nikto-2-2025'")
        return

    url = sys.argv[1]

    print(f"🎬 Тестируем парсинг детальной страницы: {url}")

    # Создаем парсер
    parser = LordFilmParser(debug=True)

    try:
        # Парсим детальную информацию
        details = parser.parse_movie_details(url)

        print("\n" + "=" * 60)
        print("✅ РЕЗУЛЬТАТЫ ПАРСИНГА ДЕТАЛЬНОЙ СТРАНИЦЫ")
        print("=" * 60)

        for key, value in details.items():
            if isinstance(value, list):
                print(f"{key:15}: {', '.join(value)}\n")
            else:
                print(f"{key:15}: {value}\n")

        print("=" * 60)

    except Exception as e:
        print(f"❌ Ошибка: {e}")
    finally:
        parser.cleanup()


if __name__ == "__main__":
    test_movie_details()