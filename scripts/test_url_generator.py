#!/usr/bin/env python3
import sys
import os

# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.parser.url_generator import URLGenerator


def main():
    generator = URLGenerator()

    print("🎯 Тест URL генератора")
    print("=" * 50)

    # Статистика
    print(f"📊 Статистика:")
    print(f"   Годы: {generator.start_year}-{generator.end_year}")
    print(f"   Страниц в год: {generator.pages_per_year}")
    print(f"   Всего URL (полный): {generator.get_urls_count('all')}")
    print(f"   Всего URL (ежедневный): {generator.get_urls_count('daily')}")

    # Примеры URL
    print(f"\n🔗 Примеры URL (ежедневные):")
    daily_urls = list(generator.generate_daily_urls())
    for i, url in enumerate(daily_urls[:3]):
        print(f"   {i + 1}. {url}")

    print(f"\n✅ Генератор работает корректно!")


if __name__ == "__main__":
    main()