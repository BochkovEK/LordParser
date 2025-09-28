"""
Генератор URL для парсинга LordFilm
"""
from typing import Generator
from src.config.settings import DEFAULT_URL, DEFAULT_CATEGORY, YEAR_RANGE, PARSE_PAGES


class URLGenerator:
    """Генерирует URL страниц для парсинга"""

    def __init__(self):
        self.base_url = DEFAULT_URL
        self.category = DEFAULT_CATEGORY
        self.start_year, self.end_year = YEAR_RANGE
        self.pages_per_year = PARSE_PAGES

    def generate_page_urls(self) -> Generator[str, None, None]:
        """
        Генерирует URL всех страниц для парсинга

        Yields:
            URL страниц в формате: https://wk.lordfilm17.ru/filmy/2025/page/1/
        """
        for year in range(self.start_year, self.end_year + 1):
            for page in range(1, self.pages_per_year + 1):
                url = f"{self.base_url}/{self.category}/{year}/page/{page}/"
                yield url

    def generate_daily_urls(self) -> Generator[str, None, None]:
        """
        Генерирует URL для ежедневного парсинга (только последние годы)
        """
        daily_years = [self.end_year, self.end_year - 1]  # 2025, 2024

        for year in daily_years:
            for page in range(1, self.pages_per_year + 1):
                url = f"{self.base_url}/{self.category}/{year}/page/{page}/"
                yield url

    def generate_weekly_urls(self) -> Generator[str, None, None]:
        """
        Генерирует URL для еженедельного парсинга (все годы)
        """
        return self.generate_page_urls()

    def get_urls_count(self, mode: str = "all") -> int:
        """
        Возвращает количество URL для парсинга

        Args:
            mode: "all" | "daily" | "weekly"

        Returns:
            Количество URL
        """
        if mode == "daily":
            years = 2  # 2024-2025
        else:  # "all" или "weekly"
            years = self.end_year - self.start_year + 1

        return years * self.pages_per_year


# Синглтон экземпляр
url_generator = URLGenerator()

if __name__ == "__main__":
    # Тест генератора
    generator = URLGenerator()

    print("🔗 Тест генератора URL:")
    print(f"Всего URL (полный): {generator.get_urls_count('all')}")
    print(f"Всего URL (ежедневный): {generator.get_urls_count('daily')}")

    print("\nПервые 5 URL (ежедневные):")
    for i, url in enumerate(generator.generate_daily_urls()):
        print(f"  {i + 1}. {url}")
        if i >= 4:
            break