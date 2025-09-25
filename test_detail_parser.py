#!/usr/bin/env python3
import sys
import logging
from typing import Dict, List, Optional
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import time
import random

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('DetailParser')


def setup_driver():
    """Настройка ChromeDriver"""
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--window-size=1920,1080")

    # User-Agent
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    driver = webdriver.Remote(
        command_executor="http://localhost:4444/wd/hub",
        options=chrome_options
    )

    # Скрытие автоматизации
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": """
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """
    })

    return driver


def parse_movie_details(url: str) -> Optional[Dict]:
    """Парсинг детальной страницы фильма"""
    driver = None
    try:
        logger.info(f"🔄 Начинаем парсинг: {url}")

        # Настройка драйвера
        driver = setup_driver()
        driver.set_page_load_timeout(30)

        # Загрузка страницы
        driver.get(url)

        # Ожидание загрузки контента
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )

        # Имитация человеческого поведения
        time.sleep(random.uniform(1, 2))

        # Получение HTML
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')

        # Парсинг данных
        details = {
            'title': parse_title(soup),
            'original_title': parse_original_title(soup),
            'year': parse_year(soup),
            'country': parse_country(soup),
            'genres': parse_genres(soup),
            'director': parse_director(soup),
            'actors': parse_actors(soup),
            'description': parse_description(soup),
            'rating_kp': parse_rating_kp(soup),
            'rating_imdb': parse_rating_imdb(soup),
            'votes_kp': parse_votes_kp(soup),
            'votes_imdb': parse_votes_imdb(soup),
            'duration': parse_duration(soup),
            'categories': parse_categories(soup)
        }

        logger.info("✅ Парсинг завершен успешно")
        return details

    except Exception as e:
        logger.error(f"❌ Ошибка при парсинге: {e}")
        return None
    finally:
        if driver:
            driver.quit()


def parse_title(soup: BeautifulSoup) -> Optional[str]:
    """Парсинг названия фильма"""
    try:
        # Попробуем несколько селекторов
        selectors = [
            'h1[itemprop="name"]',
            '.movie-title',
            'h1.title',
            'h1'
        ]

        for selector in selectors:
            title_elem = soup.select_one(selector)
            if title_elem:
                return title_elem.get_text(strip=True)
        return None
    except:
        return None


def parse_original_title(soup: BeautifulSoup) -> Optional[str]:
    """Парсинг оригинального названия"""
    try:
        elem = soup.select_one('.original-title')
        if elem:
            return elem.get_text(strip=True)
        return None
    except:
        return None


def parse_year(soup: BeautifulSoup) -> Optional[int]:
    """Парсинг года выпуска"""
    try:
        # Ищем год в различных местах
        year_selectors = [
            '.year',
            '.release-year',
            '[class*="year"]'
        ]

        for selector in year_selectors:
            elem = soup.select_one(selector)
            if elem:
                text = elem.get_text(strip=True)
                # Ищем 4-значное число
                import re
                year_match = re.search(r'\b(19|20)\d{2}\b', text)
                if year_match:
                    return int(year_match.group())
        return None
    except:
        return None


def parse_country(soup: BeautifulSoup) -> Optional[str]:
    """Парсинг страны"""
    try:
        # Ищем страну в тексте
        country_indicators = ['США', 'Россия', 'Страна', 'country', 'производство']

        for indicator in country_indicators:
            elem = soup.find(text=lambda t: t and indicator in str(t))
            if elem:
                # Пытаемся извлечь название страны
                parent = elem.parent
                if parent:
                    text = parent.get_text()
                    # Упрощенная логика извлечения страны
                    countries = ['США', 'Россия', 'Франция', 'Великобритания', 'Германия', 'Китай', 'Япония']
                    for country in countries:
                        if country in text:
                            return country
        return None
    except:
        return None


def parse_genres(soup: BeautifulSoup) -> List[str]:
    """Парсинг жанров"""
    try:
        genres = []
        # Ищем жанровые теги
        genre_selectors = [
            '.genre-tag',
            '.category-tag',
            '[class*="genre"]',
            '[class*="category"]'
        ]

        for selector in genre_selectors:
            elements = soup.select(selector)
            for elem in elements:
                genre = elem.get_text(strip=True)
                if genre and len(genre) < 50:  # Фильтруем слишком длинные тексты
                    genres.append(genre)

        return list(set(genres))  # Убираем дубликаты
    except:
        return []


def parse_director(soup: BeautifulSoup) -> Optional[str]:
    """Парсинг режиссера"""
    try:
        # Ищем режиссера по ключевым словам
        director_indicators = ['Режиссер', 'режиссер', 'Director', 'director']

        for indicator in director_indicators:
            elem = soup.find(text=lambda t: t and indicator in str(t))
            if elem:
                parent = elem.parent
                if parent:
                    # Пытаемся извлечь имя режиссера
                    text = parent.get_text()
                    # Упрощенная логика - берем текст после индикатора
                    parts = text.split(indicator)
                    if len(parts) > 1:
                        name = parts[1].strip().split('\n')[0].split(',')[0]
                        if name and len(name) < 100:
                            return name
        return None
    except:
        return None


def parse_actors(soup: BeautifulSoup) -> List[str]:
    """Парсинг актеров"""
    try:
        actors = []
        actor_indicators = ['Актеры', 'актеры', 'В ролях', 'Cast', 'cast']

        for indicator in actor_indicators:
            elem = soup.find(text=lambda t: t and indicator in str(t))
            if elem:
                parent = elem.parent
                if parent:
                    text = parent.get_text()
                    # Упрощенная логика извлечения актеров
                    lines = text.split('\n')
                    for line in lines:
                        line = line.strip()
                        if line and len(line) < 100 and not any(ind in line for ind in actor_indicators):
                            actors.append(line)

        return actors[:10]  # Ограничиваем количество
    except:
        return []


def parse_description(soup: BeautifulSoup) -> Optional[str]:
    """Парсинг описания"""
    try:
        desc_selectors = [
            '[itemprop="description"]',
            '.description',
            '.plot',
            '.synopsis'
        ]

        for selector in desc_selectors:
            elem = soup.select_one(selector)
            if elem:
                text = elem.get_text(strip=True)
                if len(text) > 10:  # Минимальная длина описания
                    return text[:500]  # Ограничиваем длину
        return None
    except:
        return None


def parse_rating_kp(soup: BeautifulSoup) -> Optional[float]:
    """Парсинг рейтинга КиноПоиска"""
    try:
        kp_selectors = ['.kp-rating', '[class*="kinopoisk"]', '.rating-kp']
        for selector in kp_selectors:
            elem = soup.select_one(selector)
            if elem:
                text = elem.get_text(strip=True)
                # Ищем число с плавающей точкой
                import re
                rating_match = re.search(r'\d+\.\d+', text)
                if rating_match:
                    return float(rating_match.group())
        return None
    except:
        return None


def parse_rating_imdb(soup: BeautifulSoup) -> Optional[float]:
    """Парсинг рейтинга IMDb"""
    try:
        imdb_selectors = ['.imdb-rating', '[class*="imdb"]', '.rating-imdb']
        for selector in imdb_selectors:
            elem = soup.select_one(selector)
            if elem:
                text = elem.get_text(strip=True)
                import re
                rating_match = re.search(r'\d+\.\d+', text)
                if rating_match:
                    return float(rating_match.group())
        return None
    except:
        return None


def parse_votes_kp(soup: BeautifulSoup) -> Optional[int]:
    """Парсинг количества голосов КиноПоиска"""
    try:
        # Ищем рядом с рейтингом КП
        kp_elem = soup.select_one('.kp-rating, [class*="kinopoisk"]')
        if kp_elem:
            parent = kp_elem.parent
            if parent:
                text = parent.get_text()
                import re
                votes_match = re.search(r'(\d+)\s*голос', text)
                if votes_match:
                    return int(votes_match.group(1))
        return None
    except:
        return None


def parse_votes_imdb(soup: BeautifulSoup) -> Optional[int]:
    """Парсинг количества голосов IMDb"""
    try:
        imdb_elem = soup.select_one('.imdb-rating, [class*="imdb"]')
        if imdb_elem:
            parent = imdb_elem.parent
            if parent:
                text = parent.get_text()
                import re
                votes_match = re.search(r'(\d+)\s*votes', text, re.IGNORECASE)
                if votes_match:
                    return int(votes_match.group(1))
        return None
    except:
        return None


def parse_duration(soup: BeautifulSoup) -> Optional[str]:
    """Парсинг продолжительности"""
    try:
        duration_indicators = ['мин', 'минут', 'duration', 'время']
        for indicator in duration_indicators:
            elem = soup.find(text=lambda t: t and indicator in str(t))
            if elem:
                # Ищем число перед индикатором
                import re
                duration_match = re.search(r'(\d+)\s*' + indicator, str(elem))
                if duration_match:
                    return f"{duration_match.group(1)} мин"
        return None
    except:
        return None


def parse_categories(soup: BeautifulSoup) -> List[str]:
    """Парсинг категорий/тегов"""
    try:
        categories = []
        # Ищем различные теги и категории
        tag_selectors = ['.tag', '.category', '.label']
        for selector in tag_selectors:
            elements = soup.select(selector)
            for elem in elements:
                category = elem.get_text(strip=True)
                if category and len(category) < 50:
                    categories.append(category)
        return list(set(categories))
    except:
        return []


def main():
    """Основная функция"""
    if len(sys.argv) != 2:
        print("Использование: python test_detail_parser.py <URL>")
        print("Пример: python test_detail_parser.py 'https://mk.lordfilm17.ru/filmy/12345'")
        sys.exit(1)

    url = sys.argv[1]

    logger.info(f"🎬 Тестовый парсинг детальной страницы")
    logger.info(f"📝 URL: {url}")

    # Парсим данные
    details = parse_movie_details(url)

    # Выводим результат
    if details:
        print("\n" + "=" * 60)
        print("✅ РЕЗУЛЬТАТЫ ПАРСИНГА")
        print("=" * 60)

        for key, value in details.items():
            if value:
                if isinstance(value, list):
                    print(f"{key:20}: {', '.join(value)}")
                else:
                    print(f"{key:20}: {value}")
            else:
                print(f"{key:20}: ❌ Не найдено")

        print("=" * 60)
    else:
        print("❌ Не удалось распарсить страницу")


if __name__ == "__main__":
    main()