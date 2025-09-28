from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import json
import time
import re
import os
from pathlib import Path

SELENIUM_URL = "http://localhost:4444/wd/hub"
CACHE_DIR = "html_cache"


def setup_driver():
    """Настройка Selenium WebDriver для контейнера"""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")

    driver = webdriver.Remote(
        command_executor=SELENIUM_URL,
        options=chrome_options
    )
    return driver


def get_page_content(driver, url):
    """Получает содержимое страницы с кэшированием"""
    # Создаем директорию для кэша
    Path(CACHE_DIR).mkdir(exist_ok=True)

    # Генерируем имя файла из URL
    filename = re.sub(r'[^a-zA-Z0-9]', '_', url) + ".html"
    cache_path = os.path.join(CACHE_DIR, filename)

    # Пробуем загрузить из кэша
    if os.path.exists(cache_path):
        print(f"📁 Загружаем из кэша: {cache_path}")
        with open(cache_path, 'r', encoding='utf-8') as f:
            return f.read()

    # Если нет в кэше, загружаем через Selenium
    print(f"🌐 Загружаем через Selenium: {url}")
    driver.get(url)
    WebDriverWait(driver, 10).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )
    time.sleep(3)

    content = driver.page_source

    # Сохраняем в кэш
    with open(cache_path, 'w', encoding='utf-8') as f:
        f.write(content)
    print(f"💾 Сохранено в кэш: {cache_path}")

    return content


def extract_movie_data(driver, url):
    """
    Извлекает полную информацию о фильме: рейтинг + метаданные + дополнительные рейтинги
    """
    print(f"Анализирую URL: {url}")

    try:
        # Получаем содержимое страницы (из кэша или через Selenium)
        page_content = get_page_content(driver, url)

        # Создаем временный driver для парсинга из кэша
        from selenium.webdriver.common.by import By

        # Для извлечения текста из HTML используем простой парсинг
        body_text = extract_text_from_html(page_content)

        # Извлекаем рейтинг LordFilm
        ratings = extract_clean_ratings_from_text(body_text)

        # Извлекаем метаданные
        metadata = extract_metadata_from_text(body_text)

        # Извлекаем дополнительные рейтинги (КП и IMDB)
        additional_ratings = extract_additional_ratings_from_text(body_text)

        return {
            "url": url,
            "success": True,
            "ratings": ratings,
            "metadata": metadata,
            "additional_ratings": additional_ratings
        }

    except Exception as e:
        return {
            "url": url,
            "success": False,
            "error": str(e)
        }


def extract_text_from_html(html_content):
    """Извлекает текст из HTML содержимого"""
    # Простой способ извлечения текста (убираем теги)
    text = re.sub(r'<[^>]+>', ' ', html_content)
    # Убираем лишние пробелы
    text = re.sub(r'\s+', ' ', text)
    return text


def extract_clean_ratings_from_text(body_text):
    """Извлекает чистые значения рейтинга из текста"""
    try:
        rating_pattern = re.findall(r'(\d+)\s+(\d+\.\d+)\s+(\d+)', body_text)

        if rating_pattern:
            likes, rating, dislikes = rating_pattern[0]
            return {
                "likes": int(likes),
                "rating": float(rating),
                "dislikes": int(dislikes)
            }

        return {"error": "Рейтинг не найден"}

    except Exception as e:
        return {"error": str(e)}


def extract_additional_ratings_from_text(body_text):
    """Извлекает рейтинги КиноПоиск (КП) и IMDB из текста"""
    additional_ratings = {}

    try:
        # Паттерны для поиска рейтингов
        patterns = [
            # Формат: КП 7.8, IMDB 7.5
            (r'КП\s*(\d+\.\d+)', 'kinopoisk'),
            (r'IMDB\s*(\d+\.\d+)', 'imdb'),
            # Формат: КиноПоиск: 7.8, IMDb: 7.5
            (r'КиноПоиск[:\s]*(\d+\.\d+)', 'kinopoisk'),
            (r'IMDb[:\s]*(\d+\.\d+)', 'imdb'),
            # Формат с русскими буквами: КП, ИМДБ
            (r'КП[:\s]*(\d+\.\d+)', 'kinopoisk'),
            (r'ИМДБ[:\s]*(\d+\.\d+)', 'imdb'),
        ]

        for pattern, rating_type in patterns:
            match = re.search(pattern, body_text, re.IGNORECASE)
            if match:
                additional_ratings[rating_type] = float(match.group(1))

    except Exception as e:
        print(f"Ошибка поиска дополнительных рейтингов: {e}")

    return additional_ratings


def extract_metadata_from_text(body_text):
    """Извлекает метаданные фильма из текста"""
    metadata = {}

    try:
        # Русское название (ищем в тексте)
        title_match = re.search(r'([^<]+?)\s+смотреть онлайн', body_text)
        if title_match:
            metadata['title'] = title_match.group(1).strip()

        # Год выхода
        year_match = re.search(r'Год выхода:\s*(\d{4})', body_text)
        if year_match:
            metadata['year'] = year_match.group(1)

        # Страна
        country_match = re.search(r'Страна:\s*([^\n]+)', body_text)
        if country_match:
            metadata['country'] = country_match.group(1).strip()

        # Оригинальное название
        original_title_match = re.search(r'Оригинальное название:\s*([^\n]+)', body_text)
        if original_title_match:
            metadata['original_title'] = original_title_match.group(1).strip()

        # Категории
        categories_match = re.search(r'Категории?:\s*([^\n]+)', body_text, re.IGNORECASE)
        if categories_match:
            categories_text = categories_match.group(1).strip()
            # Разделяем категории по слешам
            metadata['categories'] = [cat.strip() for cat in categories_text.split('/')]

        # Режиссер
        director_match = re.search(r'Режиссер:\s*([^\n]+)', body_text)
        if director_match:
            metadata['director'] = director_match.group(1).strip()

        # Актеры (упрощенный поиск)
        actors_match = re.search(r'Актеры:[^<]*([^<]+)', body_text)
        if actors_match:
            actors_text = actors_match.group(1).strip()
            actors = [actor.strip() for actor in re.split(r'[,\n]', actors_text) if actor.strip()]
            metadata['actors'] = actors[:10]  # Ограничиваем список

    except Exception as e:
        metadata['error'] = f"Ошибка извлечения метаданных: {str(e)}"

    return metadata


# Основная функция
def main():
    # Загрузка конфига из файла
    with open('config.json', 'r') as f:
        config = json.load(f)

    driver = setup_driver()
    all_results = {}

    try:
        for url_key, target_values in config.items():
            # Если ключ начинается с 'https', считаем его URL
            if url_key.startswith('https'):
                result = extract_movie_data(driver, url_key)
                all_results[url_key] = result

                if result['success']:
                    ratings = result['ratings']
                    metadata = result['metadata']

                    print(f"🎬 {metadata.get('title', 'Название не найдено')}")
                    print(f"📅 Год: {metadata.get('year', 'Не указан')}")
                    print(f"🌍 Страна: {metadata.get('country', 'Не указана')}")
                    print(f"🔤 Оригинал: {metadata.get('original_title', 'Не указано')}")
                    print(f"🎭 Режиссер: {metadata.get('director', 'Не указан')}")
                    print(f"📊 Категории: {', '.join(metadata.get('categories', []))}")

                    if 'error' not in ratings:
                        print(f"⭐ Рейтинг: {ratings['rating']} (👍 {ratings['likes']} 👎 {ratings['dislikes']})")
                    else:
                        print(f"⭐ Рейтинг: {ratings['error']}")

                    if metadata.get('actors'):
                        print(f"🎭 Актеры: {', '.join(metadata['actors'][:5])}...")

                else:
                    print(f"❌ Ошибка: {result['error']}")

                print("=" * 60)
                time.sleep(1)  # Уменьшили паузу для кэшированных запросов

        # Сохранение результатов
        output = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "results": all_results
        }

        with open('movie_data.json', 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=2)

        print("🎉 Парсинг завершен! Результаты в movie_data.json")

    finally:
        driver.quit()


if __name__ == "__main__":
    main()