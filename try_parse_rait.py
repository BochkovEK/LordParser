from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import json
import time
import re
# import os

SELENIUM_URL = "http://localhost:4444/wd/hub"

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


def extract_movie_data(driver, url):
    """
    Извлекает полную информацию о фильме: рейтинг + метаданные
    """
    print(f"Анализирую URL: {url}")

    try:
        driver.get(url)
        WebDriverWait(driver, 10).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        time.sleep(3)

        # Извлекаем рейтинг
        ratings = extract_clean_ratings(driver)

        # Извлекаем метаданные
        metadata = extract_metadata(driver)

        return {
            "url": url,
            "success": True,
            "ratings": ratings,
            "metadata": metadata
        }

    except Exception as e:
        return {
            "url": url,
            "success": False,
            "error": str(e)
        }


def extract_metadata(driver):
    """Извлекает метаданные фильма со страницы"""
    metadata = {}

    try:
        # Получаем весь текст страницы для анализа
        body_text = driver.find_element(By.TAG_NAME, "body").text

        # Русское название (ищем в заголовке h1/h2 или в начале текста)
        try:
            # Пробуем найти заголовок с названием фильма
            title_elements = driver.find_elements(By.XPATH, "//h1 | //h2")
            for title_element in title_elements:
                title_text = title_element.text.strip()
                if title_text and 'смотреть онлайн' in title_text.lower():
                    metadata['title'] = title_text.split('смотреть онлайн')[0].strip()
                    break

            # Если не нашли, ищем в начале body текста
            if 'title' not in metadata:
                first_lines = body_text.split('\n')[:10]  # Первые 10 строк
                for line in first_lines:
                    line = line.strip()
                    if line and 'смотреть онлайн' in line.lower():
                        metadata['title'] = line.split('смотреть онлайн')[0].strip()
                        break
        except:
            pass

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

        # Актеры (многострочный поиск)
        actors_section = extract_actors_section(body_text)
        if actors_section:
            metadata['actors'] = actors_section

    except Exception as e:
        metadata['error'] = f"Ошибка извлечения метаданных: {str(e)}"

    return metadata


def extract_actors_section(body_text):
    """Извлекает список актеров (сложный многострочный поиск)"""
    try:
        # Ищем блок после "Актеры:" до следующего заголовка
        lines = body_text.split('\n')
        actors_started = False
        actors_lines = []

        for line in lines:
            line = line.strip()

            if 'Актеры:' in line:
                actors_started = True
                # Убираем "Актеры:" из начала строки
                actor_line = line.replace('Актеры:', '').strip()
                if actor_line:
                    actors_lines.append(actor_line)
                continue

            if actors_started:
                # Останавливаемся на следующем заголовке или пустой строке
                if not line or line in ['Поиск по параметрам', 'Выберите жанр', 'Выберите страну']:
                    break
                actors_lines.append(line)

        if actors_lines:
            # Объединяем все строки и разбиваем по запятым/точкам
            all_actors_text = ' '.join(actors_lines)
            # Разделяем актеров (предполагаем разделение запятыми)
            actors = [actor.strip() for actor in re.split(r'[,\n]', all_actors_text) if actor.strip()]
            return actors[:20]  # Ограничиваем список

    except Exception as e:
        print(f"Ошибка извлечения актеров: {e}")

    return None


def extract_clean_ratings(driver):
    """Извлекает чистые значения рейтинга"""
    try:
        body_text = driver.find_element(By.TAG_NAME, "body").text
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


# Основная функция
def main():

    # Или загрузка конфига из файла
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

                # Красивый вывод
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
            time.sleep(2)

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
