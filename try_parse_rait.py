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


def extract_clean_ratings(driver):
    """Извлекает чистые значения рейтинга со страницы"""
    try:
        # Способ 1: Поиск в тексте (оригинальный способ)
        body_text = driver.find_element(By.TAG_NAME, "body").text
        rating_pattern = re.findall(r'(\d+)\s+(\d+\.\d+)\s+(\d+)', body_text)

        if rating_pattern:
            likes, rating, dislikes = rating_pattern[0]
            return {
                "likes": int(likes),
                "rating": float(rating),
                "dislikes": int(dislikes),
                "source": "text_pattern"
            }

        # Способ 2: Поиск в HTML элементах через Selenium
        likes_from_elements = extract_likes_from_elements(driver)
        dislikes_from_elements = extract_dislikes_from_elements(driver)
        rating_from_elements = extract_rating_from_elements(driver)

        if likes_from_elements is not None and dislikes_from_elements is not None:
            return {
                "likes": likes_from_elements,
                "rating": rating_from_elements or 0.0,
                "dislikes": dislikes_from_elements,
                "source": "html_elements"
            }

        return {"error": "Рейтинг не найден"}

    except Exception as e:
        return {"error": str(e)}


def extract_likes_from_elements(driver):
    """Извлекает количество лайков из элементов страницы"""
    try:
        # Ищем элемент с классом rate-plus и внутри span с классом psc
        like_selectors = [
            "div.rate-plus span.psc",
            ".rate-plus .psc",
            "[class*='rate-plus'] [class*='psc']",
            "#ps-\\d+ .psc"  # по ID типа ps-53544
        ]

        for selector in like_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    text = element.text.strip()
                    if text and text.isdigit():
                        print(f"✅ Найден лайк: {text}")
                        return int(text)
            except:
                continue

        return None

    except Exception as e:
        print(f"Ошибка извлечения лайков: {e}")
        return None


def extract_dislikes_from_elements(driver):
    """Извлекает количество дизлайков из элементов страницы"""
    try:
        # Ищем элемент с классом rate-minus и внутри span с классом msc
        dislike_selectors = [
            "div.rate-minus span.msc",
            ".rate-minus .msc",
            "[class*='rate-minus'] [class*='msc']",
            "#ms-\\d+ .msc"  # по ID типа ms-53544
        ]

        for selector in dislike_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    text = element.text.strip()
                    if text and text.isdigit():
                        print(f"✅ Найден дизлайк: {text}")
                        return int(text)
            except:
                continue

        return None

    except Exception as e:
        print(f"Ошибка извлечения дизлайков: {e}")
        return None


def extract_rating_from_elements(driver):
    """Извлекает рейтинг из элементов страницы"""
    try:
        # Ищем рейтинг в различных элементах
        rating_selectors = [
            "[class*='rating']",
            "[class*='rate-value']",
            "[class*='score']"
        ]

        for selector in rating_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    text = element.text.strip()
                    # Ищем число с точкой
                    match = re.search(r'(\d+\.\d+)', text)
                    if match:
                        print(f"✅ Найден рейтинг: {match.group(1)}")
                        return float(match.group(1))
            except:
                continue

        return None

    except Exception as e:
        print(f"Ошибка извлечения рейтинга: {e}")
        return None


def extract_additional_ratings(driver):
    """Извлекает рейтинги КиноПоиск (КП) и IMDB"""
    additional_ratings = {}

    try:
        # Получаем весь текст страницы
        body_text = driver.find_element(By.TAG_NAME, "body").text

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

        # Альтернативный поиск по элементам страницы
        if not additional_ratings:
            additional_ratings = find_ratings_in_elements(driver)

    except Exception as e:
        print(f"Ошибка поиска дополнительных рейтингов: {e}")

    return additional_ratings


def find_ratings_in_elements(driver):
    """Ищет рейтинги в элементах страницы (более надежный метод)"""
    ratings = {}

    try:
        # Ищем элементы, содержащие слова КП, IMDB и числа
        elements = driver.find_elements(By.XPATH,
                                        "//*[text()[contains(., 'КП') or contains(., 'IMDB') or contains(., 'КиноПоиск') or contains(., 'IMDb')]]")

        for element in elements:
            text = element.text

            # Ищем КП рейтинг
            kp_match = re.search(r'КП[:\s]*(\d+\.\d+)', text, re.IGNORECASE)
            if kp_match and 'kinopoisk' not in ratings:
                ratings['kinopoisk'] = float(kp_match.group(1))

            # Ищем IMDB рейтинг
            imdb_match = re.search(r'IMDB[:\s]*(\d+\.\d+)', text, re.IGNORECASE)
            if imdb_match and 'imdb' not in ratings:
                ratings['imdb'] = float(imdb_match.group(1))

    except Exception as e:
        print(f"Ошибка поиска в элементах: {e}")

    return ratings


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


def extract_movie_data(driver, url):
    """
    Извлекает полную информацию о фильме: рейтинг + метаданные + дополнительные рейтинги
    """
    print(f"Анализирую URL: {url}")

    try:
        # Используем кэширование для ускорения
        page_content = get_page_content(driver, url)

        # Перезагружаем страницу для работы с Selenium элементами
        driver.get(url)
        WebDriverWait(driver, 10).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        time.sleep(3)

        # Извлекаем рейтинг LordFilm (из текста и элементов)
        ratings = extract_clean_ratings(driver)

        # Извлекаем метаданные
        metadata = extract_metadata(driver)

        # Извлекаем дополнительные рейтинги (КП и IMDB)
        additional_ratings = extract_additional_ratings(driver)

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
                    additional = result['additional_ratings']

                    print(f"🎬 {metadata.get('title', 'Название не найдено')}")
                    print(f"📅 Год: {metadata.get('year', 'Не указан')}")
                    print(f"🌍 Страна: {metadata.get('country', 'Не указана')}")
                    print(f"🔤 Оригинал: {metadata.get('original_title', 'Не указано')}")
                    print(f"🎭 Режиссер: {metadata.get('director', 'Не указан')}")
                    print(f"📊 Категории: {', '.join(metadata.get('categories', []))}")

                    if 'error' not in ratings:
                        source_info = f" ({ratings.get('source', 'unknown')})"
                        print(
                            f"⭐ Рейтинг: {ratings['rating']} (👍 {ratings['likes']} 👎 {ratings['dislikes']}){source_info}")
                    else:
                        print(f"⭐ Рейтинг: {ratings['error']}")

                    # Дополнительные рейтинги
                    if additional:
                        kp = additional.get('kinopoisk')
                        imdb = additional.get('imdb')
                        if kp or imdb:
                            print(f"🎯 Доп. рейтинги: ", end="")
                            if kp:
                                print(f"КП: {kp} ", end="")
                            if imdb:
                                print(f"IMDB: {imdb}", end="")
                            print()

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