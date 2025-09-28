from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import json
import time
import re

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
    Извлекает полную информацию о фильме: рейтинг + метаданные + дополнительные рейтинги
    """
    print(f"Анализирую URL: {url}")

    try:
        driver.get(url)
        WebDriverWait(driver, 10).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        time.sleep(3)

        # Извлекаем рейтинг LordFilm (старым методом)
        ratings_old = extract_clean_ratings(driver)

        # Извлекаем рейтинг новым методом (из HTML-элементов)
        ratings_new = extract_ratings_from_elements(driver)

        # Извлекаем метаданные
        metadata = extract_metadata(driver)

        # Извлекаем дополнительные рейтинги (КП и IMDB)
        additional_ratings = extract_additional_ratings(driver)

        return {
            "url": url,
            "success": True,
            "ratings": {
                "old_method": ratings_old,
                "new_method": ratings_new
            },
            "metadata": metadata,
            "additional_ratings": additional_ratings
        }

    except Exception as e:
        return {
            "url": url,
            "success": False,
            "error": str(e)
        }


def extract_ratings_from_elements(driver):
    """
    Извлекает лайки и дизлайки из HTML-элементов (новый метод)
    """
    ratings = {}

    try:
        # Ищем элемент лайков по классу rate-plus и классу psc
        likes_elements = driver.find_elements(By.CSS_SELECTOR, "div.rate-plus span.psc")
        if likes_elements:
            likes_text = likes_elements[0].text.strip()
            if likes_text.isdigit():
                ratings['likes'] = int(likes_text)
            else:
                ratings['likes_error'] = f"Некорректное значение лайков: {likes_text}"
        else:
            ratings['likes_error'] = "Элемент лайков не найден"

    except Exception as e:
        ratings['likes_error'] = f"Ошибка извлечения лайков: {str(e)}"

    try:
        # Ищем элемент дизлайков по классу rate-minus и классу msc
        dislikes_elements = driver.find_elements(By.CSS_SELECTOR, "div.rate-minus span.msc")
        if dislikes_elements:
            dislikes_text = dislikes_elements[0].text.strip()
            if dislikes_text.isdigit():
                ratings['dislikes'] = int(dislikes_text)
            else:
                ratings['dislikes_error'] = f"Некорректное значение дизлайков: {dislikes_text}"
        else:
            ratings['dislikes_error'] = "Элемент дизлайков не найден"

    except Exception as e:
        ratings['dislikes_error'] = f"Ошибка извлечения дизлайков: {str(e)}"

    # Вычисляем рейтинг на основе лайков и дизлайков, если оба значения доступны
    if 'likes' in ratings and 'dislikes' in ratings:
        total = ratings['likes'] + ratings['dislikes']
        if total > 0:
            ratings['rating'] = round((ratings['likes'] / total) * 10, 1)
        else:
            ratings['rating_error'] = "Невозможно вычислить рейтинг (лайки + дизлайки = 0)"

    return ratings


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


def extract_clean_ratings(driver):
    """Извлекает чистые значения рейтинга (старый метод)"""
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

                if result['success']:
                    ratings_old = result['ratings']['old_method']
                    ratings_new = result['ratings']['new_method']
                    metadata = result['metadata']

                    print(f"🎬 {metadata.get('title', 'Название не найдено')}")
                    print(f"📅 Год: {metadata.get('year', 'Не указан')}")
                    print(f"🌍 Страна: {metadata.get('country', 'Не указана')}")
                    print(f"🔤 Оригинал: {metadata.get('original_title', 'Не указано')}")
                    print(f"🎭 Режиссер: {metadata.get('director', 'Не указан')}")
                    print(f"📊 Категории: {', '.join(metadata.get('categories', []))}")

                    # Вывод рейтингов старым методом
                    print("\n📊 РЕЙТИНГИ (старый метод):")
                    if 'error' not in ratings_old:
                        print(
                            f"⭐ Рейтинг: {ratings_old['rating']} (👍 {ratings_old['likes']} 👎 {ratings_old['dislikes']})")
                    else:
                        print(f"❌ Ошибка: {ratings_old['error']}")

                    # Вывод рейтингов новым методом
                    print("\n📊 РЕЙТИНГИ (новый метод из HTML):")
                    if 'rating' in ratings_new:
                        print(
                            f"⭐ Рейтинг: {ratings_new['rating']} (👍 {ratings_new['likes']} 👎 {ratings_new['dislikes']})")
                    else:
                        if 'likes' in ratings_new:
                            print(f"👍 Лайки: {ratings_new['likes']}")
                        else:
                            print(f"❌ Лайки: {ratings_new.get('likes_error', 'Не найдены')}")

                        if 'dislikes' in ratings_new:
                            print(f"👎 Дизлайки: {ratings_new['dislikes']}")
                        else:
                            print(f"❌ Дизлайки: {ratings_new.get('dislikes_error', 'Не найдены')}")

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