from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
import json
import time

SELENIUM_URL = "http://localhost:4444/wd/hub"


def setup_driver():
    """Настройка Selenium WebDriver"""
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


def extract_lf_ratings(driver):
    """Извлекает лайки и дизлайки LordFilm"""
    ratings = {}

    # Ищем лайки
    likes_elements = driver.find_elements(By.CSS_SELECTOR, "div.rate-plus span.psc")
    if likes_elements:
        likes_text = likes_elements[0].text.strip()
        if likes_text.isdigit():
            ratings['lf_likes'] = int(likes_text)
        else:
            ratings['lf_likes_error'] = f"Некорректное значение лайков: {likes_text}"
    else:
        ratings['lf_likes_error'] = "Элемент лайков не найден"

    # Ищем дизлайки
    dislikes_elements = driver.find_elements(By.CSS_SELECTOR, "div.rate-minus span.msc")
    if dislikes_elements:
        dislikes_text = dislikes_elements[0].text.strip()
        if dislikes_text.isdigit():
            ratings['lf_dislikes'] = int(dislikes_text)
        else:
            ratings['lf_dislikes_error'] = f"Некорректное значение дизлайков: {dislikes_text}"
    else:
        ratings['lf_dislikes_error'] = "Элемент дизлайков не найден"

    return ratings


def extract_external_ratings(driver):
    """Извлекает рейтинги КиноПоиск и IMDB"""
    ratings = {}

    # Ищем рейтинг КиноПоиск
    kp_elements = driver.find_elements(By.CSS_SELECTOR, "div.frate.frate-kp span")
    if kp_elements:
        kp_text = kp_elements[0].text.strip()
        try:
            ratings['kp_rating'] = float(kp_text)
        except ValueError:
            ratings['kp_error'] = f"Некорректное значение КП: {kp_text}"
    else:
        ratings['kp_error'] = "Рейтинг КиноПоиск не найден"

    # Ищем рейтинг IMDB
    imdb_elements = driver.find_elements(By.CSS_SELECTOR, "div.frate.frate-imdb span")
    if imdb_elements:
        imdb_text = imdb_elements[0].text.strip()
        try:
            ratings['imdb_rating'] = float(imdb_text)
        except ValueError:
            ratings['imdb_error'] = f"Некорректное значение IMDB: {imdb_text}"
    else:
        ratings['imdb_error'] = "Рейтинг IMDB не найден"

    return ratings


def extract_movie_data(driver, url):
    """Извлекает всю информацию о фильме"""
    print(f"Анализирую URL: {url}")

    try:
        driver.get(url)
        WebDriverWait(driver, 10).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        time.sleep(2)

        # Извлекаем рейтинги LordFilm
        lf_ratings = extract_lf_ratings(driver)

        # Извлекаем внешние рейтинги
        external_ratings = extract_external_ratings(driver)

        return {
            "url": url,
            "success": True,
            "lf_ratings": lf_ratings,
            "external_ratings": external_ratings
        }

    except Exception as e:
        return {
            "url": url,
            "success": False,
            "error": str(e)
        }


def main():
    # Загрузка конфига
    with open('config.json', 'r') as f:
        config = json.load(f)

    driver = setup_driver()
    all_results = {}

    try:
        for url_key in config:
            if url_key.startswith('https'):
                result = extract_movie_data(driver, url_key)
                all_results[url_key] = result

                if result['success']:
                    lf_ratings = result['lf_ratings']
                    external_ratings = result['external_ratings']

                    print(f"🎬 Фильм: {url_key}")

                    # Вывод рейтингов LordFilm
                    print("\n📊 LordFilm Рейтинги:")
                    if 'lf_likes' in lf_ratings:
                        print(f"👍 Лайки: {lf_ratings['lf_likes']}")
                    else:
                        print(f"❌ Лайки: {lf_ratings.get('lf_likes_error', 'Не найдены')}")

                    if 'lf_dislikes' in lf_ratings:
                        print(f"👎 Дизлайки: {lf_ratings['lf_dislikes']}")
                    else:
                        print(f"❌ Дизлайки: {lf_ratings.get('lf_dislikes_error', 'Не найдены')}")

                    # Вывод внешних рейтингов
                    print("\n⭐ Внешние рейтинги:")
                    if 'kp_rating' in external_ratings:
                        print(f"🎯 КиноПоиск: {external_ratings['kp_rating']}")
                    else:
                        print(f"❌ КиноПоиск: {external_ratings.get('kp_error', 'Не найден')}")

                    if 'imdb_rating' in external_ratings:
                        print(f"🌍 IMDB: {external_ratings['imdb_rating']}")
                    else:
                        print(f"❌ IMDB: {external_ratings.get('imdb_error', 'Не найден')}")

                else:
                    print(f"❌ Ошибка: {result['error']}")

                print("=" * 50)
                time.sleep(1)

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