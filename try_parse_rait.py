from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
import json
import time
import os

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


def extract_ratings_from_elements(driver):
    """Извлекает лайки и дизлайки из HTML-элементов"""
    ratings = {}

    # Ищем лайки
    likes_elements = driver.find_elements(By.CSS_SELECTOR, "div.rate-plus span.psc")
    if likes_elements:
        likes_text = likes_elements[0].text.strip()
        if likes_text.isdigit():
            ratings['likes'] = int(likes_text)

    # Ищем дизлайки
    dislikes_elements = driver.find_elements(By.CSS_SELECTOR, "div.rate-minus span.msc")
    if dislikes_elements:
        dislikes_text = dislikes_elements[0].text.strip()
        if dislikes_text.isdigit():
            ratings['dislikes'] = int(dislikes_text)

    return ratings


def save_html_content(driver, url, output_dir="html_pages"):
    """Сохраняет HTML контент страницы для анализа"""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Создаем безопасное имя файла из URL
    filename = url.replace('https://', '').replace('http://', '').replace('/', '_')[:100] + ".html"
    filepath = os.path.join(output_dir, filename)

    html_content = driver.page_source

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(html_content)

    print(f"💾 HTML сохранен: {filepath}")
    return filepath


def extract_movie_data(driver, url):
    """Извлекает информацию о фильме и сохраняет HTML для анализа"""
    print(f"Анализирую URL: {url}")

    try:
        driver.get(url)
        WebDriverWait(driver, 10).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        time.sleep(2)

        # Извлекаем рейтинги
        ratings = extract_ratings_from_elements(driver)

        # Сохраняем HTML для ручного анализа
        html_file = save_html_content(driver, url)

        return {
            "url": url,
            "success": True,
            "ratings": ratings,
            "html_saved": html_file
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
                    ratings = result['ratings']
                    print(f"📊 Рейтинги для {url_key}:")
                    print(f"👍 Лайки: {ratings.get('likes', 'Не найдены')}")
                    print(f"👎 Дизлайки: {ratings.get('dislikes', 'Не найдены')}")
                    print(f"💾 HTML сохранен: {result['html_saved']}")
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

        print("🎉 Парсинг завершен!")
        print("📁 HTML страницы сохранены в папку 'html_pages'")
        print("📊 Результаты в 'movie_data.json'")

    finally:
        driver.quit()


if __name__ == "__main__":
    main()