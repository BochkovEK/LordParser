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

def extract_rating_numbers(driver, url):
    """
    Автоматически находит числа рейтинга на странице фильма
    """
    print(f"Анализирую URL: {url}")

    try:
        driver.get(url)
        WebDriverWait(driver, 10).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        time.sleep(3)

        # Получаем чистые числа рейтинга
        ratings = extract_clean_ratings(driver)

        return {
            "url": url,
            "success": True,
            "ratings": ratings
        }

    except Exception as e:
        return {
            "url": url,
            "success": False,
            "error": str(e)
        }


def extract_clean_ratings(driver):
    """Извлекает чистые значения рейтинга: лайки, дизлайки, рейтинг"""
    try:
        # Ищем блок с числами в формате: число число число.число число
        body_text = driver.find_element(By.TAG_NAME, "body").text

        # Паттерн: ищем последовательность из 3 чисел (лайки, рейтинг, дизлайки)
        rating_pattern = re.findall(r'(\d+)\s+(\d+\.\d+)\s+(\d+)', body_text)

        if rating_pattern:
            likes, rating, dislikes = rating_pattern[0]
            return {
                "likes": int(likes),
                "rating": float(rating),
                "dislikes": int(dislikes)
            }

        # Альтернативный паттерн: ищем числа рядом друг с другом
        elements = driver.find_elements(By.XPATH, "//*[text()[contains(., ' ')]]")
        for element in elements:
            text = element.text.strip()
            numbers = re.findall(r'\d+\.?\d*', text)

            # Ищем паттерн: целое число, число с точкой, целое число
            if len(numbers) >= 3:
                for i in range(len(numbers) - 2):
                    if '.' in numbers[i + 1] and '.' not in numbers[i] and '.' not in numbers[i + 2]:
                        return {
                            "likes": int(numbers[i]),
                            "rating": float(numbers[i + 1]),
                            "dislikes": int(numbers[i + 2])
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
                result = extract_rating_numbers(driver, url_key)
                all_results[url_key] = result

                # Чистый вывод
                if result['success']:
                    ratings = result['ratings']
                    if 'error' not in ratings:
                        print(f"Лайки: {ratings['likes']}, Рейтинг: {ratings['rating']}, Дизлайки: {ratings['dislikes']}")
                    else:
                        print(f"Ошибка: {ratings['error']}")
                else:
                    print(f"Ошибка загрузки: {result['error']}")

                print("-" * 50)
                time.sleep(2)

            # Сохранение результатов
            output = {
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "results": all_results
            }

            with open('clean_ratings.json', 'w', encoding='utf-8') as f:
                json.dump(output, f, ensure_ascii=False, indent=2)

            print("Парсинг завершен! Результаты в clean_ratings.json")

    finally:
        driver.quit()


if __name__ == "__main__":
    main()