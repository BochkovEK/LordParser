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

        # Стратегия 1: Ищем блоки с числами, похожие на рейтинг
        candidate_elements = find_rating_candidates(driver)

        # Стратегия 2: Анализируем и фильтруем найденные числа
        rating_data = analyze_rating_patterns(candidate_elements)

        return {
            "url": url,
            "success": True,
            "ratings": rating_data
        }

    except Exception as e:
        return {
            "url": url,
            "success": False,
            "error": str(e)
        }


def find_rating_candidates(driver):
    """Находит элементы-кандидаты содержащие числа рейтинга"""
    candidates = []

    # Ищем элементы с небольшим текстом (только числа или короткий текст)
    all_elements = driver.find_elements(By.XPATH, "//*[text()]")

    for element in all_elements:
        try:
            text = element.text.strip()
            if not text or len(text) > 50:  # Слишком длинный текст пропускаем
                continue

            # Ищем числа в тексте
            numbers = re.findall(r'\d+\.?\d*', text)
            if numbers:
                candidates.append({
                    'element': element,
                    'text': text,
                    'numbers': numbers,
                    'tag': element.tag_name,
                    'class': element.get_attribute('class') or ''
                })
        except:
            continue

    return candidates


def analyze_rating_patterns(candidates):
    """Анализирует кандидатов и определяет числа рейтинга"""
    rating_patterns = []

    # Группируем элементы по их расположению (ищем группы чисел)
    position_groups = {}
    for candidate in candidates:
        try:
            location = candidate['element'].location
            y_pos = location['y']

            # Группируем по вертикальной позиции (элементы в одной строке)
            group_key = y_pos // 10  # Группируем с допуском 10px

            if group_key not in position_groups:
                position_groups[group_key] = []
            position_groups[group_key].append(candidate)
        except:
            continue

    # Анализируем группы элементов
    for group_key, group_candidates in position_groups.items():
        if len(group_candidates) >= 2:  # Группа из нескольких чисел
            all_numbers = []
            for candidate in group_candidates:
                all_numbers.extend(candidate['numbers'])

            # Фильтруем по типичным паттернам рейтинга
            if is_rating_pattern(all_numbers):
                rating_patterns.append({
                    'type': 'group_pattern',
                    'numbers': all_numbers,
                    'count': len(group_candidates),
                    'elements': [c['text'] for c in group_candidates]
                })

    # Также ищем одиночные элементы с типичными значениями рейтинга
    for candidate in candidates:
        for number in candidate['numbers']:
            if is_single_rating_value(number):
                rating_patterns.append({
                    'type': 'single_value',
                    'number': number,
                    'context': candidate['text'],
                    'tag': candidate['tag']
                })

    return rating_patterns


def is_rating_pattern(numbers):
    """Определяет, похож ли набор чисел на рейтинг"""
    if len(numbers) < 2:
        return False

    # Паттерн: рейтинг (с точкой) + целые числа
    has_decimal = any('.' in str(num) for num in numbers)
    has_integers = any('.' not in str(num) for num in numbers)

    # Типичные значения рейтинга (0-10)
    rating_values = [float(num) for num in numbers if '.' in str(num) and 0 <= float(num) <= 10]

    return (has_decimal and has_integers) or len(rating_values) > 0


def is_single_rating_value(number_str):
    """Проверяет, похоже ли число на значение рейтинга"""
    try:
        num = float(number_str)
        # Рейтинги обычно от 0 до 10, количество голосов может быть больше
        return 0 <= num <= 10 or (num > 10 and num < 10000)
    except:
        return False


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

                # Вывод предварительных результатов
                if result['success']:
                    print(f"Найдено паттернов: {len(result['ratings'])}")
                    for i, rating in enumerate(result['ratings']):
                        print(f"  Паттерн {i + 1}: {rating}")
                print("-" * 50)

            time.sleep(2)

        # Сохранение результатов
        output = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "results": all_results
        }

        with open('auto_ratings.json', 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=2)

        print("Автопоиск завершен! Результаты в auto_ratings.json")

    finally:
        driver.quit()


if __name__ == "__main__":
    main()