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


def parse_rating(driver, url, target_values):
    """
    Улучшенный парсинг рейтинга с multiple стратегиями поиска
    """
    print(f"Обрабатываю URL: {url}")

    try:
        driver.get(url)

        # Ждем загрузки страницы
        WebDriverWait(driver, 10).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )

        # Даем время для загрузки динамического контента
        time.sleep(3)

        results = {}

        for i, target_value in enumerate(target_values):
            print(f"  Поиск значения: {target_value}")
            found_element = None
            strategy_used = ""
            details = {}

            # Стратегия 1: Приоритетный поиск в специфичных элементах
            if not found_element:
                found_element, strategy_used, details = search_in_priority_elements(driver, target_value)

            # Стратегия 2: Расширенный XPath поиск
            if not found_element:
                found_element, strategy_used, details = search_with_xpath(driver, target_value)

            # Стратегия 3: Поиск в data-атрибутах
            if not found_element:
                found_element, strategy_used, details = search_in_attributes(driver, target_value)

            # Стратегия 4: JavaScript поиск по всему DOM
            if not found_element:
                found_element, strategy_used, details = search_with_javascript(driver, target_value)

            # Стратегия 5: Поиск в meta тегах и скриптах
            if not found_element:
                found_element, strategy_used, details = search_in_metadata(driver, target_value)

            if found_element:
                results[f"value_{i}"] = {
                    "target": target_value,
                    "found": True,
                    "strategy": strategy_used,
                    "details": details
                }
                print(f"    ✓ Найдено ({strategy_used}): {details.get('text', '')}")
            else:
                results[f"value_{i}"] = {
                    "target": target_value,
                    "found": False,
                    "strategies_tried": ["priority_elements", "xpath", "attributes", "javascript", "metadata"]
                }
                print(f"    ✗ Не найдено")

        return {
            "url": url,
            "success": True,
            "results": results
        }

    except Exception as e:
        return {
            "url": url,
            "success": False,
            "error": str(e)
        }


def search_in_priority_elements(driver, target_value):
    """Поиск в элементах, которые обычно содержат рейтинги/голоса"""
    priority_selectors = [
        '.rating', '.score', '.votes', '.rating-value', '.imdb-rating',
        '.kinopoisk-rating', '[class*="rating"]', '[class*="score"]',
        '[class*="vote"]', '.value', '.count', '.number'
    ]

    for selector in priority_selectors:
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            for element in elements:
                text = element.text.strip()
                if target_value in text:
                    return element, "priority_css", {
                        "text": text,
                        "selector": selector,
                        "tag": element.tag_name
                    }
        except:
            continue

    return None, "", {}


def search_with_xpath(driver, target_value):
    """Расширенный XPath поиск с учетом структуры из скриншота"""
    xpath_strategies = [
        # Для чисел, идущих подряд (как в скриншоте)
        f"//*[contains(., ' {target_value} ')]",  # пробелы вокруг
        f"//*[contains(., '{target_value} ')]",  # пробел после
        f"//*[contains(., ' {target_value}')]",  # пробел перед
        f"//*[contains(., '{target_value}')]",  # без пробелов

        # Специфично для структуры "Голоса: 8.2 570 126"
        f"//*[contains(., 'Голоса:') and contains(., '{target_value}')]",
        f"//*[contains(., 'Рейтинг:') and contains(., '{target_value}')]",

        # Поиск в элементах с классом, содержащим rating/votes
        "//*[contains(@class, 'rating')]",
        "//*[contains(@class, 'votes')]",
        "//*[contains(@class, 'score')]",
    ]

    for xpath in xpath_strategies:
        try:
            elements = driver.find_elements(By.XPATH, xpath)
            for element in elements:
                text = element.text.strip()
                if target_value in text:
                    return element, "xpath", {
                        "text": text,
                        "xpath": xpath,
                        "tag": element.tag_name,
                        "full_context": text
                    }
        except:
            continue

    return None, "", {}


def search_rating_block(driver, target_value):
    """Специфичный поиск в блоке рейтинга (по структуре скриншота)"""
    try:
        # Ищем блок, содержащий "Голоса:" и нужное число
        rating_blocks = driver.find_elements(By.XPATH, "//*[contains(., 'Голоса:')]")

        for block in rating_blocks:
            block_text = block.text
            if target_value in block_text:
                # Разбираем блок на составляющие
                numbers = re.findall(r'\d+\.?\d*', block_text)
                return block, "rating_block", {
                    "full_text": block_text,
                    "all_numbers": numbers,
                    "target_position": numbers.index(target_value) if target_value in numbers else -1
                }
    except:
        pass

    return None, "", {}


def search_in_attributes(driver, target_value):
    """Поиск в data-атрибутах и других атрибутах"""
    attributes = ['data-rating', 'data-votes', 'data-score', 'data-value',
                  'data-count', 'rating', 'votes', 'score', 'value', 'content']

    for attr in attributes:
        try:
            # Поиск элементов с атрибутом, содержащим значение
            elements = driver.find_elements(By.XPATH, f"//*[@{attr}='{target_value}']")
            if not elements:
                elements = driver.find_elements(By.XPATH, f"//*[contains(@{attr}, '{target_value}')]")

            if elements:
                element = elements[0]
                attr_value = element.get_attribute(attr)
                return element, "attribute", {
                    "attribute": attr,
                    "value": attr_value,
                    "tag": element.tag_name,
                    "text": element.text.strip() if element.text else ""
                }
        except:
            continue

    return None, "", {}


def search_with_javascript(driver, target_value):
    """JavaScript поиск по всему DOM"""
    js_script = f"""
    function findValueInPage(value) {{
        const results = [];

        // Поиск в текстовых узлах
        const walker = document.createTreeWalker(
            document.body,
            NodeFilter.SHOW_TEXT,
            null,
            false
        );

        let node;
        while (node = walker.nextNode()) {{
            if (node.textContent.includes(value)) {{
                const parent = node.parentElement;
                results.push({{
                    type: 'text',
                    content: node.textContent.trim(),
                    parentHtml: parent.outerHTML.slice(0, 200),
                    tag: parent.tagName
                }});
            }}
        }}

        // Поиск в атрибутах
        const allElements = document.querySelectorAll('*');
        for (const el of allElements) {{
            for (const attr of el.attributes) {{
                if (attr.value.includes(value)) {{
                    results.push({{
                        type: 'attribute',
                        attribute: attr.name,
                        value: attr.value,
                        tag: el.tagName
                    }});
                }}
            }}
        }}

        return results.slice(0, 10); // Ограничиваем количество результатов
    }}

    return findValueInPage('{target_value}');
    """

    try:
        js_results = driver.execute_script(js_script)
        if js_results and len(js_results) > 0:
            return True, "javascript_dom", {
                "matches_found": len(js_results),
                "first_match": js_results[0]
            }
    except:
        pass

    return None, "", {}


def search_in_metadata(driver, target_value):
    """Поиск в meta тегах и script тегах"""
    try:
        # Поиск в meta тегах
        meta_elements = driver.find_elements(By.XPATH, f"//meta[contains(@content, '{target_value}')]")
        if meta_elements:
            element = meta_elements[0]
            return element, "meta_tag", {
                "name": element.get_attribute("name") or element.get_attribute("property"),
                "content": element.get_attribute("content")
            }

        # Поиск в script тегах (переменные JavaScript)
        script_elements = driver.find_elements(By.TAG_NAME, "script")
        for script in script_elements:
            script_content = script.get_attribute("innerHTML")
            if script_content and target_value in script_content:
                return script, "script_tag", {
                    "snippet": script_content[:500]  # Первые 500 символов
                }

    except:
        pass

    return None, "", {}


def search_rating_numbers(driver, target_value):
    """Поиск чисел, которые идут подряд без текста (как в скриншоте)"""
    try:
        # Ищем все элементы с текстом
        all_elements = driver.find_elements(By.XPATH, "//*[text()]")

        for element in all_elements:
            text = element.text.strip()
            # Ищем элементы, которые содержат ТОЛЬКО числа (или наше число)
            if text == target_value or (target_value in text and len(text) < 10):
                # Проверяем, что это похоже на блок рейтинга (рядом есть другие числа)
                parent_text = element.find_element(By.XPATH, "..").text
                numbers_in_parent = re.findall(r'\d+\.?\d*', parent_text)

                if len(numbers_in_parent) >= 2:  # Если в родителе есть несколько чисел
                    return element, "rating_number", {
                        "text": text,
                        "parent_numbers": numbers_in_parent,
                        "tag": element.tag_name
                    }

        # Альтернатива: ищем блоки, где есть несколько чисел подряд
        elements_with_numbers = driver.find_elements(By.XPATH, "//*[text()[contains(., ' ')]]")
        for element in elements_with_numbers:
            text = element.text.strip()
            numbers = re.findall(r'\d+\.?\d*', text)
            if target_value in numbers and len(numbers) >= 2:
                return element, "number_sequence", {
                    "text": text,
                    "all_numbers": numbers,
                    "tag": element.tag_name
                }

    except:
        pass

    return None, "", {}


def search_specific_location(driver, target_value):
    """Поиск в конкретных местах (конец страницы, блоки рейтинга)"""
    try:
        # Стратегия 1: Ищем в нижней части страницы (где обычно рейтинги)
        body = driver.find_element(By.TAG_NAME, "body")
        body_html = body.get_attribute("innerHTML")

        # Ищем паттерн: число, пробел, число, пробел, число
        if target_value in body_html:
            # Находим конкретный элемент с этим числом
            elements = driver.find_elements(By.XPATH, f"//*[text()='{target_value}']")
            if elements:
                return elements[0], "exact_match", {"text": target_value}

        # Стратегия 2: Ищем элементы, содержащие только числа
        elements = driver.find_elements(By.XPATH, f"//*[normalize-space(text())='{target_value}']")
        if elements:
            return elements[0], "exact_text_match", {"text": target_value}

    except:
        pass

    return None, "", {}


def main():
    # Конфигурация (можно вынести в отдельный файл)
    # config = {
    #     "url": ['25', '35', '48'],
    #     "url_2": ['string', 'string', 'string'],
    #     "url_3": ['string', 'string', 'string']
    # }

    # Или загрузка конфига из файла
    with open('config.json', 'r') as f:
        config = json.load(f)

    driver = None
    all_results = {}

    try:
        driver = setup_driver()

        for url_key, target_values in config.items():
            # Если ключ начинается с 'https', считаем его URL
            if url_key.startswith('https'):
                # В реальном сценарии здесь были бы настоящие URL
                # Для примера используем заглушки
                # actual_url = f"https://example.com/{url_key}"

                result = parse_rating(driver, url_key, target_values)
                all_results[url_key] = result

                # Пауза между запросами
                time.sleep(1)

        # Сохраняем результаты
        output = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "results": all_results
        }

        with open('parsing_results.json', 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=2)

        print("\n" + "=" * 50)
        print("ПАРСИНГ ЗАВЕРШЕН")
        print("=" * 50)

        # Краткая статистика
        total_urls = len(all_results)
        successful_urls = sum(1 for r in all_results.values() if r.get('success', False))
        total_values = sum(len(config[key]) for key in config if key.startswith('url'))
        found_values = 0

        for url_result in all_results.values():
            if url_result.get('success') and 'results' in url_result:
                found_values += sum(1 for r in url_result['results'].values() if r.get('found', False))

        print(f"Обработано URL: {successful_urls}/{total_urls}")
        print(f"Найдено значений: {found_values}/{total_values}")
        print(f"Результаты сохранены в: parsing_results.json")

    except Exception as e:
        print(f"Критическая ошибка: {e}")

    finally:
        if driver:
            driver.quit()


if __name__ == "__main__":
    main()