from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import json
import time
# import os


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
    """Расширенный XPath поиск"""
    xpath_strategies = [
        # Точное совпадение текста
        f"//*[text()='{target_value}']",
        # Содержит значение как подстроку
        f"//*[contains(text(), '{target_value}')]",
        # Ищем числа, окруженные пробелами/знаками препинания
        f"//*[contains(., ' {target_value} ')]",
        f"//*[contains(., '{target_value}.')]",
        f"//*[contains(., '{target_value},')]",
        # Поиск в span, div, strong, b (часто содержат числа)
        f"//span[contains(., '{target_value}')]",
        f"//div[contains(., '{target_value}')]",
        f"//strong[contains(., '{target_value}')]",
        f"//b[contains(., '{target_value}')]",
    ]

    for xpath in xpath_strategies:
        try:
            elements = driver.find_elements(By.XPATH, xpath)
            if elements:
                element = elements[0]
                return element, "xpath", {
                    "text": element.text.strip(),
                    "xpath": xpath,
                    "tag": element.tag_name
                }
        except:
            continue

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