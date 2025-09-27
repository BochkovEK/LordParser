from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import json
import time
# import os

# # Загружаем конфиг один раз при импорте
# with open('config.json', 'r') as f:
#     CONFIG = json.load(f)

# SELENIUM_URL = os.getenv('SELENIUM_URL', CONFIG['selenium_url'])

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
    Парсит рейтинг со страницы, ища целевые значения в DOM
    """
    print(f"Обрабатываю URL: {url}")

    try:
        driver.get(url)

        # Ждем загрузки страницы
        WebDriverWait(driver, 5).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )

        # Даем время для загрузки динамического контента
        time.sleep(3)

        results = {}

        for i, target_value in enumerate(target_values):
            print(f"  Поиск значения: {target_value}")

            # Стратегия 1: Ищем элемент, содержащий целевое значение
            try:
                # XPath для поиска элемента, содержащего текст
                xpath = f"//*[contains(text(), '{target_value}')]"
                element = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, xpath))
                )
                results[f"value_{i}"] = {
                    "target": target_value,
                    "found": True,
                    "element_text": element.text,
                    "tag_name": element.tag_name,
                    "strategy": "xpath_text_search"
                }
                print(f"    ✓ Найдено через XPath: {element.text}")

            except Exception as e:
                # Стратегия 2: Ищем в JavaScript переменных
                try:
                    # Пробуем найти значение в глобальных переменных JS
                    js_script = f"""
                    var results = [];
                    for (var key in window) {{
                        try {{
                            if (typeof window[key] === 'string' && window[key].includes('{target_value}')) {{
                                results.push({{key: key, value: window[key]}});
                            }}
                            else if (typeof window[key] === 'number' && window[key].toString() === '{target_value}') {{
                                results.push({{key: key, value: window[key]}});
                            }}
                        }} catch(e) {{}}
                    }}
                    return results.length > 0 ? results : null;
                    """

                    js_result = driver.execute_script(js_script)
                    if js_result:
                        results[f"value_{i}"] = {
                            "target": target_value,
                            "found": True,
                            "js_variables": js_result,
                            "strategy": "javascript_global_vars"
                        }
                        print(f"    ✓ Найдено в JS переменных: {js_result}")
                    else:
                        raise Exception("Не найдено в JS переменных")

                except Exception as js_e:
                    # Стратегия 3: Ищем в тексте страницы
                    page_source = driver.page_source
                    if target_value in page_source:
                        results[f"value_{i}"] = {
                            "target": target_value,
                            "found": True,
                            "location": "page_source",
                            "strategy": "source_code_search"
                        }
                        print(f"    ✓ Найдено в исходном коде")
                    else:
                        results[f"value_{i}"] = {
                            "target": target_value,
                            "found": False,
                            "error": str(e),
                            "strategy": "all_methods_failed"
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