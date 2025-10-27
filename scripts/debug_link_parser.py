#!/usr/bin/env python3
"""
Диагностика парсера ссылок
"""
import sys
import os
from selenium.webdriver.common.by import By
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.parser.link_parser import LinkParser


def debug_page_parsing():
    """Диагностика парсинга страницы"""
    parser = LinkParser()

    try:
        parser.setup_driver()

        # Тестовый URL
        test_urls = [
            "https://wk.lordfilm17.ru/filmy/2025/page/1/",
            "https://wk.lordfilm17.ru",
            "https://wk.lordfilm17.ru/filmy/2024/page/5/", # другой год
        ]

        for url in test_urls:
            print(f"\n🔍 Анализируем: {url}")
            print("=" * 50)

            try:
                parser.driver.get(url)
                time.sleep(3)

                # Сохраняем HTML для анализа
                html_content = parser.driver.page_source
                with open(f"debug_page.html", "w", encoding="utf-8") as f:
                    f.write(html_content)
                print(f"💾 HTML сохранен в debug_page.html")

                # Проверяем основные элементы (современные методы Selenium)
                print("📊 Проверка элементов страницы:")

                # 1. Проверяем заголовок
                title = parser.driver.title
                print(f"   📝 Заголовок страницы: {title}")

                # 2. Проверяем body
                body = parser.driver.find_element(By.TAG_NAME, "body")
                body_text = body.text[:200]
                print(f"   📄 Текст body (первые 200 символов): {body_text}...")

                # 3. Ищем любые ссылки с 'film' в href
                film_links = parser.driver.find_elements(By.XPATH, "//a[contains(@href, 'film')]")
                print(f"   🔗 Найдено ссылок с 'film': {len(film_links)}")

                # 4. Ищем карточки фильмов по разным селекторам
                selectors = [
                    "div.movie-item",
                    "div.film-item",
                    "div.item",
                    "article.movie",
                    "div.card",
                    "a[href*='film']",
                    ".th-item",  # возможные селекторы LordFilm
                    ".th-one",
                    ".th-title",
                    ".th-in",
                    "div.th"
                ]

                for selector in selectors:
                    try:
                        elements = parser.driver.find_elements(By.CSS_SELECTOR, selector)
                        if elements:
                            print(f"   ✅ Селектор '{selector}': найдено {len(elements)} элементов")
                            # Покажем первые 3 элемента
                            for i, elem in enumerate(elements[:3]):
                                try:
                                    href = elem.get_attribute("href")
                                    if not href:  # если у самого элемента нет href, ищем внутри
                                        link = elem.find_element(By.CSS_SELECTOR, "a")
                                        href = link.get_attribute("href")
                                    text = elem.text[:50] if elem.text else "no text"
                                    print(f"      {i + 1}. {href} -> '{text}...'")
                                except:
                                    print(f"      {i + 1}. [не удалось извлечь ссылку] -> '{elem.text[:50]}...'")
                    except Exception as e:
                        print(f"   ❌ Ошибка с селектором '{selector}': {e}")

                # 5. Проверяем наличие блокировок
                page_text = body.text
                blocking_indicators = ["Доступ запрещен", "404", "Not Found", "Cloudflare", "Капча"]
                for indicator in blocking_indicators:
                    if indicator in page_text:
                        print(f"   🚫 Обнаружен индикатор блокировки: {indicator}")

                # 6. Проверяем есть ли вообще контент на странице
                if len(body_text.strip()) < 50:
                    print("   ⚠️ Страница почти пустая - возможна блокировка")

            except Exception as e:
                print(f"   ❌ Ошибка при анализе {url}: {e}")
                import traceback
                traceback.print_exc()

    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
    finally:
        parser.close()


if __name__ == "__main__":
    debug_page_parsing()