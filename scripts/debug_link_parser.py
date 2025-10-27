#!/usr/bin/env python3
"""
Диагностика парсера ссылок
"""
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.parser.link_parser import LinkParser
import time


def debug_page_parsing():
    """Диагностика парсинга страницы"""
    parser = LinkParser()

    try:
        parser.setup_driver()

        # Тестовый URL - попробуем разные варианты
        test_urls = [
            "https://wk.lordfilm17.ru/filmy/2025/page/1/",
            "https://lordfilm.works/films/",  # главная страница
            "https://lordfilm.works/films/2024/",  # другой год
        ]

        for url in test_urls:
            print(f"\n🔍 Анализируем: {url}")
            print("=" * 50)

            try:
                parser.driver.get(url)
                time.sleep(3)

                # Сохраняем HTML для анализа
                html_content = parser.driver.page_source
                with open(f"debug_{url.split('/')[-2]}.html", "w", encoding="utf-8") as f:
                    f.write(html_content)
                print(f"💾 HTML сохранен в debug_{url.split('/')[-2]}.html")

                # Проверяем основные элементы
                print("📊 Проверка элементов страницы:")

                # 1. Проверяем заголовок
                title = parser.driver.title
                print(f"   📝 Заголовок страницы: {title}")

                # 2. Проверяем body
                body_text = parser.driver.find_element_by_tag_name("body").text[:200]
                print(f"   📄 Текст body (первые 200 символов): {body_text}...")

                # 3. Ищем любые ссылки с 'film' в href
                film_links = parser.driver.find_elements_by_xpath("//a[contains(@href, 'film')]")
                print(f"   🔗 Найдено ссылок с 'film': {len(film_links)}")

                # 4. Ищем карточки фильмов по разным селекторам
                selectors = [
                    "div.movie-item",
                    "div.film-item",
                    "div.item",
                    "article.movie",
                    "div.card",
                    "a[href*='film']"
                ]

                for selector in selectors:
                    elements = parser.driver.find_elements_by_css_selector(selector)
                    if elements:
                        print(f"   ✅ Селектор '{selector}': найдено {len(elements)} элементов")
                        # Покажем первые 3 ссылки
                        for i, elem in enumerate(elements[:3]):
                            href = elem.get_attribute("href")
                            text = elem.text[:50] if elem.text else "no text"
                            print(f"      {i + 1}. {href} -> '{text}...'")

                # 5. Проверяем наличие блокировок
                page_text = parser.driver.find_element_by_tag_name("body").text
                blocking_indicators = ["Доступ запрещен", "404", "Not Found", "Cloudflare"]
                for indicator in blocking_indicators:
                    if indicator in page_text:
                        print(f"   🚫 Обнаружен индикатор блокировки: {indicator}")

            except Exception as e:
                print(f"   ❌ Ошибка при анализе {url}: {e}")

    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
    finally:
        parser.close()


if __name__ == "__main__":
    debug_page_parsing()