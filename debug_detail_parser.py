#!/usr/bin/env python3
import sys
import os
import time
import random
from bs4 import BeautifulSoup
from typing import List, Dict, Optional

# Добавляем путь к вашим модулям
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from parser.lordfilm_parser import LordFilmParser


class DebugLordFilmParser(LordFilmParser):
    """Отладочная версия парсера с сохранением содержимого страниц"""

    def _fetch_page_debug(self, url: str) -> str:
        """Загрузка страницы с сохранением содержимого для отладки"""
        try:
            print(f"🔍 Загружаем страницу: {url}")

            # Увеличиваем таймаут для отладки
            self.driver.set_page_load_timeout(120)

            # Загружаем страницу
            self.driver.get(url)

            # Ждем загрузки body (более надежный селектор)
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            from selenium.webdriver.common.by import By

            WebDriverWait(self.driver, 30).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )

            # Дополнительная пауза
            time.sleep(3)

            # Получаем HTML
            html = self.driver.page_source

            # Сохраняем сырой HTML
            filename = f"debug_page_{int(time.time())}.html"
            with open(filename, "w", encoding="utf-8") as f:
                f.write(html)
            print(f"💾 Сырой HTML сохранен в: {filename}")

            # Сохраняем также "красивый" вариант
            soup = BeautifulSoup(html, 'html.parser')
            pretty_filename = f"debug_page_pretty_{int(time.time())}.html"
            with open(pretty_filename, "w", encoding="utf-8") as f:
                f.write(soup.prettify())
            print(f"💾 Форматированный HTML сохранен в: {pretty_filename}")

            # Базовая информация о странице
            print(f"📊 Информация о странице:")
            print(f"   - Заголовок: {self.driver.title}")
            print(f"   - URL: {self.driver.current_url}")
            print(f"   - Длина HTML: {len(html)} символов")

            return html

        except Exception as e:
            print(f"❌ Ошибка загрузки страницы: {e}")

            # Сохраняем хотя бы то, что успели получить
            try:
                html = self.driver.page_source if hasattr(self.driver, 'page_source') else ""
                if html:
                    error_filename = f"debug_page_error_{int(time.time())}.html"
                    with open(error_filename, "w", encoding="utf-8") as f:
                        f.write(html)
                    print(f"💾 HTML при ошибке сохранен в: {error_filename}")
            except:
                pass

            return None

    def parse_movie_details_debug(self, movie_url: str) -> Dict:
        """Отладочная версия парсинга детальной страницы"""
        print(f"\n🎬 НАЧИНАЕМ ПАРСИНГ ДЕТАЛЬНОЙ СТРАНИЦЫ")
        print(f"📝 URL: {movie_url}")
        print("=" * 60)

        # 1. Загружаем и сохраняем страницу
        html = self._fetch_page_debug(movie_url)
        if not html:
            print("❌ Не удалось загрузить страницу")
            return {}

        # 2. Анализируем структуру страницы
        soup = BeautifulSoup(html, 'html.parser')
        self._analyze_page_structure(soup)

        # 3. Пробуем парсить данные
        print(f"\n🔍 ПРОБУЕМ ПАРСИНГ ДАННЫХ:")
        print("-" * 40)

        details = {}

        # Тестируем каждый парсер отдельно
        parsers = [
            ('country', self._parse_detail_country),
            ('genres', self._parse_detail_genres),
            ('director', self._parse_detail_director),
            ('actors', self._parse_detail_actors),
            ('description', self._parse_detail_description),
            ('duration', self._parse_detail_duration),
        ]

        for field_name, parser_func in parsers:
            try:
                result = parser_func(soup)
                details[field_name] = result

                if result:
                    if isinstance(result, list):
                        print(f"✅ {field_name}: {result[:3]}...")  # Показываем первые 3 элемента
                    else:
                        print(f"✅ {field_name}: {result}")
                else:
                    print(f"❌ {field_name}: Не найдено")

            except Exception as e:
                print(f"💥 {field_name}: Ошибка - {e}")
                details[field_name] = None

        print("=" * 60)
        print(f"📊 РЕЗУЛЬТАТ: {len([v for v in details.values() if v])} из {len(parsers)} полей заполнены")

        return details

    def _analyze_page_structure(self, soup: BeautifulSoup):
        """Анализ структуры страницы"""
        print(f"\n🔍 АНАЛИЗ СТРУКТУРЫ СТРАНИЦЫ:")
        print("-" * 40)

        # 1. Заголовки
        title = soup.find('title')
        print(f"📄 Заголовок страницы: {title.get_text() if title else 'Не найден'}")

        h1 = soup.find('h1')
        print(f"📋 H1: {h1.get_text() if h1 else 'Не найден'}")

        # 2. Классы
        all_classes = set()
        for element in soup.find_all(class_=True):
            all_classes.update(element.get('class', []))

        print(f"🏷️ Уникальных классов: {len(all_classes)}")

        # Покажем топ-20 самых частых классов
        class_count = {}
        for element in soup.find_all(class_=True):
            for cls in element.get('class', []):
                class_count[cls] = class_count.get(cls, 0) + 1

        print("📊 Топ-10 классов:")
        for cls, count in sorted(class_count.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"   .{cls}: {count} элементов")

        # 3. Текстовые элементы
        text_elements = list(soup.stripped_strings)
        print(f"📝 Текстовых элементов: {len(text_elements)}")

        # Покажем примеры текста, связанного с фильмами
        movie_keywords = ['рейтинг', 'rating', 'год', 'страна', 'жанр', 'актер', 'режиссер']
        relevant_texts = []

        for text in text_elements:
            if any(keyword in text.lower() for keyword in movie_keywords):
                relevant_texts.append(text)
                if len(relevant_texts) >= 5:
                    break

        if relevant_texts:
            print("🎯 Релевантные текстовые элементы:")
            for i, text in enumerate(relevant_texts, 1):
                print(f"   {i}. {text}")


def main():
    """Основная функция"""
    if len(sys.argv) != 2:
        print("Использование: python debug_detail_parser.py <URL>")
        print("Пример: python debug_detail_parser.py 'https://wh.lordfilm17.ru/filmy/49255-v-ozhidanii-dali-2023.html'")
        return

    url = sys.argv[1]

    print("🎬 ОТЛАДОЧНЫЙ ПАРСЕР ДЕТАЛЬНЫХ СТРАНИЦ")
    print("=" * 60)

    # Создаем отладочный парсер
    parser = DebugLordFilmParser(debug=True)

    try:
        # Парсим детальную информацию
        details = parser.parse_movie_details_debug(url)

        # Выводим итоговый результат
        print(f"\n📋 ИТОГОВЫЕ ДАННЫХ:")
        for key, value in details.items():
            if value:
                if isinstance(value, list):
                    print(f"   {key}: {value}")
                else:
                    print(f"   {key}: {value}")
            else:
                print(f"   {key}: ❌ Не найдено")

    except Exception as e:
        print(f"💥 Критическая ошибка: {e}")
    finally:
        parser.cleanup()
        print("\n✅ Отладка завершена")


if __name__ == "__main__":
    main()