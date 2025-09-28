from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import json
import time
import re
import os
from pathlib import Path

SELENIUM_URL = "http://localhost:4444/wd/hub"
CACHE_DIR = "html_cache"


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


def get_page_content(driver, url):
    """Получает содержимое страницы с кэшированием"""
    # Создаем директорию для кэша
    Path(CACHE_DIR).mkdir(exist_ok=True)

    # Генерируем имя файла из URL
    filename = re.sub(r'[^a-zA-Z0-9]', '_', url) + ".html"
    cache_path = os.path.join(CACHE_DIR, filename)

    # Пробуем загрузить из кэша
    if os.path.exists(cache_path):
        print(f"📁 Загружаем из кэша: {cache_path}")
        with open(cache_path, 'r', encoding='utf-8') as f:
            return f.read()

    # Если нет в кэше, загружаем через Selenium
    print(f"🌐 Загружаем через Selenium: {url}")
    driver.get(url)
    WebDriverWait(driver, 10).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )
    time.sleep(3)

    content = driver.page_source

    # Сохраняем в кэш
    with open(cache_path, 'w', encoding='utf-8') as f:
        f.write(content)
    print(f"💾 Сохранено в кэш: {cache_path}")

    return content


def extract_movie_data_with_targets(driver, url, target_values):
    """
    Извлекает информацию о фильме с целевым поиском рейтинга
    """
    print(f"🎯 Анализируем URL: {url}")
    print(f"🎯 Целевые значения: лайки={target_values[0]}, рейтинг={target_values[1]}, дизлайки={target_values[2]}")

    try:
        # Получаем содержимое страницы (из кэша или через Selenium)
        page_content = get_page_content(driver, url)

        # Извлекаем рейтинг LordFilm с анализом стратегий
        ratings_analysis = extract_ratings_with_analysis(page_content, target_values)

        return {
            "url": url,
            "success": True,
            "target_values": target_values,
            "ratings_analysis": ratings_analysis
        }

    except Exception as e:
        return {
            "url": url,
            "success": False,
            "error": str(e)
        }


def extract_ratings_with_analysis(page_content, target_values):
    """
    Анализирует стратегии поиска рейтинга с целевыми значениями
    """
    target_likes, target_rating, target_dislikes = target_values
    analysis = {
        'strategies_tried': [],
        'best_match': None,
        'accuracy_score': 0,
        'found_values': None
    }

    try:
        # Извлекаем текст из HTML
        body_text = extract_text_from_html(page_content)

        # СТРАТЕГИЯ 1: Исходный способ (без проверки валидности)
        strategy1_matches = re.findall(r'(\d+)\s+(\d+\.\d+)\s+(\d+)', body_text)
        analysis['strategies_tried'].append({
            'name': '1. Исходный паттерн (число число.число число)',
            'matches': strategy1_matches,
            'score': len(strategy1_matches)  # Просто количество найденных совпадений
        })

        # СТРАТЕГИЯ 2: Паттерн для целых чисел
        strategy2_matches = re.findall(r'(\d+)\s+(\d+)\s+(\d+)', body_text)
        analysis['strategies_tried'].append({
            'name': '2. Паттерн целые числа (число число число)',
            'matches': strategy2_matches,
            'score': len(strategy2_matches)
        })

        # СТРАТЕГИЯ 3: Паттерн для нулевых рейтингов
        strategy3_matches = re.findall(r'(\d+)\s+0\s+(\d+)', body_text)
        analysis['strategies_tried'].append({
            'name': '3. Паттерн нулевой рейтинг (число 0 число)',
            'matches': strategy3_matches,
            'score': len(strategy3_matches)
        })

        # СТРАТЕГИЯ 4: Паттерн для нулевых рейтингов с точкой
        strategy4_matches = re.findall(r'(\d+)\s+0\.0\s+(\d+)', body_text)
        analysis['strategies_tried'].append({
            'name': '4. Паттерн нулевой рейтинг (число 0.0 число)',
            'matches': strategy4_matches,
            'score': len(strategy4_matches)
        })

        # СТРАТЕГИЯ 5: Гибкий паттерн с разными разделителями
        strategy5_matches = re.findall(r'(\d+)[\s\-]+(\d+\.?\d*)[\s\-]+(\d+)', body_text)
        analysis['strategies_tried'].append({
            'name': '5. Гибкий паттерн (разные разделители)',
            'matches': strategy5_matches,
            'score': len(strategy5_matches)
        })

        # СТРАТЕГИЯ 6: Поиск в HTML атрибутах
        strategy6_matches = find_ratings_in_attributes(page_content)
        analysis['strategies_tried'].append({
            'name': '6. Поиск в data-атрибутах',
            'matches': strategy6_matches,
            'score': len(strategy6_matches)
        })

        # СТРАТЕГИЯ 7: Поиск в meta-тегах
        strategy7_matches = find_ratings_in_meta(page_content)
        analysis['strategies_tried'].append({
            'name': '7. Поиск в meta-тегах',
            'matches': strategy7_matches,
            'score': len(strategy7_matches)
        })

        # СТРАТЕГИЯ 8: Поиск в script-тегах
        strategy8_matches = find_ratings_in_scripts(page_content)
        analysis['strategies_tried'].append({
            'name': '8. Поиск в JavaScript',
            'matches': strategy8_matches,
            'score': len(strategy8_matches)
        })

        # СТРАТЕГИЯ 9: Поиск по классам в HTML
        strategy9_matches = find_ratings_in_classes(page_content)
        analysis['strategies_tried'].append({
            'name': '9. Поиск по CSS классам',
            'matches': strategy9_matches,
            'score': len(strategy9_matches)
        })

        # СТРАТЕГИЯ 10: Поиск конкретных целевых чисел
        strategy10_matches = find_specific_numbers_in_text(body_text, target_values)
        analysis['strategies_tried'].append({
            'name': '10. Поиск целевых чисел',
            'matches': strategy10_matches,
            'score': len(strategy10_matches)
        })

        # Находим стратегию с наибольшим количеством совпадений
        best_strategy = max(analysis['strategies_tried'], key=lambda x: x['score'])
        analysis['best_match'] = best_strategy
        analysis['accuracy_score'] = best_strategy['score']

        # Берем первое найденное совпадение из лучшей стратегии
        if best_strategy['matches']:
            best_match = best_strategy['matches'][0]
            try:
                analysis['found_values'] = {
                    'likes': int(best_match[0]),
                    'rating': float(best_match[1]),
                    'dislikes': int(best_match[2])
                }
            except (ValueError, IndexError):
                pass

        # Дополнительная диагностика
        analysis['debug_info'] = {
            'all_triple_patterns': re.findall(r'(\d+)\s+(\S+)\s+(\d+)', body_text)[:5],
            'total_triples_found': len(re.findall(r'(\d+)\s+(\S+)\s+(\d+)', body_text)),
            'body_text_length': len(body_text)
        }

    except Exception as e:
        analysis['error'] = str(e)

    return analysis


def extract_text_from_html(html_content):
    """Извлекает текст из HTML содержимого"""
    # Простой способ извлечения текста (убираем теги)
    text = re.sub(r'<[^>]+>', ' ', html_content)
    # Убираем лишние пробелы
    text = re.sub(r'\s+', ' ', text)
    return text


def find_ratings_in_attributes(html_content):
    """Ищет рейтинги в data-атрибутах"""
    matches = []
    try:
        # Ищем в data-атрибутах
        data_patterns = [
            r'data-likes=["\']?(\d+)["\']?.*?data-rating=["\']?(\d+\.?\d*)["\']?.*?data-dislikes=["\']?(\d+)["\']?',
            r'data-rating=["\']?(\d+\.?\d*)["\']?.*?data-likes=["\']?(\d+)["\']?.*?data-dislikes=["\']?(\d+)["\']?',
        ]

        for pattern in data_patterns:
            found = re.findall(pattern, html_content, re.DOTALL)
            if found:
                matches.extend(found)

    except Exception as e:
        print(f"Ошибка поиска в атрибутах: {e}")

    return matches


def find_ratings_in_meta(html_content):
    """Ищет рейтинги в meta-тегах"""
    matches = []
    try:
        meta_patterns = [
            r'<meta[^>]*name=["\']?rating["\'][^>]*content=["\']?(\d+\.?\d*)["\']',
            r'<meta[^>]*property=["\']?og:rating["\'][^>]*content=["\']?(\d+\.?\d*)["\']',
        ]

        for pattern in meta_patterns:
            found = re.findall(pattern, html_content)
            for rating in found:
                # Для meta обычно только рейтинг, добавляем заглушки для лайков/дизлайков
                matches.append(('0', rating, '0'))

    except Exception as e:
        print(f"Ошибка поиска в meta: {e}")

    return matches


def find_ratings_in_scripts(html_content):
    """Ищет рейтинги в JavaScript коде"""
    matches = []
    try:
        # Ищем в script тегах
        script_patterns = [
            r'likes[\s:=\-]+(\d+).*?rating[\s:=\-]+(\d+\.?\d*).*?dislikes[\s:=\-]+(\d+)',
            r'rating[\s:=\-]+(\d+\.?\d*).*?likes[\s:=\-]+(\d+).*?dislikes[\s:=\-]+(\d+)',
        ]

        scripts = re.findall(r'<script[^>]*>(.*?)</script>', html_content, re.DOTALL)
        for script in scripts:
            for pattern in script_patterns:
                found = re.findall(pattern, script, re.DOTALL)
                if found:
                    matches.extend(found)

    except Exception as e:
        print(f"Ошибка поиска в scripts: {e}")

    return matches


def find_ratings_in_classes(html_content):
    """Ищет рейтинги по CSS классам"""
    matches = []
    try:
        # Ищем элементы с классами рейтингов
        class_patterns = [
            r'class=["\'][^"\']*rating[^"\']*["\'][^>]*>.*?(\d+).*?(\d+\.?\d*).*?(\d+)',
            r'class=["\'][^"\']*like[^"\']*["\'][^>]*>.*?(\d+).*?class=["\'][^"\']*rating[^"\']*["\'][^>]*>.*?(\d+\.?\d*).*?class=["\'][^"\']*dislike[^"\']*["\'][^>]*>.*?(\d+)',
        ]

        for pattern in class_patterns:
            found = re.findall(pattern, html_content, re.DOTALL)
            if found:
                matches.extend(found)

    except Exception as e:
        print(f"Ошибка поиска в классах: {e}")

    return matches


def find_specific_numbers_in_text(body_text, target_values):
    """Ищет конкретные целевые числа в тексте"""
    matches = []
    try:
        target_likes, target_rating, target_dislikes = target_values

        # Ищем комбинации где есть наши целевые числа
        patterns = [
            rf'({target_likes})\s+({target_rating})\s+({target_dislikes})',
            rf'({target_likes})\s+(\d+\.?\d*)\s+({target_dislikes})',
            rf'(\d+)\s+({target_rating})\s+(\d+)'
        ]

        for pattern in patterns:
            found = re.findall(pattern, body_text)
            if found:
                matches.extend(found)

    except Exception as e:
        print(f"Ошибка поиска конкретных чисел: {e}")

    return matches


# Основная функция (остается без изменений)
def main():
    with open('config.json', 'r') as f:
        config = json.load(f)

    driver = setup_driver()

    try:
        for url, target_values in config.items():
            if url.startswith('https'):
                result = extract_movie_data_with_targets(driver, url, target_values)

                if result['success']:
                    analysis = result['ratings_analysis']
                    targets = result['target_values']

                    print(f"\n🎯 РЕЗУЛЬТАТЫ ДЛЯ: {url}")
                    print(f"🎯 Целевые значения: лайки={targets[0]}, рейтинг={targets[1]}, дизлайки={targets[2]}")
                    print(f"📊 Лучшая стратегия нашла: {analysis['accuracy_score']} совпадений")

                    if analysis['found_values']:
                        found = analysis['found_values']
                        print(
                            f"✅ Найдено: лайки={found['likes']}, рейтинг={found['rating']}, дизлайки={found['dislikes']}")

                    print(f"\n🔍 ВСЕ СТРАТЕГИИ:")
                    for strategy in analysis['strategies_tried']:
                        status = "✅" if strategy['score'] > 0 else "❌"
                        print(f"   {status} {strategy['name']}: {strategy['score']} совпадений")
                        if strategy['matches']:
                            print(f"      Пример: {strategy['matches'][0]}")

                    # Показываем диагностику
                    if 'debug_info' in analysis:
                        debug = analysis['debug_info']
                        print(f"\n🔧 ДИАГНОСТИКА:")
                        print(f"   Примеры троек: {debug.get('all_triple_patterns', [])}")
                        print(f"   Всего троек: {debug.get('total_triples_found', 0)}")

                else:
                    print(f"❌ Ошибка: {result['error']}")

                print("=" * 80)
                time.sleep(1)  # Уменьшили паузу для кэшированных запросов

    finally:
        driver.quit()


if __name__ == "__main__":
    main()