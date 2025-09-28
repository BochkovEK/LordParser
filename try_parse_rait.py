from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import json
import time
import re

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


def extract_movie_data_with_targets(driver, url, target_values):
    """
    Извлекает информацию о фильме с целевым поиском рейтинга
    target_values: [лайки, рейтинг, дизлайки]
    """
    print(f"🎯 Анализируем URL: {url}")
    print(f"🎯 Целевые значения: лайки={target_values[0]}, рейтинг={target_values[1]}, дизлайки={target_values[2]}")

    try:
        driver.get(url)
        WebDriverWait(driver, 10).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        time.sleep(3)

        # Извлекаем рейтинг LordFilm с анализом стратегий
        ratings_analysis = extract_ratings_with_analysis(driver, target_values)

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


def extract_ratings_with_analysis(driver, target_values):
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
        body_text = driver.find_element(By.TAG_NAME, "body").text

        # Стратегия 1: Оригинальный рабочий паттерн "число число.число число"
        strategy1_matches = re.findall(r'(\d+)\s+(\d+\.\d+)\s+(\d+)', body_text)
        analysis['strategies_tried'].append({
            'name': 'Оригинальный паттерн (число число.число число)',
            'matches': strategy1_matches,
            'score': calculate_accuracy(strategy1_matches, target_values)
        })

        # Стратегия 2: Паттерн для нулевых рейтингов "число 0 число"
        strategy2_matches = re.findall(r'(\d+)\s+0\s+(\d+)', body_text)
        analysis['strategies_tried'].append({
            'name': 'Паттерн для нулевых рейтингов (число 0 число)',
            'matches': strategy2_matches,
            'score': calculate_accuracy(strategy2_matches, target_values, is_zero_rating=True)
        })

        # Стратегия 3: Паттерн "число 0.0 число"
        strategy3_matches = re.findall(r'(\d+)\s+0\.0\s+(\d+)', body_text)
        analysis['strategies_tried'].append({
            'name': 'Паттерн для нулевых рейтингов (число 0.0 число)',
            'matches': strategy3_matches,
            'score': calculate_accuracy(strategy3_matches, target_values, is_zero_rating=True)
        })

        # Стратегия 4: Поиск трех чисел через разные разделители
        strategy4_matches = re.findall(r'(\d+)[\s\-]+(\d+\.?\d*)[\s\-]+(\d+)', body_text)
        analysis['strategies_tried'].append({
            'name': 'Гибкий паттерн (разные разделители)',
            'matches': strategy4_matches,
            'score': calculate_accuracy(strategy4_matches, target_values)
        })

        # Стратегия 5: Поиск в элементах с классами rating (более агрессивный)
        strategy5_matches = find_rating_elements_aggressive(driver, target_values)
        analysis['strategies_tried'].append({
            'name': 'Агрессивный поиск в элементах',
            'matches': strategy5_matches,
            'score': calculate_accuracy(strategy5_matches, target_values)
        })

        # Стратегия 6: Поиск по всему DOM с приоритетом видимых элементов
        strategy6_matches = find_visible_ratings(driver, target_values)
        analysis['strategies_tried'].append({
            'name': 'Поиск в видимых элементах',
            'matches': strategy6_matches,
            'score': calculate_accuracy(strategy6_matches, target_values)
        })

        # Стратегия 7: Поиск конкретных чисел из целевых значений
        strategy7_matches = find_specific_numbers(driver, target_values)
        analysis['strategies_tried'].append({
            'name': 'Поиск конкретных целевых чисел',
            'matches': strategy7_matches,
            'score': calculate_accuracy(strategy7_matches, target_values)
        })

        # Находим лучшую стратегию
        best_strategy = max(analysis['strategies_tried'], key=lambda x: x['score'])
        analysis['best_match'] = best_strategy
        analysis['accuracy_score'] = best_strategy['score']

        if best_strategy['matches'] and best_strategy['score'] > 0:
            # Берем первое наилучшее совпадение
            best_match = best_strategy['matches'][0]
            try:
                analysis['found_values'] = {
                    'likes': int(best_match[0]),
                    'rating': float(best_match[1]) if '.' in str(best_match[1]) else float(best_match[1]),
                    'dislikes': int(best_match[2])
                }
            except (ValueError, IndexError):
                pass

        # Дополнительная диагностика
        analysis['debug_info'] = {
            'all_triple_patterns': re.findall(r'(\d+)\s+(\S+)\s+(\d+)', body_text),
            'all_double_patterns': re.findall(r'(\d+)\s+(\S+)', body_text),
            'all_rating_elements': len(driver.find_elements(By.CSS_SELECTOR, "[class*='rating'], [class*='vote']")),
            'body_text_sample': body_text[:500] + "..." if len(body_text) > 500 else body_text
        }

    except Exception as e:
        analysis['error'] = str(e)

    return analysis


def calculate_accuracy(matches, target_values, is_zero_rating=False):
    """Вычисляет точность совпадения с целевыми значениями"""
    if not matches:
        return 0

    target_likes, target_rating, target_dislikes = target_values
    best_score = 0

    for match in matches:
        try:
            if len(match) < 3:
                continue

            found_likes = int(match[0])

            # Обрабатываем рейтинг (может быть "0", "0.0", "5.4")
            rating_str = str(match[1])
            if '.' in rating_str:
                found_rating = float(rating_str)
            else:
                found_rating = float(rating_str)  # "0" -> 0.0

            found_dislikes = int(match[2])

            # Для нулевых рейтингов более строгая проверка
            if is_zero_rating:
                likes_score = 1 if found_likes == target_likes else 0
                rating_score = 1 if found_rating == 0 else 0
                dislikes_score = 1 if found_dislikes == target_dislikes else 0
            else:
                # Для ненулевых рейтингов допускаем небольшие отклонения
                likes_score = 1 if found_likes == target_likes else 0.3 if abs(found_likes - target_likes) <= 5 else 0
                rating_score = 1 if abs(found_rating - target_rating) < 0.1 else 0.5 if abs(
                    found_rating - target_rating) < 1 else 0
                dislikes_score = 1 if found_dislikes == target_dislikes else 0.3 if abs(
                    found_dislikes - target_dislikes) <= 5 else 0

            total_score = (likes_score + rating_score + dislikes_score) / 3
            best_score = max(best_score, total_score)

        except (ValueError, IndexError):
            continue

    return best_score


def find_rating_elements_aggressive(driver, target_values):
    """Агрессивный поиск в элементах с разными классами"""
    matches = []
    try:
        # Расширенный список классов для поиска
        class_selectors = [
            "[class*='rating']", "[class*='vote']", "[class*='like']", "[class*='dislike']",
            "[class*='score']", "[class*='rate']", "[class*='stat']", "[class*='count']"
        ]

        for selector in class_selectors:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            for element in elements:
                text = element.text.strip()
                # Ищем три числа
                numbers = re.findall(r'\d+', text)
                if len(numbers) >= 3:
                    matches.append(numbers[:3])
                    # Также пробуем найти числа с точками
                    decimal_match = re.findall(r'(\d+)\s+(\d+\.\d+)\s+(\d+)', text)
                    if decimal_match:
                        matches.extend(decimal_match)

    except Exception as e:
        print(f"Ошибка агрессивного поиска: {e}")

    return matches


def find_visible_ratings(driver, target_values):
    """Поиск в видимых элементах (не скрытых)"""
    matches = []
    try:
        # Ищем все элементы с числами
        elements = driver.find_elements(By.XPATH, "//*[text()[contains(., ' ')]]")

        for element in elements:
            if element.is_displayed():  # Только видимые элементы
                text = element.text.strip()
                # Ищем разные паттерны
                patterns = [
                    r'(\d+)\s+(\d+\.\d+)\s+(\d+)',
                    r'(\d+)\s+(\d+)\s+(\d+)',
                    r'(\d+)\s+0\s+(\d+)',
                    r'(\d+)\s+0\.0\s+(\d+)'
                ]
                for pattern in patterns:
                    found = re.findall(pattern, text)
                    if found:
                        matches.extend(found)

    except Exception as e:
        print(f"Ошибка поиска в видимых элементах: {e}")

    return matches


def find_specific_numbers(driver, target_values):
    """Поиск конкретных целевых чисел на странице"""
    matches = []
    try:
        target_likes, target_rating, target_dislikes = target_values
        body_text = driver.find_element(By.TAG_NAME, "body").text

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


def calculate_accuracy(matches, target_values, is_integer=False):
    """Вычисляет точность совпадения с целевыми значениями"""
    if not matches:
        return 0

    target_likes, target_rating, target_dislikes = target_values
    best_score = 0

    for match in matches:
        try:
            found_likes = int(match[0])
            found_rating = float(match[1]) if not is_integer else float(match[1])
            found_dislikes = int(match[2])

            # Вычисляем точность для каждого значения
            likes_score = 1 if found_likes == target_likes else 0.5 if abs(found_likes - target_likes) <= 10 else 0
            rating_score = 1 if abs(found_rating - target_rating) < 0.1 else 0.5 if abs(
                found_rating - target_rating) < 1 else 0
            dislikes_score = 1 if found_dislikes == target_dislikes else 0.5 if abs(
                found_dislikes - target_dislikes) <= 10 else 0

            total_score = (likes_score + rating_score + dislikes_score) / 3
            best_score = max(best_score, total_score)

        except (ValueError, IndexError):
            continue

    return best_score


def find_adjacent_numbers(driver, target_values):
    """Ищет три числа расположенных рядом в DOM"""
    matches = []
    try:
        # Ищем элементы содержащие числа
        elements_with_numbers = driver.find_elements(By.XPATH, "//*[text()[contains(., ' ')]]")

        for element in elements_with_numbers:
            text = element.text.strip()
            # Ищем три числа подряд
            numbers = re.findall(r'\d+', text)
            if len(numbers) >= 3:
                # Проверяем разные комбинации трех чисел
                for i in range(len(numbers) - 2):
                    triple = numbers[i:i + 3]
                    matches.append(triple)

    except Exception as e:
        print(f"Ошибка поиска соседних чисел: {e}")

    return matches


def find_rating_elements(driver, target_values):
    """Ищет рейтинги в элементах с определенными классами"""
    matches = []
    try:
        rating_elements = driver.find_elements(By.CSS_SELECTOR,
                                               "[class*='rating'], [class*='like'], [class*='dislike'], [class*='vote']")

        for element in rating_elements:
            text = element.text.strip()
            numbers = re.findall(r'\d+', text)
            if len(numbers) >= 3:
                matches.append(numbers[:3])

    except Exception as e:
        print(f"Ошибка поиска в элементах рейтинга: {e}")

    return matches


# Основная функция
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
                    print(f"📊 Точность: {analysis['accuracy_score']:.2%}")

                    if analysis['found_values']:
                        found = analysis['found_values']
                        target_likes, target_rating, target_dislikes = targets

                        print(
                            f"✅ Найдено: лайки={found['likes']}, рейтинг={found['rating']}, дизлайки={found['dislikes']}")

                        # Сравнение с целевыми значениями
                        print(f"📈 Сравнение:")
                        print(
                            f"   Лайки: найдено {found['likes']} vs целевое {target_likes} {'✅' if found['likes'] == target_likes else '❌'}")
                        print(
                            f"   Рейтинг: найдено {found['rating']} vs целевое {target_rating} {'✅' if abs(found['rating'] - target_rating) < 0.1 else '❌'}")
                        print(
                            f"   Дизлайки: найдено {found['dislikes']} vs целевое {target_dislikes} {'✅' if found['dislikes'] == target_dislikes else '❌'}")

                    print(f"\n🔍 СТРАТЕГИИ ПОИСКА:")
                    for strategy in analysis['strategies_tried']:
                        status = "✅" if strategy['score'] > 0.8 else "⚠️" if strategy['score'] > 0.3 else "❌"
                        print(f"   {status} {strategy['name']}: {strategy['score']:.2%}")
                        if strategy['matches']:
                            print(f"      Совпадения: {strategy['matches'][:2]}")  # Показываем первые 2

                    # Показываем дополнительную диагностику
                    if 'debug_info' in analysis:
                        debug = analysis['debug_info']
                        print(f"\n🔧 ДИАГНОСТИКА:")
                        print(f"   Все тройки чисел: {debug.get('all_triple_numbers', [])[:3]}")
                        print(f"   Элементов с rating: {debug.get('all_rating_elements', 0)}")

                    if analysis['accuracy_score'] < 0.8:
                        print(f"\n🔧 РЕКОМЕНДАЦИЯ: Нужно улучшить стратегию поиска")

                else:
                    print(f"❌ Ошибка: {result['error']}")

                print("=" * 80)
                time.sleep(2)

    finally:
        driver.quit()

if __name__ == "__main__":
    main()