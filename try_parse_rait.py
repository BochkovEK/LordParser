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

        # Стратегия 1: Точный паттерн "число число.число число"
        strategy1_matches = re.findall(r'(\d+)\s+(\d+\.\d+)\s+(\d+)', body_text)
        analysis['strategies_tried'].append({
            'name': 'Тройной паттерн с точкой',
            'matches': strategy1_matches,
            'score': calculate_accuracy(strategy1_matches, target_values)
        })

        # Стратегия 2: Паттерн "число число число" (для целых рейтингов)
        strategy2_matches = re.findall(r'(\d+)\s+(\d+)\s+(\d+)', body_text)
        analysis['strategies_tried'].append({
            'name': 'Тройной паттерн целые числа',
            'matches': strategy2_matches,
            'score': calculate_accuracy(strategy2_matches, target_values, is_integer=True)
        })

        # Стратегия 3: Поиск отдельных чисел рядом
        strategy3_matches = find_adjacent_numbers(driver, target_values)
        analysis['strategies_tried'].append({
            'name': 'Соседние числа в элементах',
            'matches': strategy3_matches,
            'score': calculate_accuracy(strategy3_matches, target_values)
        })

        # Стратегия 4: Поиск в конкретных элементах с классами rating
        strategy4_matches = find_rating_elements(driver, target_values)
        analysis['strategies_tried'].append({
            'name': 'Элементы с классами рейтинга',
            'matches': strategy4_matches,
            'score': calculate_accuracy(strategy4_matches, target_values)
        })

        # Находим лучшую стратегию
        best_strategy = max(analysis['strategies_tried'], key=lambda x: x['score'])
        analysis['best_match'] = best_strategy
        analysis['accuracy_score'] = best_strategy['score']

        if best_strategy['matches']:
            # Берем первое наилучшее совпадение
            best_match = best_strategy['matches'][0]
            analysis['found_values'] = {
                'likes': int(best_match[0]),
                'rating': float(best_match[1]),
                'dislikes': int(best_match[2])
            }

        # Дополнительная диагностика
        analysis['debug_info'] = {
            'all_triple_numbers': re.findall(r'(\d+)\s+(\S+)\s+(\d+)', body_text),
            'all_rating_elements': len(driver.find_elements(By.CSS_SELECTOR, "[class*='rating']"))
        }

    except Exception as e:
        analysis['error'] = str(e)

    return analysis


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