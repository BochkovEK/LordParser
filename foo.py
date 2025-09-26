#!/usr/bin/env python3
import sys
import os
import time
# from selenium.webdriver.common.by import By

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from parser.lordfilm_parser import LordFilmParser


def full_debug():
    if len(sys.argv) != 2:
        print("Использование: python debug_ratings.py <URL>")
        return

    url = sys.argv[1]
    parser = LordFilmParser(debug=True)

    try:
        print(f"🔍 Диагностика страницы: {url}")
        parser.driver.get(url)
        time.sleep(3)

        # Запускаем все проверки
        parser.debug_page_ratings()
        parser.check_specific_elements()
        parser.check_parent_container()

    except Exception as e:
        print(f"❌ Ошибка: {e}")
    finally:
        parser.cleanup()


if __name__ == "__main__":
    full_debug()