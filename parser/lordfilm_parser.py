from typing import List, Dict
import sys
import json
import time
import random
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By


# Default constants
DEFAULT_URL = "https://mk.lordfilm17.ru"
DEFAULT_PAGES = 20
DEFAULT_TOP_LIST = 20
DEFAULT_YEAR = 2024
DEFAULT_DEBUG = True
BROWSERLESS_URL = "http://localhost:4444/wd/hub"


def apply_stealth_settings(chrome_options):
    """Настройки для обхода детекции автоматизации"""
    # Базовые настройки
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")

    # Дополнительные параметры скрытности
    chrome_options.add_argument("--disable-infobars")
    chrome_options.add_argument("--disable-web-security")
    chrome_options.add_argument("--disable-site-isolation-trials")
    chrome_options.add_argument("--disable-notifications")
    chrome_options.add_argument("--disable-popup-blocking")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")

    # User-Agent
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ]
    chrome_options.add_argument(f"user-agent={random.choice(user_agents)}")

    # Экспериментальные опции
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option("useAutomationExtension", False)

    return chrome_options


def human_like_delay():
    """Имитация человеческой задержки между действиями"""
    time.sleep(random.uniform(0.5, 1.5))


class LordFilmParser:
    def __init__(self, base_url: str = DEFAULT_URL, year: int = DEFAULT_YEAR, debug: bool = DEFAULT_DEBUG):
        """Initialize the parser with Selenium WebDriver"""
        self.base_url = base_url
        self.year = year
        self.debug = debug

        # Настройка Browserless с параметрами скрытности
        chrome_options = Options()
        chrome_options = apply_stealth_settings(chrome_options)

        if not debug:
            chrome_options.add_argument("--headless=new")  # Фоновый режим

        # Подключение к Browserless
        self.driver = webdriver.Remote(
            command_executor=BROWSERLESS_URL,
            options=chrome_options
        )
        self.driver.set_page_load_timeout(60)  # 60 секунд на загрузку страницы

        # 2. Таймаут ожидания элементов (секунды)
        # wait = WebDriverWait(driver, 30)  # Ожидание до 30 секунд

        # Изменение свойств браузера для обхода детекции
        self.driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5]
                });
                window.chrome = {
                    runtime: {},
                };
            """
        })

        self._cached_movies = None

    # def cleanup(self):
    #     """Безопасное освобождение ресурсов"""
    #     if hasattr(self, 'driver') and self.driver is not None:
    #         try:
    #             self.driver.quit()
    #         except Exception as e:
    #             if self.debug:
    #                 print(f"Debug: Error during driver quit - {str(e)}")
    #         finally:
    #             self.driver = None

    # def cleanup(self):
    #     """Безопасное освобождение ресурсов"""
    #     if getattr(self, 'driver', None) is not None:
    #         try:
    #             if not is_python_shutting_down():
    #                 self.driver.quit()
    #         except Exception as e:
    #             if self.debug:
    #                 print(f"Debug: Error during cleanup - {str(e)}")
    #         finally:
    #             self.driver = None

    def cleanup(self):
        """Потокобезопасная версия"""
        import threading
        if not hasattr(self, '_cleanup_lock'):
            self._cleanup_lock = threading.Lock()

        with self._cleanup_lock:
            if getattr(self, 'driver', None) is not None:
                try:
                    if not is_python_shutting_down():
                        self.driver.quit()
                except Exception as e:
                    if self.debug:
                        print(f"Debug: Error during cleanup - {str(e)}")
                        # logging.exception("Cleanup error")
                finally:
                    self.driver = None

    def __enter__(self):
        """Для использования с контекстным менеджером"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Гарантированное закрытие при выходе из контекста"""
        self.cleanup()

    # def __del__(self):
    #     """Деструктор с защитой от ошибок при завершении"""
    #     try:
    #         self.cleanup()
    #     except Exception:
    #         pass  # Игнорируем любые ошибки при завершении

    def _fetch_page(self, url: str) -> str:
        """Получение страницы через Selenium"""
        try:
            self.driver.get(url)

            # Имитация человеческого поведения
            if not self.debug:
                self._simulate_human_interaction()

            # Ожидание загрузки контента
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CLASS_NAME, "th-item"))
            )
            human_like_delay()
            return self.driver.page_source

        except Exception as e:
            if self.debug:
                print(f"Debug: Error loading {url} - {str(e)}")
            return None

    def _simulate_human_interaction(self):
        """Имитация человеческого взаимодействия"""
        try:
            # Случайный скроллинг
            scroll_positions = [
                "window.scrollTo(0, document.body.scrollHeight/4)",
                "window.scrollTo(0, document.body.scrollHeight/2)",
                "window.scrollTo(0, document.body.scrollHeight*0.75)"
            ]
            for script in random.sample(scroll_positions, 2):
                self.driver.execute_script(script)
                time.sleep(random.uniform(0.5, 1.8))

        except Exception:
            pass

    def _fetch_movies(self, pages: int) -> List[Dict]:
        """Получение фильмов с использованием Selenium"""
        all_movies = []
        use_year_in_url = True

        for page in range(1, pages + 1):
            # Формируем URL с учетом года
            url = f"{self.base_url}/filmy/{f'{self.year}/' if self.year and use_year_in_url else ''}page/{page}/"

            if self.debug:
                print(f"Debug: Loading page {url}")

            html = self._fetch_page(url)

            # Если получили 404, пробуем без указания года
            if html is None and use_year_in_url and self.year:
                use_year_in_url = False
                url = f"{self.base_url}/filmy/page/{page}/"
                html = self._fetch_page(url)

            if not html:
                continue

            soup = BeautifulSoup(html, 'html.parser')

            for item in soup.select('.th-item'):
                try:
                    link_elem = item.select_one('a.th-in')
                    if not link_elem:
                        continue

                    # Парсим основные данные
                    link = link_elem.get('href', '')
                    title = link_elem.select_one('.th-title').get_text(strip=True) if link_elem.select_one(
                        '.th-title') else "No title"

                    # Год выпуска
                    year_elem = link_elem.select_one('.th-series')
                    year = int(year_elem.text.strip()) if year_elem and year_elem.text.strip().isdigit() else None

                    # Рейтинги
                    rating_kp = self._parse_rating(item, '.th-rate-kp span')
                    rating_imdb = self._parse_rating(item, '.th-rate-imdb span')

                    all_movies.append({
                        'title': title,
                        'link': link,
                        'year': year,
                        'rating_kp': rating_kp,
                        'rating_imdb': rating_imdb,
                        'rating_avg': (
                            round((rating_kp + rating_imdb) / 2, 1)
                            if rating_kp is not None and rating_imdb is not None  # Оба не None → среднее
                            else rating_kp if rating_imdb is None else rating_imdb  # Один None → берём не-None
                            if rating_kp is not None or rating_imdb is not None  # Хотя бы один не None
                            else None  # Оба None → None
                        )
                    })

                except Exception as e:
                    if self.debug:
                        print(f"Debug: Error parsing item - {str(e)}")
                    continue

            # Случайная задержка между запросами
            time.sleep(random.uniform(2.5, 6.0) if self.debug else random.uniform(1.0, 3.5))

        return all_movies

    def _parse_rating(self, parent_element, selector: str) -> float:
        """Парсинг рейтинга из элемента"""
        elem = parent_element.select_one(selector)
        if elem:
            try:
                return float(elem.get_text(strip=True))
            except ValueError:
                return None
        return None

    def get_sorted_movies(self, pages: int = DEFAULT_PAGES, sort_by: str = 'kp') -> List[Dict]:
        """Получение и сортировка фильмов"""
        if self._cached_movies is None:
            self._cached_movies = self._fetch_movies(pages)

        if self.year:
            self._cached_movies = [m for m in self._cached_movies if m.get('year') == self.year]

        key = {
            'avg': 'rating_avg',
            'imdb': 'rating_imdb',
        }.get(sort_by.lower(), 'rating_kp')

        return sorted(
            self._cached_movies,
            key=lambda x: (x[key] is not None, x[key]),
            reverse=True
        )

    def save_to_json(self, data: List[Dict], filename: str = 'movies.json'):
        """Сохранение в JSON"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        if self.debug:
            print(f"Data saved to {filename}")

    # def __del__(self):
    #     """Закрытие драйвера при уничтожении объекта"""
    #     if hasattr(self, 'driver'):
    #         self.driver.quit()


def is_python_shutting_down():
    """Проверяет, находится ли Python в процессе завершения работы"""

    return sys.meta_path is None

if __name__ == "__main__":
    try:
        parser = LordFilmParser(
            year=YEAR if 'YEAR' in globals() else DEFAULT_YEAR,
            debug=DEBUG if 'DEBUG' in globals() else DEFAULT_DEBUG
        )
    except NameError:
        parser = LordFilmParser()

    pages = PAGES if 'PAGES' in globals() else DEFAULT_PAGES
    top_list = TOP_LIST if 'TOP_LIST' in globals() else DEFAULT_TOP_LIST

    for sort_criteria, filename in [('kp', 'top_kp.json'), ('imdb', 'top_imdb.json'), ('avg', 'top_avg.json')]:
        top_movies = parser.get_sorted_movies(pages=pages, sort_by=sort_criteria)[:top_list]
        parser.save_to_json(top_movies, filename)

        print(f"\nTop {top_list} by {sort_criteria.upper()}:")
        for i, movie in enumerate(top_movies, 1):
            rating = movie[f'rating_{sort_criteria}'] if sort_criteria != 'avg' else movie['rating_avg']
            rating_display = f"{rating:.1f}" if isinstance(rating, (int, float)) else str(rating)
            print(f"{i}. {movie['title']} ({movie['year']}) - {rating_display} {movie['link']}")

