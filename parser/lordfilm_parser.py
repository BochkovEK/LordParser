from typing import List, Dict, Optional
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
NONE_RATING_KP = 6


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
    def __init__(self, base_url: str = DEFAULT_URL, year: int = DEFAULT_YEAR, debug: bool = DEFAULT_DEBUG,
                 none_rating_kp: int = NONE_RATING_KP):
        """Initialize the parser with Selenium WebDriver"""
        self.base_url = base_url
        self.year = year
        self.none_rating_kp = none_rating_kp
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
                            else round((self.none_rating_kp + rating_imdb) / 2, 1)  # rating_kp None → считаем как 6 + imdb
                            if rating_kp is None and rating_imdb is not None
                            else rating_kp  # rating_imdb None → используем rating_kp
                            if rating_kp is not None and rating_imdb is None
                            else None
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

    def _fetch_page_with_retry(self, url: str, max_retries: int = 3) -> str:
        """Загрузка страницы с повторными попытками"""
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    print(f"Retry {attempt}/{max_retries} for {url}")
                    time.sleep(5 * attempt)  # Увеличиваем паузу между попытками

                self.driver.set_page_load_timeout(60 + (20 * attempt))  # Увеличиваем таймаут
                self.driver.get(url)

                WebDriverWait(self.driver, 20).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )

                return self.driver.page_source

            except Exception as e:
                if attempt == max_retries - 1:  # Последняя попытка
                    if self.debug:
                        print(f"Debug: Final attempt failed for {url} - {e}")
                    return None
                continue

        return None

    def parse_movie_details(self, movie_url: str) -> Dict:
        """Парсинг детальной страницы фильма с улучшенной обработкой ошибок"""
        try:
            if self.debug:
                print(f"Debug: Parsing movie details from {movie_url}")

            # Пробуем разные методы загрузки
            html = self._fetch_page_with_retry(movie_url)
            if not html:
                if self.debug:
                    print(f"Debug: Failed to load page {movie_url}")
                return {}

            soup = BeautifulSoup(html, 'html.parser')

            # Быстрая проверка что страница загружена корректно
            if not soup.find('body'):
                if self.debug:
                    print(f"Debug: Empty or invalid page for {movie_url}")
                return {}

            # Парсим детальную информацию
            details = {
                'country': self._parse_detail_country(soup),
                'genres': self._parse_detail_genres(soup),
                'director': self._parse_detail_director(soup),
                'actors': self._parse_detail_actors(soup),
                'description': self._parse_detail_description(soup),
                'duration': self._parse_detail_duration(soup),
            }

            # Фильтруем пустые значения
            return {k: v for k, v in details.items() if v is not None and v != [] and v != ''}

        except Exception as e:
            if self.debug:
                print(f"Debug: Error parsing movie details from {movie_url} - {str(e)}")
            return {}

    def _parse_detail_from_flist(self, soup: BeautifulSoup, field_name: str, is_list: bool = False) -> Union[
        str, List[str], None]:
        """Универсальный метод для парсинга данных из списка flist"""
        try:
            list_items = soup.select('ul.flist li')

            for li in list_items:
                span = li.find('span')
                if span and f"{field_name}:" in span.get_text():
                    # Для актеров - извлекаем ссылки
                    if field_name == 'Актеры':
                        actor_links = li.select('a[href*="/actors:"]')
                        actors = [link.get_text(strip=True) for link in actor_links if link.get_text(strip=True)]
                        return actors

                    # Для жанров/категорий - извлекаем ссылки
                    elif field_name == 'Категории' or field_name == 'Жанр':
                        genre_links = li.select('a[href*="/filmy/"]')
                        genres = [link.get_text(strip=True) for link in genre_links if link.get_text(strip=True)]
                        return genres

                    # Для обычных текстовых полей
                    else:
                        # Удаляем span с названием поля и берем оставшийся текст
                        span.extract()  # Удаляем span из элемента
                        text_content = li.get_text(strip=True)
                        if text_content:
                            if is_list:
                                return [item.strip() for item in text_content.split(',')]
                            return text_content

            return None if not is_list else []
        except Exception as e:
            if self.debug:
                print(f"Debug: Error parsing {field_name} - {str(e)}")
            return None if not is_list else []

    def _parse_detail_title(self, soup: BeautifulSoup) -> Optional[str]:
        """Парсинг названия фильма"""
        try:
            # Сначала ищем в заголовке страницы
            title_tag = soup.find('title')
            if title_tag:
                title_text = title_tag.get_text()
                # Убираем лишние части (например, "смотреть онлайн")
                if 'смотреть онлайн' in title_text:
                    title = title_text.split('смотреть онлайн')[0].strip()
                    return title

            # Ищем в h1 или других заголовках
            h1 = soup.find('h1')
            if h1:
                return h1.get_text(strip=True)

            # Ищем в списке
            return self._parse_detail_from_flist(soup, 'Название')
        except Exception as e:
            if self.debug:
                print(f"Debug: Error parsing title - {str(e)}")
            return None

    def _parse_detail_original_title(self, soup: BeautifulSoup) -> Optional[str]:
        """Парсинг оригинального названия"""
        try:
            # Пробуем разные варианты названий полей
            fields_to_try = ['Оригинальное название', 'Original title', 'Название оригинала']

            for field in fields_to_try:
                result = self._parse_detail_from_flist(soup, field)
                if result:
                    return result

            return None
        except Exception as e:
            if self.debug:
                print(f"Debug: Error parsing original title - {str(e)}")
            return None

    def _parse_detail_year(self, soup: BeautifulSoup) -> Optional[str]:
        """Парсинг года выпуска"""
        try:
            # Пробуем разные варианты названий полей
            fields_to_try = ['Год выхода', 'Год', 'Year', 'Дата выхода']

            for field in fields_to_try:
                result = self._parse_detail_from_flist(soup, field)
                if result:
                    # Извлекаем только цифры года
                    import re
                    year_match = re.search(r'\b(19|20)\d{2}\b', result)
                    if year_match:
                        return year_match.group()
                    return result

            # Альтернативный поиск в URL или категориях
            year_link = soup.find('a', href=re.compile(r'/filmy/\d{4}/'))
            if year_link:
                year_match = re.search(r'/(\d{4})/', year_link.get('href', ''))
                if year_match:
                    return year_match.group(1)

            return None
        except Exception as e:
            if self.debug:
                print(f"Debug: Error parsing year - {str(e)}")
            return None

    def _parse_detail_country(self, soup: BeautifulSoup) -> Optional[str]:
        """Парсинг страны"""
        try:
            fields_to_try = ['Страна', 'Country', 'Производство']

            for field in fields_to_try:
                result = self._parse_detail_from_flist(soup, field)
                if result:
                    return result

            return None
        except Exception as e:
            if self.debug:
                print(f"Debug: Error parsing country - {str(e)}")
            return None

    def _parse_detail_director(self, soup: BeautifulSoup) -> Optional[str]:
        """Парсинг режиссера"""
        try:
            fields_to_try = ['Режиссер', 'Director', 'Режиссеры']

            for field in fields_to_try:
                result = self._parse_detail_from_flist(soup, field)
                if result:
                    return result

            return None
        except Exception as e:
            if self.debug:
                print(f"Debug: Error parsing director - {str(e)}")
            return None

    def _parse_detail_genres(self, soup: BeautifulSoup) -> List[str]:
        """Парсинг жанров"""
        try:
            # Пробуем получить жанры из категорий
            result = self._parse_detail_from_flist(soup, 'Категории')
            if result:
                return result

            # Пробуем поле "Жанр"
            result = self._parse_detail_from_flist(soup, 'Жанр')
            if result:
                if isinstance(result, list):
                    return result
                return [result]

            return []
        except Exception as e:
            if self.debug:
                print(f"Debug: Error parsing genres - {str(e)}")
            return []

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

