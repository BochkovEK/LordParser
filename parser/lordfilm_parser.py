from typing import List, Dict, Optional, Union
import sys
import json
import time
import random
import re
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

        # Добавляем WebDriverWait
        self.wait = WebDriverWait(self.driver, 10)  # 10 секунд ожидания по умолчанию

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

            html = self._fetch_page_with_retry(movie_url)
            if not html:
                return {}

            soup = BeautifulSoup(html, 'html.parser')

            if not soup.find('body'):
                return {}

            # Парсим детальную информацию
            details = {
                'title': self._parse_detail_title(soup),
                'original_title': self._parse_detail_original_title(soup),
                'year': self._parse_detail_year(soup),
                'country': self._parse_detail_country(soup),
                'categories': self._parse_detail_categories(soup),
                'director': self._parse_detail_director(soup),
                'actors': self._parse_detail_actors(soup),
                'description': self._parse_detail_description(soup),
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
                        categories = [link.get_text(strip=True) for link in genre_links if link.get_text(strip=True)]
                        return categories

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
                    # import re
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

    def _parse_detail_categories(self, soup: BeautifulSoup) -> List[str]:
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
                print(f"Debug: Error parsing categories - {str(e)}")
            return []

    def _parse_detail_actors(self, soup: BeautifulSoup) -> List[str]:
        """Быстрое исправление для тестирования"""
        try:
            # Ищем элемент с актерами по точной структуре
            actors_span = soup.find('span', text='Актеры:')
            if actors_span:
                actors_li = actors_span.find_parent('li')
                if actors_li:
                    actor_links = actors_li.select('a[href*="/actors:"]')
                    actors = [link.get_text(strip=True) for link in actor_links]
                    return actors
            return []
        except Exception as e:
            if self.debug:
                print(f"Debug: Error parsing actors - {str(e)}")
            return []

    def _parse_detail_description(self, soup: BeautifulSoup) -> Optional[str]:
        """Парсинг только описания, без метаданных"""
        try:
            # Сначала пытаемся найти чистое описание
            # Ищем текст который НЕ содержит метаданные
            paragraphs = soup.find_all('p')
            for p in paragraphs:
                text = p.get_text(strip=True)
                # Описание обычно не содержит ключевые слова метаданных
                if (len(text) > 100 and
                        'Название' not in text and
                        'Год' not in text and
                        'Страна' not in text and
                        'Актеры' not in text and
                        'Режиссер' not in text):
                    return text

            # Если не нашли, берем первый длинный текст, но обрезаем метаданные
            full_text = soup.get_text()
            # Находим описание до первого мета-тега
            # import re
            match = re.search(r'(.+?)(?=Название:|Год выхода:|Страна:|Актеры:|Режиссер:|$)', full_text, re.DOTALL)
            if match:
                description = match.group(1).strip()
                if len(description) > 50:
                    return description

            return None
        except Exception as e:
            if self.debug:
                print(f"Debug: Error parsing description - {str(e)}")
            return None

    def parse_movie_details_with_rating(self, movie_url: str) -> Dict:
        """Парсинг с рейтингом через Selenium wait"""
        # Базовый парсинг
        details = self.parse_movie_details(movie_url)

        # Парсинг рейтинга с использованием self.wait
        rating_data = self._parse_rating_with_wait()
        details.update(rating_data)

        return details

    def _parse_rating_with_wait(self) -> Dict[str, str]:
        """Парсинг рейтинга с использованием ожидания"""
        try:
            # Селекторы для различных вариантов отображения рейтинга
            rating_selectors = [
                '.rating',
                '.vote',
                '.imdb-rating',
                '.kinopoisk-rating',
                '.kp-rating',
                '[class*="rating"]',
                '[class*="vote"]',
                '[itemprop="ratingValue"]',
                '.rating-value',
                '.score',
                '.rate',
                '.value',
                '.film-rating',
                '.movie-rating',
            ]

            for selector in rating_selectors:
                try:
                    if self.debug:
                        print(f"Debug: Trying rating selector: {selector}")

                    # Ждем появления элемента с рейтингом
                    rating_element = self.wait.until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )

                    # Прокручиваем элемент в viewport для активации возможного lazy loading
                    self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", rating_element)

                    # Даем время для возможной анимации/загрузки
                    import time
                    time.sleep(1)

                    rating_text = rating_element.text.strip()
                    if rating_text and any(char.isdigit() for char in rating_text):
                        if self.debug:
                            print(f"Debug: Found rating text: '{rating_text}' with selector '{selector}'")

                        # Извлекаем рейтинг и голоса из текста
                        rating_data = self._extract_rating_from_text(rating_text)

                        # Если нашли рейтинг, пытаемся найти количество голосов рядом
                        if rating_data.get('rating'):
                            if self.debug:
                                print(f"Debug: Extracted rating: {rating_data}")

                            # Ищем голоса в соседних элементах
                            votes_data = self._find_votes_near_rating(rating_element)
                            if votes_data.get('votes'):
                                rating_data['votes'] = votes_data['votes']
                                if self.debug:
                                    print(f"Debug: Found votes: {votes_data['votes']}")

                            return rating_data
                    else:
                        if self.debug:
                            print(f"Debug: Selector '{selector}' found but no rating text: '{rating_text}'")

                except Exception as e:
                    if self.debug:
                        print(f"Debug: Selector '{selector}' failed: {str(e)}")
                    continue

            # Если не нашли стандартными селекторами, пробуем поиск по тексту на странице
            if self.debug:
                print("Debug: Trying text pattern search...")
            return self._find_rating_by_text_pattern()

        except Exception as e:
            if self.debug:
                print(f"Debug: Error in _parse_rating_with_wait - {str(e)}")
            return {}

    def _extract_rating_from_text(self, text: str) -> Dict[str, str]:
        """Извлечение рейтинга и голосов из текста"""
        try:
            # import re

            rating = None
            votes = None

            # Паттерны для рейтинга (приоритет по порядку)
            rating_patterns = [
                r'(\d+\.?\d*)\s*\/\s*10',  # 8.5/10
                r'(\d+\.?\d*)\s*из\s*10',  # 8.5 из 10
                r'IMDb[:\s]*(\d+\.?\d*)',  # IMDb: 8.5 или IMDb 8.5
                r'КП[:\s]*(\d+\.?\d*)',  # КП: 7.8 или КП 7.8
                r'KP[:\s]*(\d+\.?\d*)',  # KP: 7.8
                r'КиноПоиск[:\s]*(\d+\.?\d*)',  # КиноПоиск: 7.8
                r'Рейтинг[:\s]*(\d+\.?\d*)',  # Рейтинг: 8.5
                r'Rating[:\s]*(\d+\.?\d*)',  # Rating: 8.5
                r'(\d+\.?\d*)\s*\(',  # 8.5 (1234)
                r'\b(\d+\.?\d*)\b',  # просто число 8.5
            ]

            # Паттерны для голосов
            votes_patterns = [
                r'\((\d+)\s*голос',  # (1234 голосов)
                r'\((\d+)\s*оцен',  # (1234 оценок)
                r'\((\d+)\s*vote',  # (1234 votes)
                r'(\d+)\s*голос',  # 1234 голосов
                r'(\d+)\s*оцен',  # 1234 оценок
                r'(\d+)\s*vote',  # 1234 votes
                r'голосов[:\s]*(\d+)',  # голосов: 1234
                r'оценок[:\s]*(\d+)',  # оценок: 1234
                r'votes[:\s]*(\d+)',  # votes: 1234
            ]

            # Ищем рейтинг
            for pattern in rating_patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    rating = match.group(1)
                    if self.debug:
                        print(f"Debug: Rating pattern '{pattern}' matched: {rating}")
                    break

            # Ищем голоса
            for pattern in votes_patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    votes = match.group(1)
                    if self.debug:
                        print(f"Debug: Votes pattern '{pattern}' matched: {votes}")
                    break

            result = {'rating': rating}
            if votes:
                result['votes'] = votes

            return result

        except Exception as e:
            if self.debug:
                print(f"Debug: Error extracting rating from text - {str(e)}")
            return {}

    def _find_votes_near_rating(self, rating_element) -> Dict[str, str]:
        """Поиск количества голосов рядом с рейтингом"""
        try:
            # import re

            # Сначала проверяем родительский элемент
            parent = rating_element.find_element(By.XPATH, "./..")
            parent_text = parent.text.strip()

            # Ищем голоса в тексте родителя
            votes_patterns = [
                r'\((\d+)\s*голос',
                r'\((\d+)\s*оцен',
                r'\((\d+)\s*vote',
                r'(\d+)\s*голос',
                r'(\d+)\s*оцен',
                r'(\d+)\s*vote',
            ]

            for pattern in votes_patterns:
                match = re.search(pattern, parent_text, re.IGNORECASE)
                if match:
                    votes = match.group(1)
                    if self.debug:
                        print(f"Debug: Found votes in parent: {votes}")
                    return {'votes': votes}

            # Ищем в соседних элементах
            try:
                siblings = parent.find_elements(By.XPATH, "./*")
                for sibling in siblings:
                    if sibling != rating_element:
                        sibling_text = sibling.text.strip()
                        if any(word in sibling_text.lower() for word in ['голос', 'оцен', 'vote']):
                            votes_match = re.search(r'(\d+)', sibling_text)
                            if votes_match:
                                votes = votes_match.group(1)
                                if self.debug:
                                    print(f"Debug: Found votes in sibling: {votes}")
                                return {'votes': votes}
            except:
                pass

            # Ищем в том же элементе после рейтинга
            rating_text = rating_element.text
            votes_match = re.search(r'[^\d](\d+)\s*(?:голос|оцен|vote)', rating_text, re.IGNORECASE)
            if votes_match:
                votes = votes_match.group(1)
                if self.debug:
                    print(f"Debug: Found votes in same element: {votes}")
                return {'votes': votes}

            return {}

        except Exception as e:
            if self.debug:
                print(f"Debug: Error finding votes near rating - {str(e)}")
            return {}

    def _find_rating_by_text_pattern(self) -> Dict[str, str]:
        """Поиск рейтинга по текстовым паттернам на всей странице"""
        try:
            # import re

            # Получаем весь текст страницы
            body = self.driver.find_element(By.TAG_NAME, "body")
            page_text = body.text

            if self.debug:
                print(f"Debug: Searching rating in page text (first 500 chars): {page_text[:500]}...")

            # Паттерны для поиска рейтинга в тексте
            patterns = [
                r'IMDb[:\s]*(\d+\.?\d*)',
                r'КП[:\s]*(\d+\.?\d*)',
                r'KP[:\s]*(\d+\.?\d*)',
                r'КиноПоиск[:\s]*(\d+\.?\d*)',
                r'Рейтинг[:\s]*(\d+\.?\d*)',
                r'Rating[:\s]*(\d+\.?\d*)',
                r'Оценка[:\s]*(\d+\.?\d*)',
                r'Score[:\s]*(\d+\.?\d*)',
            ]

            for pattern in patterns:
                match = re.search(pattern, page_text, re.IGNORECASE)
                if match:
                    rating = match.group(1)
                    if self.debug:
                        print(f"Debug: Found rating with text pattern '{pattern}': {rating}")

                    # Пытаемся найти голоса рядом с рейтингом в тексте
                    votes = None
                    votes_match = re.search(r'(\d+)\s*голос', page_text[match.start():match.start() + 100],
                                            re.IGNORECASE)
                    if votes_match:
                        votes = votes_match.group(1)

                    result = {'rating': rating}
                    if votes:
                        result['votes'] = votes

                    return result

            if self.debug:
                print("Debug: No rating found with text patterns")
            return {}

        except Exception as e:
            if self.debug:
                print(f"Debug: Error in _find_rating_by_text_pattern - {str(e)}")
            return {}

    def debug_page_ratings(self):
        """Комплексная диагностика рейтингов на странице"""
        print("=== ДИАГНОСТИКА РЕЙТИНГОВ ===")

        # 1. Проверяем видимый текст
        print("\n1. Поиск по текстовым паттернам:")
        patterns = ['+941', '1105', '1023', '82', 'голос', 'оцен', 'like', 'dislike', 'rating', '105', '9', '12' ]
        for pattern in patterns:
            elements = self.driver.find_elements(By.XPATH, f"//*[contains(text(), '{pattern}')]")
            print(f"   '{pattern}': {len(elements)} элементов")
            for elem in elements[:2]:  # первые 2
                print(f"     - '{elem.text.strip()}'")

        # 2. Проверяем элементы по классам/ID
        print("\n2. Поиск по селекторам:")
        selectors = [
            '[id*="ratig"]',
            '[class*="ratig"]',
            '[id*="rating"]',
            '[class*="rating"]',
            '[class*="vote"]',
            '[class*="like"]',
            '.ignore-select',
            '[onclick*="rating"]'
        ]

        for selector in selectors:
            try:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                print(f"   '{selector}': {len(elements)} элементов")
                for elem in elements[:2]:
                    text = elem.text.strip()
                    if text:
                        print(f"     - Текст: '{text}'")
                        print(f"       HTML: {elem.get_attribute('innerHTML')[:100]}...")
            except Exception as e:
                print(f"   '{selector}': ошибка - {e}")

        # 3. Проверяем все span элементы (часто рейтинги в span)
        print("\n3. Анализ span элементов с цифрами:")
        spans = self.driver.find_elements(By.TAG_NAME, "span")
        rating_spans = []
        for span in spans[:50]:  # первые 50 span
            text = span.text.strip()
            if any(char in text for char in ['+', '/10', 'IMDb', 'КП']) or re.search(r'\d+\.\d+', text):
                rating_spans.append(span)

        print(f"   Найдено подозрительных span: {len(rating_spans)}")
        for span in rating_spans[:5]:
            print(f"     - '{span.text}'")
            print(f"       Класс: '{span.get_attribute('class')}'")

    def check_specific_elements(self):
        """Проверка конкретных элементов из твоего JSON"""
        print("=== ПРОВЕРКА КОНКРЕТНЫХ ЭЛЕМЕНТОВ ===")

        # 1. Ищем элемент с ID из твоего JSON
        element_id = "ratig-layer-30747"
        try:
            element = self.driver.find_element(By.ID, element_id)
            print(f"✅ Найден элемент с ID '{element_id}':")
            print(f"   Текст: '{element.text}'")
            print(f"   HTML: {element.get_attribute('innerHTML')}")
        except:
            print(f"❌ Элемент с ID '{element_id}' не найден")

        # 2. Ищем по классам из JSON
        classes_to_find = ['ignore-select', 'ratingtypeplusminus', 'ratingplus']
        for class_name in classes_to_find:
            elements = self.driver.find_elements(By.CLASS_NAME, class_name)
            print(f"   Класс '.{class_name}': {len(elements)} элементов")
            for elem in elements[:2]:
                print(f"     - Текст: '{elem.text.strip()}'")

    def check_parent_container(self):
        """Поиск контейнера который содержит рейтинг"""
        print("\n=== ПОИСК КОНТЕЙНЕРА ===")

        # Ищем элементы которые могут содержать рейтинг
        containers = self.driver.find_elements(By.XPATH,
                                               "//div[contains(@class, 'rating') or contains(@class, 'vote')]")
        print(f"Найдено контейнеров: {len(containers)}")

        for container in containers[:3]:
            print(f"Контейнер: {container.get_attribute('class')}")
            print(f"Текст: '{container.text.strip()}'")
            print("---")

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

