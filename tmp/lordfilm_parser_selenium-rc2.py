# none root user
# sudo yum in python
# pip install virtualenv -i https://pypi.org/simple/
# sudo yum in -y  epel-release-latest-9.noarch.rpm
# yum in chromium chromedriver
# source venv/bin/activate  # Для bash/zsh
# pip install -r ~/test/requirements.txt -i https://pypi.org/simple/

# Не выполнять: sudo systemctl start docker
# Не выполнять: sudo systemctl enable docker now
# Не выполнять: docker run -d -p 4444:4444 --shm-size=2g selenium/standalone-chrome

# Не выполнять: pkill -f chromecurl -O https://chromedriver.storage.googleapis.com/120.0.6099.130/chromedriver_linux64.zip
# Не выполнять: unzip chromedriver_linux64.zip
# Не выполнять: mv chromedriver venv/bin/
# Не выполнять: chmod +x venv/bin/chromedriver

# sudo podman run -d   -p 3000:3000   -e "MAX_CONCURRENT_SESSIONS=10"   -e "MAX_QUEUE_LENGTH=100"   --shm-size=2g   browserless/chrome:latest

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from typing import List, Dict
import json
import time
from bs4 import BeautifulSoup

# Default constants
DEFAULT_URL = "https://mk.lordfilm17.ru"
DEFAULT_PAGES = 10
DEFAULT_TOP_LIST = 20
DEFAULT_YEAR = 2024
DEFAULT_DEBUG = False
BROWSERLESS_URL = "http://localhost:3000/webdriver"  # Browserless endpoint


class LordFilmParser:
    def __init__(self, base_url: str = DEFAULT_URL, year: int = DEFAULT_YEAR, debug: bool = DEFAULT_DEBUG):
        """Initialize the parser with Selenium WebDriver"""
        self.base_url = base_url
        self.year = year
        self.debug = debug

        # Настройка Browserless
        chrome_options = Options()
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

        if not debug:
            chrome_options.add_argument("--headless=new")  # Фоновый режим

        # Подключение к Browserless
        self.driver = webdriver.Remote(
            command_executor=BROWSERLESS_URL,
            options=chrome_options
        )
        self._cached_movies = None

    def _fetch_page(self, url: str) -> str:
        """Получение страницы через Selenium"""
        try:
            self.driver.get(url)
            # Ожидание загрузки контента
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "th-item"))
            )
            return self.driver.page_source

        except Exception as e:
            if self.debug:
                print(f"Debug: Error loading {url} - {str(e)}")
            return None

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
                        'rating_avg': round((rating_kp + rating_imdb) / 2, 1) if None not in (
                            rating_kp, rating_imdb) else None
                    })

                except Exception as e:
                    if self.debug:
                        print(f"Debug: Error parsing item - {str(e)}")
                    continue

            # Задержка между запросами
            time.sleep(2 if self.debug else 0.5)

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

    def __del__(self):
        """Закрытие драйвера при уничтожении объекта"""
        if hasattr(self, 'driver'):
            self.driver.quit()


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