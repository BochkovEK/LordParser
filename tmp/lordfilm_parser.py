import requests
from bs4 import BeautifulSoup
import re
from typing import List, Dict
import json

# Const
URL = "https://mk.lordfilm17.ru"
PAGES = 50
TOP_LIST = 50
YEAR = None
YEAR = 2025
DEBUG = False


class LordFilmParser:
    def __init__(self, base_url: str = URL, year: int = None):
        self.base_url = base_url
        self.year = year
        self.headers = {'User-Agent': 'Mozilla/5.0'}
        self._cached_movies = None  # Кэш для загруженных фильмов

# class LordFilmParser:
#     def __init__(self, base_url: str = "https://mk.lordfilm17.ru", year: int = None):
#         self.base_url = base_url
#         self.year = year
#         self.headers = {'User-Agent': 'Mozilla/5.0'}
#         self._cached_movies = None  # Кэш для загруженных фильмов

    def _fetch_movies(self, pages: int) -> List[Dict]:
        """Внутренний метод для загрузки фильмов"""
        all_movies = []

        for page in range(1, pages + 1):
            url = f"{self.base_url}/filmy/{f'{self.year}/' if self.year else ''}page/{page}/"
            try:
                response = requests.get(url, headers=self.headers)
                if DEBUG:
                    print(f"response:\n{response.text}")
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')

                for item in soup.select('.th-item'):
                    # Основной элемент с ссылкой
                    link_elem = item.select_one('a.th-in')
                    if not link_elem:
                        continue

                    # Парсим основные данные
                    link = link_elem.get('href')
                    title_elem = link_elem.select_one('.th-title')
                    title = title_elem.get_text(strip=True) if title_elem else "Без названия"

                    # Парсим год выпуска (ищем элемент с классом th-series)
                    year_elem = link_elem.select_one('.th-series')
                    year = int(year_elem.get_text(strip=True)) if year_elem and year_elem.get_text(
                        strip=True).isdigit() else None

                    # Парсим рейтинги
                    rating_kp = 0.0
                    rating_imdb = 0.0

                    kp_elem = link_elem.select_one('.th-rate-kp span')
                    if kp_elem:
                        try:
                            rating_kp = float(kp_elem.get_text(strip=True))
                        except ValueError:
                            pass

                    imdb_elem = link_elem.select_one('.th-rate-imdb span')
                    if imdb_elem:
                        try:
                            rating_imdb = float(imdb_elem.get_text(strip=True))
                        except ValueError:
                            pass

                    all_movies.append({
                        'title': title,
                        'link': link,
                        'year': year,
                        'rating_kp': rating_kp,
                        'rating_imdb': rating_imdb,
                        'rating_avg': round((rating_kp + rating_imdb) / 2, 1) if rating_kp and rating_imdb else None
                    })

                # for item in soup.select('.th-item'):
                #     # Находим элемент с ссылкой
                #     link_elem = item.select_one('a.th-in')
                #     if not link_elem:
                #         continue
                #
                #     # Извлекаем ссылку
                #     link = link_elem['href'] if link_elem.has_attr('href') else None
                #
                #     # Находим название
                #     title_elem = link_elem.select_one('.th-title')
                #     title = title_elem.get_text(strip=True) if title_elem else "Без названия"
                #
                #     if DEBUG:
                #         print(f"Title: {title}, Link: {link}")
                #
                #     # Извлекаем рейтинги
                #     rating_kp = 0.0
                #     rating_imdb = 0.0
                #
                #     # Парсим рейтинг КиноПоиска
                #     kp_elem = item.select_one('.th-rate-kp span')
                #     if kp_elem:
                #         try:
                #             rating_kp = float(kp_elem.get_text(strip=True))
                #         except ValueError:
                #             pass
                #
                #     # Парсим рейтинг IMDB
                #     imdb_elem = item.select_one('.th-rate-imdb span')
                #     if imdb_elem:
                #         try:
                #             rating_imdb = float(imdb_elem.get_text(strip=True))
                #         except ValueError:
                #             pass
                #
                #     all_movies.append({
                #         'title': title,
                #         'link': link,
                #         'rating_kp': rating_kp,
                #         'rating_imdb': rating_imdb
                #     })

            except requests.RequestException as e:
                print(f"Ошибка при загрузке страницы {page}: {e}")
                continue

        return all_movies

    def get_sorted_movies(self, pages: int = 3, sort_by: str = 'kp') -> List[Dict]:
        """Получаем и сортируем фильмы (с кэшированием результатов)"""
        if self._cached_movies is None:
            self._cached_movies = self._fetch_movies(pages)

        # Вычисляем средний рейтинг если нужно
        if sort_by == 'avg':
            for movie in self._cached_movies:
                movie['rating_avg'] = round((movie['rating_kp'] + movie['rating_imdb']) / 2, 1)
            key = 'rating_avg'
        else:
            key = f'rating_{sort_by}'

        return sorted(self._cached_movies, key=lambda x: x[key], reverse=True)
    #
    # def get_movies(self, page: int = 1) -> List[Dict]:
    #     """Получаем список фильмов с указанной страницы с учетом года"""
    #     if self.year:
    #         url = f"{self.base_url}/filmy/{self.year}/page/{page}/"
    #     else:
    #         url = f"{self.base_url}/filmy/page/{page}/"
    #
    #     try:
    #         response = requests.get(url, headers=self.headers)
    #         response.raise_for_status()
    #     except requests.RequestException as e:
    #         print(f"Ошибка при запросе {url}: {e}")
    #         return []
    #
    #     soup = BeautifulSoup(response.text, 'html.parser')
    #     movies = []
    #
    #     for item in soup.select('.th-item'):
    #         title_elem = item.select_one('.th-title')
    #         print(f"title_elem: {title_elem}")
    #         if not title_elem:
    #             continue
    #
    #         title = title_elem.get_text(strip=True)
    #         link = title_elem['href'] if title_elem.has_attr('href') else None
    #
    #         # Извлекаем рейтинги
    #         rating_kp = 0.0
    #         rating_imdb = 0.0
    #
    #         # Парсим рейтинг КиноПоиска
    #         kp_elem = item.select_one('.th-rate-kp span')
    #         if kp_elem:
    #             try:
    #                 rating_kp = float(kp_elem.get_text(strip=True))
    #             except ValueError:
    #                 pass
    #
    #         # Парсим рейтинг IMDB
    #         imdb_elem = item.select_one('.th-rate-imdb span')
    #         if imdb_elem:
    #             try:
    #                 rating_imdb = float(imdb_elem.get_text(strip=True))
    #             except ValueError:
    #                 pass
    #
    #         movies.append({
    #             'title': title,
    #             'link': link,
    #             'rating_kp': rating_kp,
    #             'rating_imdb': rating_imdb
    #         })
    #
    #     return movies
    #
    # # def get_sorted_movies(self, pages: int = 3, sort_by: str = 'kp') -> List[Dict]:
    # #     """Получаем и сортируем фильмы с нескольких страниц"""
    # #     all_movies = []
    # #
    # #     for page in range(1, pages + 1):
    # #         print(f"Парсинг страницы {page}...")
    # #         all_movies.extend(self.get_movies(page))
    # #
    # #     # Сортировка
    # #     reverse = True  # по убыванию
    # #     if sort_by.lower() == 'kp':
    # #         key = 'rating_kp'
    # #     elif sort_by.lower() == 'imdb':
    # #         key = 'rating_imdb'
    # #     else:
    # #         key = 'rating_kp'
    # #
    # #     return sorted(all_movies, key=lambda x: x[key], reverse=reverse)
    # def get_sorted_movies(self, pages: int = 3, sort_by: str = 'kp') -> List[Dict]:
    #     """Получаем и сортируем фильмы с учетом гоа
    #
    #     Аргументы:
    #         pages: количество страниц для парсинга
    #         sort_by: критерий сортировки:
    #             'kp' - по рейтингу КиноПоиска
    #             'imdb' - по рейтингу IMDB
    #             'avg' - по среднему арифметическому двух рейтингов
    #     """
    #     all_movies = []
    #
    #     for page in range(1, pages + 1):
    #         print(f"Парсинг страницы {page}...")
    #         all_movies.extend(self.get_movies(page))
    #
    #     # Сортировка
    #     reverse = True  # по убыванию
    #
    #     if sort_by.lower() == 'kp':
    #         key = 'rating_kp'
    #     elif sort_by.lower() == 'imdb':
    #         key = 'rating_imdb'
    #     elif sort_by.lower() == 'avg':
    #         # Добавляем вычисленный средний рейтинг в каждый словарь
    #         for movie in all_movies:
    #             avg = (movie['rating_kp'] + movie['rating_imdb']) / 2
    #             movie['rating_avg'] = round(avg, 1)  # Округление до десятых
    #         key = 'rating_avg'
    #     else:
    #         key = 'rating_kp'
    #
    #     return sorted(all_movies, key=lambda x: x[key], reverse=reverse)

    def save_to_json(self, data: List[Dict], filename: str = 'movies.json'):
        """Сохраняем данные в JSON файл"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"Данные сохранены в {filename}")


# Использование
if __name__ == "__main__":
    try:
        if YEAR is not None:  # Если переменная YEAR существует и не None
            parser = LordFilmParser(year=YEAR)
        else:
            parser = LordFilmParser()
    except NameError:  # Если переменная YEAR не определена
        parser = LordFilmParser()

    # Получаем топ 50 фильмов по рейтингу КиноПоиска
    top_kp = parser.get_sorted_movies(pages=PAGES, sort_by='kp')
    parser.save_to_json(top_kp, 'top_kp.json')

    # Получаем топ 50 фильмов по рейтингу IMDB
    top_imdb = parser.get_sorted_movies(pages=PAGES, sort_by='imdb')
    parser.save_to_json(top_imdb, 'top_imdb.json')

    # Получаем топ 50 фильмов по рейтингу IMDB
    top_avg = parser.get_sorted_movies(pages=PAGES, sort_by='avg')
    parser.save_to_json(top_avg, 'top_avg.json')

    # Выводим топ
    print("\nТоп-10 по КиноПоиску:")
    for i, movie in enumerate(top_kp[:TOP_LIST], 1):
        print(f"{i}. {movie['title']} ({movie['year']}) - {movie['rating_kp']} {movie['link']}")

    print("\nТоп-10 по IMDB:")
    for i, movie in enumerate(top_imdb[:TOP_LIST], 1):
        print(f"{i}. {movie['title']} ({movie['year']}) - {movie['rating_imdb']} {movie['link']}")

    print("\nТоп-10 по AVG:")
    for i, movie in enumerate(top_avg[:TOP_LIST], 1):
        print(f"{i}. {movie['title']} ({movie['year']}) - {movie['rating_avg']} {movie['link']}")

