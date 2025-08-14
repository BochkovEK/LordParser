import os
import requests
from bs4 import BeautifulSoup
from typing import List, Dict
import json

# Default constants
DEFAULT_URL = "https://mk.lordfilm17.ru"
DEFAULT_PAGES = 100
DEFAULT_TOP_LIST = 20
DEFAULT_YEAR = 2025
DEFAULT_DEBUG = True
MAX_VOTERS = 100  # Новая константа для расчета веса


class LordFilmParser:
    def __init__(self, base_url: str = DEFAULT_URL, year: int = DEFAULT_YEAR, debug: bool = DEFAULT_DEBUG):
        """Initialize the parser with base URL, year filter and debug mode"""
        self.base_url = base_url
        self.year = year
        self.debug = debug
        self.headers = {'User-Agent': 'Mozilla/5.0'}
        self._cached_movies = None
        self.images_dir = "movie_images"
        os.makedirs(self.images_dir, exist_ok=True)

    def _download_image(self, url: str, movie_id: str) -> str:
        """Download image and return local path (always overwrites existing)"""
        if not url:
            if self.debug:
                print(f"Debug: Empty image URL for movie {movie_id}")
            return ""

        try:
            # Получаем расширение файла из URL
            ext = url.split('.')[-1].split('?')[0].lower()
            if ext not in ['jpg', 'jpeg', 'png', 'gif', 'webp']:
                ext = 'jpg'  # используем jpg как расширение по умолчанию

            filename = f"{movie_id}.{ext}"
            path = os.path.join(self.images_dir, filename)

            # Удаляем существующий файл, если он есть
            if os.path.exists(path):
                os.remove(path)
                if self.debug:
                    print(f"Debug: Removed existing image for {movie_id}")

            # Обрабатываем относительные URL
            if not url.startswith('http'):
                if not url.startswith('/'):
                    url = '/' + url
                url = f"{self.base_url}{url}"

            if self.debug:
                print(f"Debug: Downloading image from {url}")

            # Скачиваем и сохраняем изображение
            response = requests.get(url, headers=self.headers, stream=True, timeout=10)
            response.raise_for_status()

            with open(path, 'wb') as f:
                for chunk in response.iter_content(1024):
                    f.write(chunk)

            if self.debug:
                print(f"Debug: Successfully saved/overwritten image for {movie_id} at {path}")
            return filename

        except Exception as e:
            if self.debug:
                print(f"Debug: Error downloading image {url} for {movie_id}: {str(e)}")
            return ""

    def _fetch_movies(self, pages: int) -> List[Dict]:
        """Internal method to fetch movies from specified pages"""
        all_movies = []
        use_year_in_url = True

        for page in range(1, pages + 1):
            url = f"{self.base_url}/filmy/{f'{self.year}/' if self.year and use_year_in_url else ''}page/{page}/"
            try:
                response = requests.get(url, headers=self.headers)

                if response.status_code == 404 and use_year_in_url and self.year:
                    if self.debug:
                        print(f"Debug: Got 404 for year-specific page, trying general URL")
                    use_year_in_url = False
                    url = f"{self.base_url}/filmy/page/{page}/"
                    response = requests.get(url, headers=self.headers)

                if self.debug:
                    print(f"Debug: Page {page} response\n{response.text[:500]}...")
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')

                for idx, item in enumerate(soup.select('.th-item')):
                    link_elem = item.select_one('a.th-in')
                    if not link_elem:
                        continue

                    # Parse basic movie data
                    link = link_elem.get('href', '')
                    title = link_elem.select_one('.th-title').get_text(strip=True) if link_elem.select_one(
                        '.th-title') else "No title"

                    # Parse year
                    year_elem = link_elem.select_one('.th-series')
                    try:
                        year = int(year_elem.get_text(strip=True)) if year_elem else None
                    except ValueError:
                        year = None

                    # Parse ratings with None as default
                    rating_kp = self._parse_rating(link_elem, '.th-rate-kp span')
                    rating_imdb = self._parse_rating(link_elem, '.th-rate-imdb span')

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
                        ),
                        'final_rating': None  # Будет заполнено после получения данных со страницы фильма
                    })

            except requests.RequestException as e:
                print(f"Error loading page {page}: {e}")
                continue

        return all_movies

    def _fetch_movie_details(self, movie_url: str) -> Dict:
        """Fetch additional details from movie page including ratings"""
        try:
            response = requests.get(movie_url, headers=self.headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # Parse description
            description_elem = soup.select_one('.fdesc')
            description = description_elem.get_text(strip=True) if description_elem else "No description available"

            # Parse country
            country = None
            country_elem = soup.select_one('.fmeta-item:-soup-contains("Страна")') or \
                           soup.select_one('.fmeta-item:-soup-contains("страна")') or \
                           soup.select_one('li:-soup-contains("Страна")') or \
                           soup.select_one('li:-soup-contains("страна")')

            if country_elem:
                country = country_elem.get_text(strip=True).split(':')[-1].strip()

            # Parse image - сначала пробуем разные селекторы
            img_elem = (soup.select_one('.fposter img') or
                        soup.select_one('.poster img') or
                        soup.select_one('img[itemprop="image"]') or
                        soup.select_one('.th-item img'))

            image_url = img_elem.get('src') if img_elem else None

            if image_url and not image_url.startswith('http'):
                if not image_url.startswith('/'):
                    image_url = '/' + image_url
                image_url = f"{self.base_url}{image_url}"

            if self.debug and img_elem:
                print(f"Debug: Found image at {image_url}")

            # Parse lordfilm rating
            rating_elem = soup.select_one(
                '#dle-content > article > div.fmain > div.fcols.fx-row > div.fleft.fx-1.fx-row > div.fleft-img.fx-first > div > div.flikes.fx-row > div.slide-circle > div > div')
            try:
                rating_lordfilm = float(rating_elem.get_text(strip=True)) if rating_elem else None
            except (ValueError, AttributeError):
                rating_lordfilm = None

            # Parse voters count
            positive_votes_elem = soup.select_one('#ps-52866 > span.psc')
            negative_votes_elem = soup.select_one('#ms-52866 > span.msc')

            try:
                positive_votes = int(positive_votes_elem.get_text(strip=True)) if positive_votes_elem else 0
                negative_votes = int(negative_votes_elem.get_text(strip=True)) if negative_votes_elem else 0
                voters = positive_votes + negative_votes
            except (ValueError, AttributeError):
                voters = 0

            return {
                'description': description,
                'country': country,
                'image_url': image_url,
                'rating_lordfilm': rating_lordfilm,
                'voters': voters
            }
        except requests.RequestException as e:
            if self.debug:
                print(f"Error loading movie details {movie_url}: {e}")
            return {
                'description': "No description available",
                'country': None,
                'image_url': None,
                'rating_lordfilm': None,
                'voters': 0
            }

    def _parse_rating(self, parent_element, selector: str) -> float:
        """Helper method to parse rating from element"""
        elem = parent_element.select_one(selector)
        if elem:
            try:
                return float(elem.get_text(strip=True))
            except ValueError:
                return None
        return None

    def _calculate_final_rating(self, movie: Dict) -> float:
        """Calculate final rating based on the formula"""
        rating_avg = movie.get('rating_avg')
        details = self._fetch_movie_details(movie['link'])
        rating_lordfilm = details.get('rating_lordfilm')
        voters = details.get('voters', 0)

        if rating_avg is None or rating_lordfilm is None:
            return rating_avg or rating_lordfilm

        w = min(voters / MAX_VOTERS, 1.0)  # Вес не может быть больше 1
        result = rating_avg * (1 - w) + rating_lordfilm * w
        return round(result, 1)

    def get_sorted_movies(self, pages: int = DEFAULT_PAGES, sort_by: str = 'kp') -> List[Dict]:
        """Get and sort movies with caching and final rating calculation"""
        if self._cached_movies is None:
            self._cached_movies = self._fetch_movies(pages)

        # Filter by year if specified
        if self.year:
            self._cached_movies = [m for m in self._cached_movies if m.get('year') == self.year]

        # Calculate final ratings for all movies
        for movie in self._cached_movies:
            movie['final_rating'] = self._calculate_final_rating(movie)

        # Determine sorting key
        key = {
            'avg': 'rating_avg',
            'imdb': 'rating_imdb',
            'final': 'final_rating'
        }.get(sort_by.lower(), 'rating_kp')

        return sorted(
            self._cached_movies,
            key=lambda x: (x[key] is not None, x[key]),
            reverse=True
        )

    def save_to_json(self, data: List[Dict], filename: str = 'movies.json'):
        """Save data to JSON file"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        if self.debug:
            print(f"Data saved to {filename}")

    def save_to_html(self, data: List[Dict], filename: str = 'top_movies.html'):
        """Save data to HTML file with detailed information"""
        html_content = """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Top Movies</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 20px; }
                .movie { display: flex; margin-bottom: 30px; border-bottom: 1px solid #eee; padding-bottom: 20px; }
                .movie-poster { margin-right: 20px; min-width: 200px; }
                .movie-poster img { max-width: 200px; max-height: 300px; object-fit: cover; }
                .movie-info { flex: 1; }
                .movie-title { font-size: 1.5em; margin-bottom: 5px; }
                .movie-title a { color: inherit; text-decoration: none; }
                .movie-title a:hover { text-decoration: underline; }
                .movie-meta { color: #666; margin-bottom: 10px; }
                .movie-description { line-height: 1.5; }
                .no-image { width: 200px; height: 300px; background: #eee; display: flex; align-items: center; justify-content: center; }
                .movie-link { font-size: 0.9em; margin-top: 5px; }
                .movie-rating { margin-top: 5px; }
                .movie-rating span { display: inline-block; margin-right: 15px; }
            </style>
        </head>
        <body>
            <h1>Top Movies</h1>
        """

        for idx, movie in enumerate(data, 1):
            details = self._fetch_movie_details(movie['link'])
            movie_id = f"movie_{idx}"

            # Download image
            image_path = ""
            if details['image_url']:
                image_path = self._download_image(details['image_url'], movie_id)

            # Make image clickable
            image_html = ""
            if image_path:
                image_html = f'<a href="{movie["link"]}" target="_blank"><img src="movie_images/{image_path}" alt="{movie["title"]}"></a>'
            else:
                image_html = '<div class="no-image">No image</div>'

            html_content += f"""
            <div class="movie">
                <div class="movie-poster">
                    {image_html}
                </div>
                <div class="movie-info">
                    <h2 class="movie-title"><a href="{movie['link']}" target="_blank">{idx}. {movie['title']}</a></h2>
                    <div class="movie-meta">
                        <strong>Year:</strong> {movie['year']} | 
                        <strong>Country:</strong> {details['country'] or 'N/A'} | 
                        <strong>Voters:</strong> {details['voters'] or '0'}
                    </div>
                    <div class="movie-rating">
                        <span><strong>KP:</strong> {movie['rating_kp'] or 'N/A'}</span>
                        <span><strong>IMDb:</strong> {movie['rating_imdb'] or 'N/A'}</span>
                        <span><strong>Avg:</strong> {movie['rating_avg'] or 'N/A'}</span>
                        <span><strong>LordFilm:</strong> {details['rating_lordfilm'] or 'N/A'}</span>
                        <span><strong>Final:</strong> {movie['final_rating'] or 'N/A'}</span>
                    </div>
                    <div class="movie-description">
                        {details['description']}
                    </div>
                    <div class="movie-link">
                        <a href="{movie['link']}" target="_blank">View on site →</a>
                    </div>
                </div>
            </div>
            """

        html_content += """
        </body>
        </html>
        """

        with open(filename, 'w', encoding='utf-8') as f:
            f.write(html_content)
        if self.debug:
            print(f"HTML saved to {filename}")


if __name__ == "__main__":
    # Initialize parser
    try:
        parser = LordFilmParser(
            year=YEAR if 'YEAR' in globals() else DEFAULT_YEAR,
            debug=DEBUG if 'DEBUG' in globals() else DEFAULT_DEBUG
        )
    except NameError:
        parser = LordFilmParser()

    # Get pages and top_list values safely
    pages = PAGES if 'PAGES' in globals() else DEFAULT_PAGES
    top_list = TOP_LIST if 'TOP_LIST' in globals() else DEFAULT_TOP_LIST

    # Get and save top movies by different criteria
    for sort_criteria, filename in [('kp', 'top_kp.json'),
                                    ('imdb', 'top_imdb.json'),
                                    ('avg', 'top_avg.json'),
                                    ('final', 'top_final.json')]:  # Добавлен новый критерий сортировки
        top_movies = parser.get_sorted_movies(pages=pages, sort_by=sort_criteria)[:top_list]
        parser.save_to_json(top_movies, filename)

        # Save HTML only for final rating
        if sort_criteria == 'final':
            parser.save_to_html(top_movies, 'top_movies.html')

        # Print top N with links (where N = top_list)
        print(f"\nTop {top_list} by {sort_criteria.upper()}:")
        for i, movie in enumerate(top_movies, 1):
            rating = movie[f'rating_{sort_criteria}'] if sort_criteria in ['kp', 'imdb'] else movie[
                'rating_avg'] if sort_criteria == 'avg' else movie['final_rating']
            rating_display = f"{rating:.1f}" if isinstance(rating, (int, float)) else str(rating)
            print(f"{i}. {movie['title']} ({movie['year']}) - {rating_display} {movie['link']}")