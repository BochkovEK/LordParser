import requests
from bs4 import BeautifulSoup
from typing import List, Dict
import json

# Default constants
DEFAULT_URL = "https://mk.lordfilm17.ru"
DEFAULT_PAGES = 10
DEFAULT_TOP_LIST = 20
DEFAULT_YEAR = 2024
DEFAULT_DEBUG = False


class LordFilmParser:
    def __init__(self, base_url: str = DEFAULT_URL, year: int = DEFAULT_YEAR, debug: bool = DEFAULT_DEBUG):
        """Initialize the parser with base URL, year filter and debug mode"""
        self.base_url = base_url
        self.year = year
        self.debug = debug
        self.headers = {'User-Agent': 'Mozilla/5.0'}
        self._cached_movies = None  # Cache for loaded movies

    def _fetch_movies(self, pages: int) -> List[Dict]:
        """Internal method to fetch movies from specified pages"""
        all_movies = []
        use_year_in_url = True  # Start by trying with year in URL

        for page in range(1, pages + 1):
            # Try with year in URL first, fall back to general URL if 404 occurs
            url = f"{self.base_url}/filmy/{f'{self.year}/' if self.year and use_year_in_url else ''}page/{page}/"
            try:
                response = requests.get(url, headers=self.headers)

                # If we got 404 and were using year in URL, try without year
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

                for item in soup.select('.th-item'):
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
                        'rating_avg': round((rating_kp + rating_imdb) / 2, 1) if None not in (
                            rating_kp, rating_imdb) else None
                    })

            except requests.RequestException as e:
                print(f"Error loading page {page}: {e}")
                continue

        return all_movies

    def _parse_rating(self, parent_element, selector: str) -> float:
        """Helper method to parse rating from element"""
        elem = parent_element.select_one(selector)
        if elem:
            try:
                return float(elem.get_text(strip=True))
            except ValueError:
                return None  # Return None instead of 0.0 for missing ratings
        return None

    def get_sorted_movies(self, pages: int = DEFAULT_PAGES, sort_by: str = 'kp') -> List[Dict]:
        """Get and sort movies with caching"""
        if self._cached_movies is None:
            self._cached_movies = self._fetch_movies(pages)

        # Filter by year if specified
        if self.year:
            self._cached_movies = [m for m in self._cached_movies if m.get('year') == self.year]

        # Determine sorting key
        key = {
            'avg': 'rating_avg',
            'imdb': 'rating_imdb',
        }.get(sort_by.lower(), 'rating_kp')  # Default to Kinopoisk

        # Sort handling None values by putting them last
        return sorted(
            self._cached_movies,
            key=lambda x: (x[key] is not None, x[key]),  # Tuple sorting puts None last
            reverse=True
        )

    def save_to_json(self, data: List[Dict], filename: str = 'movies.json'):
        """Save data to JSON file"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        if self.debug:
            print(f"Data saved to {filename}")


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
                                    ('avg', 'top_avg.json')]:
        top_movies = parser.get_sorted_movies(pages=pages, sort_by=sort_criteria)[:top_list]
        parser.save_to_json(top_movies, filename)

        # Print top N with links (where N = top_list)
        print(f"\nTop {top_list} by {sort_criteria.upper()}:")
        for i, movie in enumerate(top_movies, 1):
            rating = movie[f'rating_{sort_criteria}'] if sort_criteria != 'avg' else movie['rating_avg']
            rating_display = f"{rating:.1f}" if isinstance(rating, (int, float)) else str(rating)
            print(f"{i}. {movie['title']} ({movie['year']}) - {rating_display} {movie['link']}")