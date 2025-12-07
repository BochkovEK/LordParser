"""
Film Parser for LordFilm
Extracts detailed information about films
"""

import time
import re
import logging
from typing import Dict, Any, Optional
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from src.config.config import DEFAULT_URL, SELENIUM_URL

logger = logging.getLogger(__name__)


class FilmParser:
    """Parser for extracting film details from film pages"""

    def __init__(self, base_url: str = None):
        """
        Initialize film parser

        Args:
            base_url: Base URL for reference (default from config)
        """
        self.base_url = base_url or DEFAULT_URL
        self.driver: Optional[webdriver.Remote] = None

        # Selenium configuration
        self.selenium_url = SELENIUM_URL
        self.timeout = 15  # seconds
        self.load_delay = 3  # seconds for dynamic content

    def setup_driver(self) -> None:
        """Setup Selenium WebDriver"""
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")

        try:
            self.driver = webdriver.Remote(
                command_executor=self.selenium_url,
                options=chrome_options
            )
            logger.info("Selenium WebDriver initialized for film parser")
        except Exception as e:
            logger.error(f"Failed to initialize WebDriver: {e}")
            raise

    def parse_film_details(self, film_url: str) -> Dict[str, Any]:
        """
        Parse detailed information about a film

        Args:
            film_url: URL of the film page

        Returns:
            Dictionary with film data or error information
        """
        if not self.driver:
            self.setup_driver()

        logger.debug(f"Parsing film: {film_url}")

        try:
            # Load film page
            self.driver.get(film_url)

            # Wait for page to load
            WebDriverWait(self.driver, self.timeout).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )

            time.sleep(self.load_delay)  # Allow dynamic content to load

            # Extract film data
            film_data = {
                'url': film_url,
                'title': self._extract_title(),
                'original_title': self._extract_original_title(),
                'year': self._extract_year(),
                'country': self._extract_country(),
                'categories': self._extract_categories(),
                'director': self._extract_director(),
                'actors': self._extract_actors(),
                'description': self._extract_description(),
                'kp_rating': self._extract_kp_rating(),
                'imdb_rating': self._extract_imdb_rating(),
                'lf_likes': self._extract_lf_likes(),
                'lf_dislikes': self._extract_lf_dislikes(),
                'lf_rating': self._extract_lf_rating(),
                'parsed_at': datetime.now().isoformat()
            }

            logger.info(f"Parsed film: {film_data.get('title', 'Unknown')}")
            return film_data

        except TimeoutException:
            logger.warning(f"Timeout loading film page: {film_url}")
            return {'url': film_url, 'error': 'Timeout loading page'}
        except Exception as e:
            logger.error(f"Error parsing film {film_url}: {e}")
            return {'url': film_url, 'error': str(e)}

    # --- Extraction methods ---

    def _extract_title(self) -> Optional[str]:
        """Extract Russian title"""
        try:
            selectors = ["h1", "h2", ".title", ".film-title"]
            for selector in selectors:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    text = element.text.strip()
                    if text and 'смотреть онлайн' in text.lower():
                        return text.split('смотреть онлайн')[0].strip()
            return None
        except:
            return None

    def _extract_original_title(self) -> Optional[str]:
        """Extract original title"""
        try:
            body_text = self.driver.find_element(By.TAG_NAME, "body").text
            match = re.search(r'Оригинальное название[:\s]*([^\n]+)', body_text, re.IGNORECASE)
            return match.group(1).strip() if match else None
        except:
            return None

    def _extract_year(self) -> Optional[int]:
        """Extract release year"""
        try:
            body_text = self.driver.find_element(By.TAG_NAME, "body").text
            match = re.search(r'Год выхода[:\s]*(\d{4})', body_text)
            if match:
                return int(match.group(1))

            match = re.search(r'Год[:\s]*(\d{4})', body_text, re.IGNORECASE)
            return int(match.group(1)) if match else None
        except:
            return None

    def _extract_country(self) -> Optional[str]:
        """Extract country"""
        try:
            body_text = self.driver.find_element(By.TAG_NAME, "body").text
            match = re.search(r'Страна[:\s]*([^\n]+)', body_text, re.IGNORECASE)
            return match.group(1).strip() if match else None
        except:
            return None

    def _extract_categories(self) -> list:
        """Extract categories/genres"""
        try:
            body_text = self.driver.find_element(By.TAG_NAME, "body").text
            match = re.search(r'Категории?[:\s]*([^\n]+)', body_text, re.IGNORECASE)
            if match:
                categories_text = match.group(1).strip()
                return [cat.strip() for cat in categories_text.split('/') if cat.strip()]
            return []
        except:
            return []

    def _extract_director(self) -> Optional[str]:
        """Extract director"""
        try:
            body_text = self.driver.find_element(By.TAG_NAME, "body").text
            match = re.search(r'Режиссер[:\s]*([^\n]+)', body_text, re.IGNORECASE)
            return match.group(1).strip() if match else None
        except:
            return None

    def _extract_actors(self) -> list:
        """Extract actors list"""
        try:
            body_text = self.driver.find_element(By.TAG_NAME, "body").text

            patterns = [
                r'Актеры[:\s]*([^А-Я]{10,500})',
                r'В ролях[:\s]*([^А-Я]{10,500})',
            ]

            for pattern in patterns:
                match = re.search(pattern, body_text, re.IGNORECASE | re.DOTALL)
                if match:
                    actors_text = match.group(1).strip()
                    actors = [actor.strip() for actor in re.split(r'[,\n]', actors_text) if len(actor.strip()) > 2]
                    return actors[:15]  # Limit list

            return []
        except:
            return []

    def _extract_description(self) -> Optional[str]:
        """Extract plot description"""
        try:
            selectors = [".description", ".plot", ".story", "p"]
            for selector in selectors:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    text = element.text.strip()
                    if len(text) > 100:  # Assume description is long enough
                        return text
            return None
        except:
            return None

    def _extract_lf_likes(self) -> Optional[int]:
        """Extract LordFilm likes"""
        try:
            likes_elements = self.driver.find_elements(By.CSS_SELECTOR, "div.rate-plus span.psc")
            if likes_elements:
                likes_text = likes_elements[0].text.strip()
                if likes_text.isdigit():
                    return int(likes_text)
            return None
        except Exception as e:
            logger.debug(f"Error extracting likes: {e}")
            return None

    def _extract_lf_dislikes(self) -> Optional[int]:
        """Extract LordFilm dislikes"""
        try:
            dislikes_elements = self.driver.find_elements(By.CSS_SELECTOR, "div.rate-minus span.msc")
            if dislikes_elements:
                dislikes_text = dislikes_elements[0].text.strip()
                if dislikes_text.isdigit():
                    return int(dislikes_text)
            return None
        except Exception as e:
            logger.debug(f"Error extracting dislikes: {e}")
            return None

    def _extract_lf_rating(self) -> Optional[float]:
        """Calculate LordFilm rating: (likes / total) * 10"""
        likes = self._extract_lf_likes()
        dislikes = self._extract_lf_dislikes()

        if likes is None or dislikes is None:
            return None

        total = likes + dislikes
        if total == 0:
            return 0.0

        rating = (likes / total) * 10
        return round(rating, 2)

    def _extract_kp_rating(self) -> Optional[float]:
        """Extract Kinopoisk rating"""
        try:
            kp_elements = self.driver.find_elements(By.CSS_SELECTOR, "div.frate.frate-kp span")
            if kp_elements:
                kp_text = kp_elements[0].text.strip()
                try:
                    return float(kp_text)
                except ValueError:
                    return None
            return None
        except Exception as e:
            logger.debug(f"Error extracting KP rating: {e}")
            return None

    def _extract_imdb_rating(self) -> Optional[float]:
        """Extract IMDB rating"""
        try:
            imdb_elements = self.driver.find_elements(By.CSS_SELECTOR, "div.frate.frate-imdb span")
            if imdb_elements:
                imdb_text = imdb_elements[0].text.strip()
                try:
                    return float(imdb_text)
                except ValueError:
                    return None
            return None
        except Exception as e:
            logger.debug(f"Error extracting IMDB rating: {e}")
            return None

    def close(self):
        """Close WebDriver"""
        if self.driver:
            self.driver.quit()
            logger.info("Film parser WebDriver closed")


# Factory function
def create_film_parser(base_url: str = None) -> FilmParser:
    """Create and return a new FilmParser instance"""
    return FilmParser(base_url=base_url)