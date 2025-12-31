"""
Link Parser for LordFilm
Extracts film links from catalog pages
"""

import time
import re
import logging
from typing import List, Optional
from urllib.parse import urljoin

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import WebDriverException

from src.config.config import DEFAULT_URL, SELENIUM_URL

logger = logging.getLogger(__name__)


class LinkParser:
    """Parser for extracting film links from catalog pages"""

    MAX_RETRY_ATTEMPTS = 3
    RETRY_DELAYS = [2, 4, 8]  # exponential backoff in seconds

    def __init__(self, base_url: str = None):
        """
        Initialize link parser

        Args:
            base_url: Base URL for building absolute links (default from config)
        """
        self.base_url = base_url or DEFAULT_URL
        self.driver: Optional[webdriver.Remote] = None

        # Selenium configuration
        self.selenium_url = SELENIUM_URL
        self.timeout = 10  # seconds
        self.load_delay = 2  # seconds

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
            logger.info("Selenium WebDriver initialized")
        except Exception as e:
            logger.error(f"Failed to initialize WebDriver: {e}")
            raise

    def parse_links_from_page(self, page_url: str) -> List[str]:
        """
        Extract all film links from a single catalog page with retries

        Args:
            page_url: URL of the catalog page

        Returns:
            List of absolute URLs to film pages

        Raises:
            ConnectionError: If site is unavailable after all retry attempts
            TimeoutError: If page loading times out repeatedly
        """
        if not self.driver:
            self.setup_driver()

        for attempt in range(1, self.MAX_RETRY_ATTEMPTS + 1):
            logger.debug(f"Parsing page (attempt {attempt}/{self.MAX_RETRY_ATTEMPTS}): {page_url}")

            try:
                # Load page
                self.driver.get(page_url)

                # Wait for content
                WebDriverWait(self.driver, self.timeout).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )

                time.sleep(self.load_delay)

                # Find film links
                film_links = self._extract_film_links()

                logger.info(f"Found {len(film_links)} films on page {page_url}")
                return film_links

            except TimeoutException as e:
                logger.warning(f"Timeout loading page (attempt {attempt}): {page_url}")
                if attempt == self.MAX_RETRY_ATTEMPTS:
                    raise ConnectionError(f"Site timeout after {self.MAX_RETRY_ATTEMPTS} attempts: {page_url}") from e

            except WebDriverException as e:
                # Network errors: ERR_NAME_NOT_RESOLVED, ERR_CONNECTION_REFUSED, etc.
                logger.warning(f"WebDriver error (attempt {attempt}): {str(e)[:100]}...")
                if attempt == self.MAX_RETRY_ATTEMPTS:
                    raise ConnectionError(f"Cannot access site {page_url}: {e}") from e

            except Exception as e:
                logger.error(f"Unexpected error parsing page {page_url}: {e}")
                if attempt == self.MAX_RETRY_ATTEMPTS:
                    raise ConnectionError(f"Parsing failed for {page_url}: {e}") from e

            # Retry delay
            if attempt < self.MAX_RETRY_ATTEMPTS:
                delay = self.RETRY_DELAYS[attempt - 1]
                logger.info(f"Retrying in {delay}s...")
                time.sleep(delay)

        # This should never be reached (exception raised above)
        raise ConnectionError(f"Failed to parse page after {self.MAX_RETRY_ATTEMPTS} attempts: {page_url}")

    def _extract_film_links(self) -> List[str]:
        """Extract film links from current page"""
        film_links = []

        # Film card selectors (order matters)
        card_selectors = [".th-item", ".th-in", ".short"]

        for selector in card_selectors:
            try:
                cards = self.driver.find_elements(By.CSS_SELECTOR, selector)

                for card in cards:
                    try:
                        link_element = card.find_element(By.CSS_SELECTOR, "a")
                        href = link_element.get_attribute("href")

                        if href and self._is_film_url(href):
                            absolute_url = urljoin(self.base_url, href)
                            if absolute_url not in film_links:
                                film_links.append(absolute_url)
                    except:
                        continue

                if film_links:
                    logger.debug(f"Selector '{selector}' worked, found {len(film_links)} links")
                    break

            except Exception as e:
                logger.debug(f"Selector '{selector}' failed: {e}")
                continue

        return film_links

    def _is_film_url(self, url: str) -> bool:
        """
        Check if URL points to a film page

        Pattern: /filmy/DIGITS-title-year.html
        Example: /filmy/12345-avatar-2025.html
        """
        if not url:
            return False

        # Must contain /filmy/
        if '/filmy/' not in url:
            return False

        # Exclude non-film pages
        exclude_patterns = [
            r'/filmy/$',
            r'/filmy/[^/]+/$',
            r'/filmy/\?',
            r'/filmy/\d{4}/page/',
            r'top-50', 'news', 'netflix', 'marvel', 'serialy', 'multfilmy'
        ]

        if any(re.search(pattern, url) for pattern in exclude_patterns):
            return False

        # Check film pattern
        film_pattern = r'/filmy/\d+-[^/]+-\d{4}\.html$'
        film_pattern_alt = r'/filmy/\d+-[^/]+\.html$'

        is_valid_film = (
            re.search(film_pattern, url) is not None or
            re.search(film_pattern_alt, url) is not None
        )

        # Must contain ID
        has_id = re.search(r'/filmy/(\d+)', url) is not None

        return is_valid_film and has_id

    def close(self):
        """Close WebDriver"""
        if self.driver:
            self.driver.quit()
            logger.info("WebDriver closed")


# Factory function for easy creation
def create_link_parser(base_url: str = None) -> LinkParser:
    """Create and return a new LinkParser instance"""
    return LinkParser(base_url=base_url)