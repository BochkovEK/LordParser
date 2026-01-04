"""
URL Generator for LordFilm Parser
Generates URLs for parsing based on templates and parameters
"""

from typing import Generator, List, Union
from src.config.config import DEFAULT_URL, DEFAULT_CATEGORY, YEAR_RANGE, PARSE_PAGES


class URLGenerator:
    """Generates URLs for parsing catalog pages"""

    def __init__(
        self,
        base_url: str = None,
        category: str = None,
        start_year: int = None,
        end_year: int = None,
        default_pages: int = None
    ):
        """
        Initialize URL generator with configurable parameters

        Args:
            base_url: Base site URL (default: DEFAULT_URL from config)
            category: Content category - filmy/serialy/multfilmy (default: DEFAULT_CATEGORY)
            start_year: Starting year for year-based parsing (default: YEAR_RANGE[0])
            end_year: Ending year for year-based parsing (default: YEAR_RANGE[1])
            default_pages: Default number of pages to parse (default: PARSE_PAGES)
        """
        self.base_url = base_url or DEFAULT_URL
        self.category = category or DEFAULT_CATEGORY
        self.start_year = start_year or YEAR_RANGE[0]
        self.end_year = end_year or YEAR_RANGE[1]
        self.default_pages = default_pages or PARSE_PAGES

        # Validate inputs
        if self.start_year > self.end_year:
            self.start_year, self.end_year = self.end_year, self.start_year

        if self.default_pages < 1:
            self.default_pages = 1

    def generate_from_template(self,
                             template_key: str,
                             pages: Union[int, str] = None,
                             years: List[int] = None) -> Generator[str, None, None]:
        """
        Generate URLs based on template and parameters

        Args:
            template_key: 'main_catalog' or 'yearly_catalog'
            pages: number of pages or 'all' for full depth
            years: list of years for yearly catalog
        """
        templates = {
            'main_catalog': f"{self.base_url}/{self.category}/page/{{page}}/",
            'yearly_catalog': f"{self.base_url}/{self.category}/{{year}}/page/{{page}}/"
        }

        if template_key not in templates:
            raise ValueError(f"Unknown template: {template_key}")

        pattern = templates[template_key]

        if template_key == 'main_catalog':
            yield from self._generate_main_catalog(pattern, pages)
        elif template_key == 'yearly_catalog':
            yield from self._generate_yearly_catalog(pattern, pages, years)

    def _generate_main_catalog(self, pattern: str, pages: Union[int, str]) -> Generator[str, None, None]:
        """Generate URLs for main catalog"""
        if pages is None:
            pages = 1
        elif pages == 'all':
            pages = self.default_pages

        for page in range(1, int(pages) + 1):
            yield pattern.format(page=page)

    def _generate_yearly_catalog(self,
                               pattern: str,
                               pages: Union[int, str],
                               years: List[int]) -> Generator[str, None, None]:
        """Generate URLs for yearly catalog"""
        if years is None:
            years = list(range(self.start_year, self.end_year + 1))

        if pages == 'all':
            pages = self.default_pages

        for year in years:
            for page in range(1, int(pages) + 1):
                yield pattern.format(year=year, page=page)


# Factory function for easy creation
def create_url_generator(
    base_url: str = None,
    category: str = None,
    start_year: int = None,
    end_year: int = None,
    default_pages: int = None
) -> URLGenerator:
    """Factory function for URLGenerator"""
    return URLGenerator(
        base_url=base_url,
        category=category,
        start_year=start_year,
        end_year=end_year,
        default_pages=default_pages
    )