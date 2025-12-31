"""
URL Generator for LordFilm Parser
Generates URLs for parsing based on templates and parameters
"""

from typing import Generator, List, Union
from src.config.config import DEFAULT_URL, DEFAULT_CATEGORY, YEAR_RANGE, PARSE_PAGES


class URLGenerator:
    """Generates URLs for parsing catalog pages"""

    def __init__(self, base_url: str = None):
        self.base_url = base_url or DEFAULT_URL
        self.category = DEFAULT_CATEGORY
        self.start_year, self.end_year = YEAR_RANGE
        self.default_pages = PARSE_PAGES

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
        if pages == 'all':
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


if __name__ == "__main__":
    # Test the generator
    generator = URLGenerator()

    print("🔗 URL Generator Test:")

    # Test main catalog
    main_urls = list(generator.generate_from_template('main_catalog', pages=2))
    print(f"Main catalog URLs (2 pages): {len(main_urls)}")
    for url in main_urls[:2]:
        print(f"  {url}")