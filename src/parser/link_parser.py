"""
Парсер ссылок на фильмы со страниц LordFilm
"""
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from urllib.parse import urljoin
import time
import logging
from typing import List, Optional

# Импорты ВНЕ класса
from src.config.settings import SELENIUM_URL
from src.parser.url_generator import URLGenerator
from src.database.connection import db_manager
from src.database.models import Film, ParsingSession, ParsingHistory
from src.utils.retry import retry_on_failure
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


class LinkParser:
    """Парсер для извлечения ссылок на фильмы со страниц каталога"""

    def __init__(self):
        self.driver: Optional[webdriver.Remote] = None
        self.url_generator = URLGenerator()
        self.base_url = self.url_generator.base_url

    def setup_driver(self) -> None:
        """Настройка Selenium WebDriver"""
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")

        try:
            self.driver = webdriver.Remote(
                command_executor=SELENIUM_URL,  # Используем глобальную переменную
                options=chrome_options
            )
            logger.info("✅ Selenium WebDriver инициализирован")
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации WebDriver: {e}")
            raise

    @retry_on_failure(max_retries=2)
    def parse_links_from_page(self, page_url: str) -> List[str]:
        """
        Извлекает все ссылки на фильмы с одной страницы каталога

        Args:
            page_url: URL страницы каталога

        Returns:
            Список абсолютных URL на фильмы
        """
        if not self.driver:
            self.setup_driver()

        logger.info(f"🔍 Парсим страницу: {page_url}")

        try:
            self.driver.get(page_url)

            # Ждем загрузки контента
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )

            time.sleep(2)  # Даем время для загрузки динамического контента

            # Ищем ссылки на фильмы
            # Обычно это ссылки внутри элементов с фильмами
            film_links = []

            # Стратегия 1: Ищем ссылки в карточках фильмов
            link_selectors = [
                "a[href*='/filmy/']",  # ссылки содержащие '/filmy/'
                ".movie-item a",
                ".film-item a",
                ".item a",
                "h2 a",  # заголовки часто содержат ссылки
            ]

            for selector in link_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for element in elements:
                        href = element.get_attribute("href")
                        if href and "/filmy/" in href and href not in film_links:
                            # Преобразуем в абсолютный URL если нужно
                            absolute_url = urljoin(self.base_url, href)
                            film_links.append(absolute_url)
                except Exception as e:
                    logger.debug(f"Селектор {selector} не сработал: {e}")
                    continue

            # Убираем дубликаты
            film_links = list(set(film_links))

            logger.info(f"✅ Найдено {len(film_links)} ссылок на странице {page_url}")
            return film_links

        except TimeoutException:
            logger.error(f"⏰ Таймаут загрузки страницы: {page_url}")
            return []
        except Exception as e:
            logger.error(f"❌ Ошибка парсинга страницы {page_url}: {e}")
            raise

    def save_links_to_db(self, links: List[str], session_id: int) -> int:
        """
        Сохраняет ссылки в базу данных
        """
        session = db_manager.get_session()
        new_films = 0

        try:
            for link in links:
                # Проверяем существует ли фильм
                existing_film = session.query(Film).filter(Film.url == link).first()

                if not existing_film:
                    film = Film(
                        url=link,
                        is_active=True
                        # first_seen_at будет автоматически установлен в datetime.utcnow()
                    )
                    session.add(film)
                    new_films += 1

                # Записываем в историю парсинга
                history = ParsingHistory(
                    film_id=existing_film.id if existing_film else None,
                    session_id=session_id,
                    parsing_type="links",
                    success=True,
                    data_processed={"url": link}
                )
                session.add(history)

            session.commit()
            logger.info(f"💾 Сохранено {len(links)} ссылок, новых: {new_films}")

        except Exception as e:
            session.rollback()
            logger.error(f"❌ Ошибка сохранения ссылок в БД: {e}")
            raise
        finally:
            session.close()

        return new_films

    def parse_links_batch(self, urls: List[str], session_id: int, batch_size: int = 10) -> dict:
        """
        Парсит пачку страниц и сохраняет ссылки

        Args:
            urls: Список URL страниц для парсинга
            session_id: ID сессии парсинга
            batch_size: Размер пачки для обработки

        Returns:
            Статистика парсинга
        """
        stats = {
            "pages_processed": 0,
            "links_found": 0,
            "new_films": 0,
            "errors": 0
        }

        current_batch = []

        for i, page_url in enumerate(urls):
            try:
                # Парсим страницу
                film_links = self.parse_links_from_page(page_url)
                current_batch.extend(film_links)

                stats["pages_processed"] += 1
                stats["links_found"] += len(film_links)

                logger.info(f"📄 Обработана страница {i + 1}/{len(urls)}: {len(film_links)} ссылок")

                # Сохраняем пачку
                if len(current_batch) >= batch_size or i == len(urls) - 1:
                    if current_batch:
                        new_films = self.save_links_to_db(current_batch, session_id)
                        stats["new_films"] += new_films
                        current_batch = []

                    # Пауза между пачками
                    time.sleep(1)

            except Exception as e:
                stats["errors"] += 1
                logger.error(f"❌ Ошибка обработки страницы {page_url}: {e}")
                continue

        return stats

    def close(self):
        """Закрывает WebDriver"""
        if self.driver:
            self.driver.quit()
            logger.info("🔚 WebDriver закрыт")


# Синглтон экземпляр
link_parser = LinkParser()