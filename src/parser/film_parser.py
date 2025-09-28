"""
Парсер деталей фильмов с LordFilm
"""
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import re
import time
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime

from src.config.settings import SELENIUM_URL
from src.database.connection import db_manager
from src.database.models import Film, ParsingSession, ParsingHistory
from src.utils.retry import retry_on_failure
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


class FilmParser:
    """Парсер для извлечения детальной информации о фильмах"""

    def __init__(self):
        self.driver: Optional[webdriver.Remote] = None

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
                command_executor=SELENIUM_URL,
                options=chrome_options
            )
            logger.info("✅ Selenium WebDriver инициализирован")
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации WebDriver: {e}")
            raise

    @retry_on_failure(max_retries=2)
    def parse_film_details(self, film_url: str) -> Dict[str, Any]:
        """
        Парсит детальную информацию о фильме

        Args:
            film_url: URL страницы фильма

        Returns:
            Словарь с данными фильма
        """
        if not self.driver:
            self.setup_driver()

        logger.info(f"🎬 Парсим фильм: {film_url}")

        try:
            self.driver.get(film_url)

            # Ждем загрузки страницы
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )

            time.sleep(3)  # Даем время для загрузки динамического контента

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
                'lf_rating': self._extract_lf_rating(),
                'lf_likes': self._extract_lf_likes(),
                'lf_dislikes': self._extract_lf_dislikes(),
                'kp_rating': self._extract_kp_rating(),
                'imdb_rating': self._extract_imdb_rating(),
            }

            logger.info(f"✅ Успешно распарсены данные для: {film_data.get('title', 'Unknown')}")
            return film_data

        except TimeoutException:
            logger.error(f"⏰ Таймаут загрузки страницы фильма: {film_url}")
            return {'url': film_url, 'error': 'Timeout'}
        except Exception as e:
            logger.error(f"❌ Ошибка парсинга фильма {film_url}: {e}")
            raise

    def _extract_title(self) -> Optional[str]:
        """Извлекает русское название фильма"""
        try:
            # Ищем в заголовке h1/h2
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
        """Извлекает оригинальное название"""
        try:
            # Ищем по паттерну "Оригинальное название:"
            body_text = self.driver.find_element(By.TAG_NAME, "body").text
            match = re.search(r'Оригинальное название[:\s]*([^\n]+)', body_text, re.IGNORECASE)
            return match.group(1).strip() if match else None
        except:
            return None

    def _extract_year(self) -> Optional[int]:
        """Извлекает год выпуска"""
        try:
            body_text = self.driver.find_element(By.TAG_NAME, "body").text
            match = re.search(r'Год выхода[:\s]*(\d{4})', body_text)
            if match:
                return int(match.group(1))

            # Альтернативный поиск
            match = re.search(r'Год[:\s]*(\d{4})', body_text, re.IGNORECASE)
            return int(match.group(1)) if match else None
        except:
            return None

    def _extract_country(self) -> Optional[str]:
        """Извлекает страну производства"""
        try:
            body_text = self.driver.find_element(By.TAG_NAME, "body").text
            match = re.search(r'Страна[:\s]*([^\n]+)', body_text, re.IGNORECASE)
            return match.group(1).strip() if match else None
        except:
            return None

    def _extract_categories(self) -> List[str]:
        """Извлекает категории/жанры"""
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
        """Извлекает режиссера"""
        try:
            body_text = self.driver.find_element(By.TAG_NAME, "body").text
            match = re.search(r'Режиссер[:\s]*([^\n]+)', body_text, re.IGNORECASE)
            return match.group(1).strip() if match else None
        except:
            return None

    def _extract_actors(self) -> List[str]:
        """Извлекает список актеров"""
        try:
            body_text = self.driver.find_element(By.TAG_NAME, "body").text

            # Ищем блок с актерами
            patterns = [
                r'Актеры[:\s]*([^А-Я]{10,500})',
                r'В ролях[:\s]*([^А-Я]{10,500})',
            ]

            for pattern in patterns:
                match = re.search(pattern, body_text, re.IGNORECASE | re.DOTALL)
                if match:
                    actors_text = match.group(1).strip()
                    actors = [actor.strip() for actor in re.split(r'[,\n]', actors_text) if len(actor.strip()) > 2]
                    return actors[:15]  # Ограничиваем список

            return []
        except:
            return []

    def _extract_description(self) -> Optional[str]:
        """Извлекает описание сюжета"""
        try:
            # Ищем блок с описанием
            selectors = [".description", ".plot", ".story", "p"]
            for selector in selectors:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    text = element.text.strip()
                    if len(text) > 100:  # Предполагаем что описание достаточно длинное
                        return text
            return None
        except:
            return None

    def _extract_lf_rating_debug(self) -> dict:
        """Debug версия: извлекает рейтинг LordFilm с информацией о стратегиях"""
        result = {
            'rating': None,
            'likes': None,
            'dislikes': None,
            'strategy': None,
            'debug_info': '',
            'all_matches': []
        }

        try:
            body_text = self.driver.find_element(By.TAG_NAME, "body").text

            # Стратегия 1: Паттерн "число число.число число"
            pattern1 = r'(\d+)\s+(\d+\.\d+)\s+(\d+)'
            matches1 = re.findall(pattern1, body_text)
            result['all_matches'].append(f"pattern1: {matches1}")

            if matches1:
                likes, rating, dislikes = matches1[0]
                result['rating'] = float(rating)
                result['likes'] = int(likes)
                result['dislikes'] = int(dislikes)
                result['strategy'] = 'regex_triple_pattern'
                result['debug_info'] = f"Найден по паттерну: число число.число число"
                return result

            # Стратегия 2: Поиск отдельных чисел в определенных элементах
            rating_elements = self.driver.find_elements(By.CSS_SELECTOR,
                                                        "[class*='rating'], [class*='like'], [class*='dislike']")

            for element in rating_elements:
                text = element.text.strip()
                result['all_matches'].append(f"element_{element.get_attribute('class')}: {text}")

                # Ищем три числа подряд
                numbers = re.findall(r'\d+', text)
                if len(numbers) >= 3:
                    result['likes'] = int(numbers[0])
                    result['rating'] = float(f"{numbers[1]}.{numbers[2]}")
                    result['dislikes'] = int(numbers[3]) if len(numbers) > 3 else None
                    result['strategy'] = 'element_class_triple'
                    result['debug_info'] = f"Найден в элементе: {element.get_attribute('class')}"
                    return result

            # Стратегия 3: Поиск в разных местах
            # Ищем все числа с точками (рейтинги)
            all_ratings = re.findall(r'\d+\.\d+', body_text)
            # Ищем все целые числа (лайки/дизлайки)
            all_integers = re.findall(r'\b\d+\b', body_text)

            result['all_matches'].append(f"all_ratings: {all_ratings}")
            result['all_matches'].append(f"all_integers: {all_integers}")

            result['debug_info'] = "Рейтинг не найден ни одной стратегией"

        except Exception as e:
            result['debug_info'] = f"Ошибка: {str(e)}"

        return result

    def _extract_lf_likes(self) -> Optional[int]:
        """Извлекает количество лайков LordFilm"""
        try:
            body_text = self.driver.find_element(By.TAG_NAME, "body").text
            rating_pattern = re.findall(r'(\d+)\s+(\d+\.\d+)\s+(\d+)', body_text)
            if rating_pattern:
                return int(rating_pattern[0][0])  # Первое число - лайки
            return None
        except:
            return None

    def _extract_lf_dislikes(self) -> Optional[int]:
        """Извлекает количество дизлайков LordFilm"""
        try:
            body_text = self.driver.find_element(By.TAG_NAME, "body").text
            rating_pattern = re.findall(r'(\d+)\s+(\d+\.\d+)\s+(\d+)', body_text)
            if rating_pattern:
                return int(rating_pattern[0][2])  # Третье число - дизлайки
            return None
        except:
            return None

    def _extract_kp_rating(self) -> Optional[float]:
        """Извлекает рейтинг КиноПоиск"""
        try:
            # Основная стратегия: поиск по классу th-rate-kp
            kp_elements = self.driver.find_elements(By.CSS_SELECTOR,
                                                    ".th-rate-kp, [class*='kp'], [class*='kinopoisk']")

            for element in kp_elements:
                text = element.text.strip()
                rating_match = re.search(r'(\d+\.\d+)', text)
                if rating_match:
                    rating = float(rating_match.group(1))
                    if 0 <= rating <= 10:
                        return rating

            # Резервная стратегия: поиск в тексте
            body_text = self.driver.find_element(By.TAG_NAME, "body").text
            match = re.search(r'КП\s*[:\-]?\s*(\d+\.\d+)', body_text, re.IGNORECASE)
            if match:
                rating = float(match.group(1))
                if 0 <= rating <= 10:
                    return rating

            return None

        except Exception as e:
            logger.debug(f"Ошибка извлечения KP рейтинга: {e}")
            return None

    def _extract_imdb_rating(self) -> Optional[float]:
        """Извлекает рейтинг IMDB"""
        try:
            # Основная стратегия: поиск по классу th-rate-imdb
            imdb_elements = self.driver.find_elements(By.CSS_SELECTOR,
                                                      ".th-rate-imdb, [class*='imdb']")

            for element in imdb_elements:
                text = element.text.strip()
                rating_match = re.search(r'(\d+\.\d+)', text)
                if rating_match:
                    rating = float(rating_match.group(1))
                    if 0 <= rating <= 10:
                        return rating

            # Резервная стратегия: поиск в тексте
            body_text = self.driver.find_element(By.TAG_NAME, "body").text
            match = re.search(r'IMDB\s*[:\-]?\s*(\d+\.\d+)', body_text, re.IGNORECASE)
            if match:
                rating = float(match.group(1))
                if 0 <= rating <= 10:
                    return rating

            return None

        except Exception as e:
            logger.debug(f"Ошибка извлечения IMDB рейтинга: {e}")
            return None

    def update_film_in_db(self, film_data: Dict[str, Any], session_id: int) -> bool:
        """
        Обновляет данные фильма в БД

        Args:
            film_data: Данные фильма
            session_id: ID сессии парсинга

        Returns:
            True если успешно
        """
        session = db_manager.get_session()

        try:
            # Находим фильм по URL
            film = session.query(Film).filter(Film.url == film_data['url']).first()

            if not film:
                logger.warning(f"⚠️ Фильм не найден в БД: {film_data['url']}")
                return False

            # Обновляем данные
            film.title = film_data.get('title')
            film.original_title = film_data.get('original_title')
            film.year = film_data.get('year')
            film.country = film_data.get('country')
            film.categories = film_data.get('categories', [])
            film.director = film_data.get('director')
            film.actors = film_data.get('actors', [])
            film.description = film_data.get('description')
            film.lf_rating = film_data.get('lf_rating')
            film.lf_likes = film_data.get('lf_likes')
            film.lf_dislikes = film_data.get('lf_dislikes')
            film.kp_rating = film_data.get('kp_rating')
            film.imdb_rating = film_data.get('imdb_rating')
            film.is_active = True  # Если фильм парсится - он активен

            # Записываем в историю
            history = ParsingHistory(
                film_id=film.id,
                session_id=session_id,
                parsing_type="film_details",
                success=True,
                data_processed={"title": film_data.get('title')}
            )
            session.add(history)

            session.commit()
            logger.info(f"💾 Обновлены данные для: {film_data.get('title', 'Unknown')}")
            return True

        except Exception as e:
            session.rollback()
            logger.error(f"❌ Ошибка обновления фильма в БД: {e}")
            return False
        finally:
            session.close()

    def parse_films_batch(self, film_urls: List[str], session_id: int, batch_size: int = 10) -> dict:
        """
        Парсит пачку фильмов и обновляет данные в БД

        Args:
            film_urls: Список URL фильмов
            session_id: ID сессии парсинга
            batch_size: Размер пачки для обработки

        Returns:
            Статистика парсинга
        """
        stats = {
            "films_processed": 0,
            "films_updated": 0,
            "errors": 0
        }

        current_batch = []

        for i, film_url in enumerate(film_urls):
            try:
                # Парсим фильм
                film_data = self.parse_film_details(film_url)
                current_batch.append(film_data)

                stats["films_processed"] += 1

                logger.info(f"🎬 Обработан фильм {i + 1}/{len(film_urls)}: {film_data.get('title', 'Unknown')}")

                # Обновляем пачку в БД
                if len(current_batch) >= batch_size or i == len(film_urls) - 1:
                    for film_data in current_batch:
                        if 'error' not in film_data:
                            success = self.update_film_in_db(film_data, session_id)
                            if success:
                                stats["films_updated"] += 1

                    current_batch = []

                    # Пауза между пачками
                    time.sleep(2)

            except Exception as e:
                stats["errors"] += 1
                logger.error(f"❌ Ошибка обработки фильма {film_url}: {e}")
                continue

        return stats

    def close(self):
        """Закрывает WebDriver"""
        if self.driver:
            self.driver.quit()
            logger.info("🔚 WebDriver закрыт")


# Синглтон экземпляр
film_parser = FilmParser()