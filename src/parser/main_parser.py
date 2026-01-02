"""
Main Parser - core orchestrator for LordFilm parsing operations
Production-ready version with graceful shutdown, timeouts, and health checks
"""

import asyncio
import signal
import logging
import time
from typing import Dict, Any, List, Optional
from enum import Enum
from datetime import datetime
from concurrent.futures import TimeoutError as FutureTimeoutError

# Конфигурация
from src.config.config import (
    DEFAULT_URL, DAILY_PAGES, WEEKLY_PAGES,
    LINKS_BATCH_SIZE, FILMS_BATCH_SIZE,
    BATCH_DELAY, FILM_DELAY,
    SELENIUM_TIMEOUT, REQUEST_TIMEOUT,
    MAX_CONCURRENT_PARSE_TASKS
)
from src.config.scheduler_conf import PARSING_SCHEDULES

# Парсеры
from src.parser.url_generator import URLGenerator
from src.parser.link_parser import create_link_parser
from src.parser.film_parser import create_film_parser

# База данных
from src.database.connection import db_manager
from src.database.models import Film, ParsingSession, ParsingHistory

# Утилиты
from src.utils.logger import setup_logger

# Настройка логгера
logger = setup_logger(__name__)


class ParseMode(Enum):
    """Parsing operation modes"""
    DAILY = "daily"
    WEEKLY = "weekly"
    MANUAL = "manual"


class ParseTask(Enum):
    """Specific parsing tasks"""
    UPDATE_RATINGS = "update_ratings"
    DISCOVER_FILMS = "discover_films"
    FULL_PARSE = "full_parse"
    PARSE_CATALOG = "parse_catalog"


class GracefulShutdown(Exception):
    """Exception for graceful shutdown"""
    pass


class HealthCheckError(Exception):
    """Exception for health check failures"""
    pass


class MainParser:
    """Main orchestrator for parsing operations with production features"""

    def __init__(self):
        self.url_generator = URLGenerator()
        self.link_parser = None
        self.film_parser = None
        self.session_id = None
        self.should_stop = False  # Флаг для graceful shutdown
        self.semaphore = None  # Для ограничения concurrent запросов

    async def main(self, mode: ParseMode, task: ParseTask, **kwargs) -> Dict[str, Any]:
        """
        Main entry point for parsing operations with production features

        Args:
            mode: DAILY, WEEKLY or MANUAL operation mode
            task: Specific task to execute
            **kwargs: Additional parameters (limit, dry_run, etc.)

        Returns:
            Execution results and statistics
        """
        start_time = time.time()

        # Параметры из kwargs
        limit = kwargs.get('limit')
        dry_run = kwargs.get('dry_run', False)

        logger.info(f"🚀 Starting parser: mode={mode.value}, task={task.value}, "
                   f"limit={limit}, dry_run={dry_run}")

        # Dry run - только проверка
        if dry_run:
            logger.info("🔍 Dry run mode - checking configuration only")
            try:
                await self._health_check()
                return {
                    "success": True,
                    "dry_run": True,
                    "message": "Configuration check passed"
                }
            except HealthCheckError as e:
                return {
                    "success": False,
                    "dry_run": True,
                    "error": str(e)
                }

        # Установка обработчиков сигналов для graceful shutdown
        self._setup_signal_handlers()

        try:
            # Проверка здоровья системы перед запуском
            await self._health_check()

            # Инициализация семафора для ограничения concurrent задач
            self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_PARSE_TASKS)

            # Создание сессии в БД
            self.session_id = await self._create_parsing_session(mode, task)

            # Выполнение задачи в зависимости от режима
            if mode == ParseMode.DAILY:
                result = await self._execute_daily_task(task, limit)
            elif mode == ParseMode.WEEKLY:
                result = await self._execute_weekly_task(task, limit)
            elif mode == ParseMode.MANUAL:
                result = await self._execute_manual_task(task, limit)
            else:
                raise ValueError(f"Unknown mode: {mode.value}")

            # Обновление статуса сессии
            await self._update_session_status(success=True)

            elapsed_time = time.time() - start_time
            result["execution_time"] = elapsed_time
            result["success"] = True

            logger.info(f"✅ Parser completed successfully in {elapsed_time:.1f}s")
            return result

        except GracefulShutdown:
            logger.info("🛑 Graceful shutdown requested")
            await self._update_session_status(success=False, error="Graceful shutdown")
            return {
                "success": False,
                "error": "Graceful shutdown",
                "partial_results": getattr(self, '_partial_stats', {})
            }

        except HealthCheckError as e:
            logger.error(f"❌ Health check failed: {e}")
            await self._update_session_status(success=False, error=f"Health check: {e}")
            return {"success": False, "error": f"Health check failed: {e}"}

        except Exception as e:
            logger.error(f"❌ Parser failed: {e}", exc_info=True)
            await self._update_session_status(success=False, error=str(e))
            return {"success": False, "error": str(e)}

        finally:
            # Очистка ресурсов
            await self._cleanup()

    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        loop = asyncio.get_event_loop()

        def signal_handler(signame):
            logger.warning(f"Received signal {signame}, initiating graceful shutdown...")
            self.should_stop = True

        for signame in ('SIGINT', 'SIGTERM'):
            try:
                loop.add_signal_handler(
                    getattr(signal, signame),
                    lambda s=signame: signal_handler(s)
                )
            except (ValueError, RuntimeError):
                # В некоторых средах (Windows) могут быть проблемы
                pass

    async def _health_check(self):
        """Check system health before starting"""
        checks = []

        # 1. Проверка БД
        try:
            session = db_manager.get_session()
            session.execute(text('SELECT 1'))
            session.close()
            checks.append(("database", True))
            logger.debug("✅ Database connection: OK")
        except Exception as e:
            checks.append(("database", False))
            logger.error(f"❌ Database connection failed: {e}")

        # 2. Проверка парсеров
        try:
            # Проверяем, что можем создать парсеры
            test_link_parser = create_link_parser()
            test_film_parser = create_film_parser()

            # Закрываем тестовые парсеры
            test_link_parser.close()
            test_film_parser.close()

            checks.append(("parsers", True))
            logger.debug("✅ Parsers initialization: OK")
        except Exception as e:
            checks.append(("parsers", False))
            logger.error(f"❌ Parsers initialization failed: {e}")

        # Проверяем все проверки
        failed = [service for service, ok in checks if not ok]
        if failed:
            raise HealthCheckError(f"Health check failed for: {', '.join(failed)}")

        logger.info("✅ All health checks passed")

    async def _execute_daily_task(self, task: ParseTask, limit: Optional[int] = None) -> Dict[str, Any]:
        """Execute daily parsing tasks"""
        if task == ParseTask.UPDATE_RATINGS:
            return await self._update_existing_ratings(limit)
        elif task == ParseTask.DISCOVER_FILMS:
            return await self._discover_new_films(limit)
        else:
            raise ValueError(f"Unknown daily task: {task.value}")

    async def _execute_weekly_task(self, task: ParseTask, limit: Optional[int] = None) -> Dict[str, Any]:
        """Execute weekly parsing tasks"""
        if task == ParseTask.FULL_PARSE:
            return await self._full_catalog_parse(limit)
        else:
            raise ValueError(f"Unknown weekly task: {task.value}")

    async def _execute_manual_task(self, task: ParseTask, limit: Optional[int] = None) -> Dict[str, Any]:
        """Execute manual parsing tasks"""
        if task == ParseTask.UPDATE_RATINGS:
            return await self._update_existing_ratings(limit)
        elif task == ParseTask.DISCOVER_FILMS:
            return await self._discover_new_films(limit)
        elif task == ParseTask.PARSE_CATALOG:
            return await self._full_catalog_parse(limit, update_existing=True)
        else:
            raise ValueError(f"Unknown manual task: {task.value}")

    async def _create_parsing_session(self, mode: ParseMode, task: ParseTask) -> int:
        """Create new parsing session in database"""
        session = db_manager.get_session()
        try:
            db_session = ParsingSession(
                start_time=datetime.now(),
                parsing_type=f"{mode.value}_{task.value}",
                status="in_progress"
            )
            session.add(db_session)
            session.commit()
            session.refresh(db_session)

            logger.info(f"📝 Created parsing session #{db_session.id}")
            return db_session.id

        except Exception as e:
            session.rollback()
            logger.error(f"Failed to create parsing session: {e}")
            raise
        finally:
            session.close()

    async def _update_session_status(self, success: bool, error: str = None):
        """Update parsing session status"""
        if not self.session_id:
            return

        session = db_manager.get_session()
        try:
            db_session = session.query(ParsingSession).get(self.session_id)
            if db_session:
                db_session.end_time = datetime.now()
                db_session.status = "completed" if success else "failed"
                db_session.error_message = error
                session.commit()

                status = "✅" if success else "❌"
                logger.info(f"{status} Updated session #{self.session_id} status")

        except Exception as e:
            logger.error(f"Failed to update session status: {e}")
            session.rollback()
        finally:
            session.close()

    # =============================================================================
    # Рейтинг расчет
    # =============================================================================

    def _calculate_base_rating(self, kp: Optional[float], imdb: Optional[float],
                              lf: Optional[float], lf_votes: int) -> float:
        """
        Calculate base rating using weighted formula

        Formula: (kp * w_kp + imdb * w_imdb + lf * w_lf) / (w_kp + w_imdb + w_lf)
        where w_lf = wd_lf * lf_votes / (lf_votes + k)
        """
        from src.config.config import RATING_WEIGHTS

        # Get weights from config
        w_kp = RATING_WEIGHTS['kp']
        w_imdb = RATING_WEIGHTS['imdb']
        wd_lf = RATING_WEIGHTS['lf_base']
        k = RATING_WEIGHTS['smoothing_k']

        # Normalize ratings to 0-10 scale
        # Some sources might be on 10-point scale (e.g., 8.4/10)
        if kp and kp > 10:
            kp_norm = kp / 10
        else:
            kp_norm = kp or 0

        if imdb and imdb > 10:
            imdb_norm = imdb / 10
        else:
            imdb_norm = imdb or 0

        lf_norm = lf if lf else 0

        # Calculate dynamic LordFilm weight based on votes
        if lf_votes > 0:
            w_lf = wd_lf * lf_votes / (lf_votes + k)
        else:
            w_lf = 0
            lf_norm = 0

        # Calculate weighted average
        numerator = (kp_norm * w_kp) + (imdb_norm * w_imdb) + (lf_norm * w_lf)
        denominator = w_kp + w_imdb + w_lf

        if denominator == 0:
            return 0.0

        return numerator / denominator

    def _get_country_multiplier(self, country: Optional[str]) -> float:
        """Get multiplier based on country tier"""
        from src.config.config import COUNTRY_MULTIPLIERS, COUNTRY_LISTS

        if not country:
            return COUNTRY_MULTIPLIERS['level3']

        country_lower = country.strip().lower()

        # Check level1 countries
        for country_name in COUNTRY_LISTS['level1']:
            if country_name.lower() in country_lower:
                return COUNTRY_MULTIPLIERS['level1']

        # Check level2 countries
        for country_name in COUNTRY_LISTS['level2']:
            if country_name.lower() in country_lower:
                return COUNTRY_MULTIPLIERS['level2']

        # All others → level3
        return COUNTRY_MULTIPLIERS['level3']

    def _calculate_final_rating(self, film_data: Dict[str, Any]) -> float:
        """Calculate final rating with country multiplier"""
        # Get data
        kp = film_data.get('kp_rating')
        imdb = film_data.get('imdb_rating')
        lf = film_data.get('lf_rating')

        # Calculate total votes on LordFilm
        lf_likes = film_data.get('lf_likes', 0) or 0
        lf_dislikes = film_data.get('lf_dislikes', 0) or 0
        lf_votes = lf_likes + lf_dislikes

        country = film_data.get('country')

        # Calculate base rating
        base_rating = self._calculate_base_rating(kp, imdb, lf, lf_votes)

        # Apply country multiplier
        country_multiplier = self._get_country_multiplier(country)
        final_rating = base_rating * country_multiplier

        # Round to 2 decimal places
        return round(final_rating, 2)

    # =============================================================================
    # Основные задачи парсинга
    # =============================================================================

    async def _update_existing_ratings(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        Update ratings for existing films in database with batching

        Args:
            limit: Maximum number of films to update (None = all)
        """
        logger.info("🔄 Starting update_existing_ratings")

        # Получаем фильмы с пагинацией для экономии памяти
        session = db_manager.get_session()
        try:
            query = session.query(Film.url).filter(Film.is_active == True)

            if limit:
                query = query.limit(limit)

            total_films = query.count()
            logger.info(f"📊 Found {total_films} active films to update")

            # Батчинг для обработки больших объемов
            batch_size = min(FILMS_BATCH_SIZE, 1000)  # Не более 1000 за раз
            offset = 0

        finally:
            session.close()

        # Инициализация парсера
        self.film_parser = create_film_parser()

        stats = {
            "total_films": total_films,
            "films_processed": 0,
            "films_updated": 0,
            "errors": 0,
            "timeouts": 0
        }

        # Сохраняем частичную статистику для graceful shutdown
        self._partial_stats = stats

        # Обработка с пагинацией
        while offset < total_films and not self.should_stop:
            # Получаем батч URL
            session = db_manager.get_session()
            try:
                film_urls = session.query(Film.url)\
                    .filter(Film.is_active == True)\
                    .offset(offset)\
                    .limit(batch_size)\
                    .all()
                film_urls = [url for (url,) in film_urls]
            finally:
                session.close()

            # Обрабатываем батч
            batch_results = await self._process_film_batch(
                film_urls,
                update_existing=True,
                check_should_stop=True
            )

            # Обновляем статистику
            stats["films_processed"] += batch_results["processed"]
            stats["films_updated"] += batch_results["updated"]
            stats["errors"] += batch_results["errors"]
            stats["timeouts"] += batch_results.get("timeouts", 0)

            # Логируем прогресс
            processed = stats["films_processed"]
            progress_pct = (processed / total_films * 100) if total_films > 0 else 0
            logger.info(
                f"Progress: {processed}/{total_films} films "
                f"({progress_pct:.1f}%) - "
                f"Updated: {stats['films_updated']}, "
                f"Errors: {stats['errors']}"
            )

            offset += batch_size

            # Проверяем graceful shutdown
            if self.should_stop:
                logger.warning(f"🛑 Stopping early due to shutdown request. "
                             f"Processed {processed}/{total_films} films")
                break

            # Задержка между батчами
            if offset < total_films:
                await asyncio.sleep(BATCH_DELAY)

        logger.info(f"✅ Completed update_existing_ratings: {stats}")
        return stats

    async def _discover_new_films(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        Discover new films from catalog pages with pagination control
        """
        logger.info("🔍 Starting discover_new_films")

        # Получаем конфигурацию для daily задачи
        daily_config = PARSING_SCHEDULES['daily']

        # Инициализация парсеров
        self.link_parser = create_link_parser()
        self.film_parser = create_film_parser()

        stats = {
            "pages_processed": 0,
            "links_found": 0,
            "new_films_added": 0,
            "errors": 0,
            "timeouts": 0
        }

        self._partial_stats = stats

        # Обработка каждой задачи в daily конфигурации
        for task_config in daily_config['tasks']:
            if task_config['task_type'] != 'parse_pages' or self.should_stop:
                continue

            logger.info(f"Processing task: {task_config['name']}")

            # Генерация URL с учетом лимита
            urls_generator = self.url_generator.generate_from_template(
                template_key=task_config['url_template'],
                pages=task_config['pages'],
                years=task_config.get('years')
            )

            # Применяем лимит если указан
            urls = list(urls_generator)
            if limit and stats["pages_processed"] + len(urls) > limit:
                urls = urls[:limit - stats["pages_processed"]]

            # Обработка страниц каталога
            for page_url in urls:
                if self.should_stop:
                    break

                try:
                    # Парсинг ссылок со таймаутом
                    film_links = await asyncio.wait_for(
                        asyncio.get_event_loop().run_in_executor(
                            None,
                            lambda: self.link_parser.parse_links_from_page(page_url)
                        ),
                        timeout=SELENIUM_TIMEOUT
                    )

                    stats["links_found"] += len(film_links)
                    stats["pages_processed"] += 1

                    logger.debug(f"Found {len(film_links)} films on {page_url}")

                    # Обработка фильмов с ограничением concurrent задач
                    if film_links:
                        batch_stats = await self._process_film_links(
                            film_links,
                            update_existing=False,
                            check_should_stop=True
                        )
                        stats["new_films_added"] += batch_stats["new"]
                        stats["errors"] += batch_stats["errors"]
                        stats["timeouts"] += batch_stats.get("timeouts", 0)

                    # Задержка между страницами
                    await asyncio.sleep(BATCH_DELAY)

                except asyncio.TimeoutError:
                    logger.warning(f"⏱️ Timeout processing page: {page_url}")
                    stats["timeouts"] += 1
                    stats["errors"] += 1

                except Exception as e:
                    logger.error(f"Error processing page {page_url}: {e}")
                    stats["errors"] += 1

                # Логируем прогресс
                if stats["pages_processed"] % 10 == 0:
                    logger.info(
                        f"Progress: {stats['pages_processed']} pages, "
                        f"New films: {stats['new_films_added']}"
                    )

        logger.info(f"✅ Completed discover_new_films: {stats}")
        return stats

    async def _full_catalog_parse(self, limit: Optional[int] = None,
                                 update_existing: bool = False) -> Dict[str, Any]:
        """
        Full catalog parsing with comprehensive error handling
        """
        logger.info("🌐 Starting full_catalog_parse")

        # Получаем конфигурацию для weekly задачи
        weekly_config = PARSING_SCHEDULES['weekly']

        # Инициализация парсеров
        self.link_parser = create_link_parser()
        self.film_parser = create_film_parser()

        stats = {
            "pages_processed": 0,
            "links_found": 0,
            "films_processed": 0,
            "films_updated": 0,
            "new_films_added": 0,
            "errors": 0,
            "timeouts": 0
        }

        self._partial_stats = stats

        # Обработка каждой задачи в weekly конфигурации
        for task_config in weekly_config['tasks']:
            if task_config['task_type'] != 'parse_pages' or self.should_stop:
                continue

            logger.info(f"Processing task: {task_config['name']}")

            # Генерация URL с учетом лимита
            urls_generator = self.url_generator.generate_from_template(
                template_key=task_config['url_template'],
                pages=task_config['pages'],
                years=task_config.get('years')
            )

            # Применяем лимит если указан
            urls = list(urls_generator)
            if limit and stats["pages_processed"] + len(urls) > limit:
                urls = urls[:limit - stats["pages_processed"]]

            # Обработка страниц каталога
            for page_url in urls:
                if self.should_stop:
                    break

                try:
                    # Парсинг ссылок со таймаутом
                    film_links = await asyncio.wait_for(
                        asyncio.get_event_loop().run_in_executor(
                            None,
                            lambda: self.link_parser.parse_links_from_page(page_url)
                        ),
                        timeout=SELENIUM_TIMEOUT
                    )

                    stats["links_found"] += len(film_links)
                    stats["pages_processed"] += 1

                    logger.debug(f"Found {len(film_links)} films on {page_url}")

                    # Обработка фильмов
                    if film_links:
                        batch_stats = await self._process_film_links(
                            film_links,
                            update_existing=update_existing,
                            check_should_stop=True
                        )

                        stats["films_processed"] += batch_stats["processed"]
                        stats["films_updated"] += batch_stats["updated"]
                        stats["new_films_added"] += batch_stats["new"]
                        stats["errors"] += batch_stats["errors"]
                        stats["timeouts"] += batch_stats.get("timeouts", 0)

                    # Задержка между страницами
                    await asyncio.sleep(BATCH_DELAY)

                except asyncio.TimeoutError:
                    logger.warning(f"⏱️ Timeout processing page: {page_url}")
                    stats["timeouts"] += 1
                    stats["errors"] += 1

                except Exception as e:
                    logger.error(f"Error processing page {page_url}: {e}")
                    stats["errors"] += 1

                # Логируем прогресс
                if stats["pages_processed"] % 5 == 0:
                    logger.info(
                        f"Progress: {stats['pages_processed']} pages, "
                        f"Films: {stats['films_processed']}, "
                        f"New: {stats['new_films_added']}, "
                        f"Updated: {stats['films_updated']}"
                    )

        logger.info(f"✅ Completed full_catalog_parse: {stats}")
        return stats

    # =============================================================================
    # Вспомогательные методы
    # =============================================================================

    async def _process_film_links(self, film_links: List[str],
                                 update_existing: bool = False,
                                 check_should_stop: bool = False) -> Dict[str, int]:
        """
        Process a batch of film links with concurrent control and timeouts

        Args:
            film_links: List of film URLs
            update_existing: Whether to update existing films
            check_should_stop: Check for graceful shutdown flag

        Returns:
            Statistics about processed films
        """
        stats = {
            "processed": 0,
            "new": 0,
            "updated": 0,
            "errors": 0,
            "timeouts": 0
        }

        # Создаем задачи для concurrent обработки
        tasks = []
        for film_url in film_links:
            if check_should_stop and self.should_stop:
                break

            task = self._parse_single_film_with_semaphore(
                film_url,
                update_existing,
                check_should_stop
            )
            tasks.append(task)

        # Запускаем все задачи
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Агрегируем результаты
        for result in results:
            if isinstance(result, Exception):
                if isinstance(result, asyncio.TimeoutError):
                    stats["timeouts"] += 1
                stats["errors"] += 1
                continue

            stats["processed"] += result.get("processed", 0)
            stats["new"] += result.get("new", 0)
            stats["updated"] += result.get("updated", 0)
            stats["errors"] += result.get("errors", 0)
            stats["timeouts"] += result.get("timeouts", 0)

        return stats

    async def _parse_single_film_with_semaphore(self, film_url: str,
                                               update_existing: bool,
                                               check_should_stop: bool) -> Dict[str, int]:
        """
        Parse single film with semaphore control for concurrent limiting
        """
        async with self.semaphore:
            if check_should_stop and self.should_stop:
                return {"processed": 0, "errors": 1}

            return await self._parse_single_film(film_url, update_existing)

    async def _parse_single_film(self, film_url: str,
                                update_existing: bool) -> Dict[str, int]:
        """
        Parse single film with timeout and error handling
        """
        try:
            # Проверяем graceful shutdown
            if self.should_stop:
                raise GracefulShutdown()

            # Парсим детали фильма с таймаутом
            film_data = await asyncio.wait_for(
                asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: self.film_parser.parse_film_details(film_url)
                ),
                timeout=REQUEST_TIMEOUT
            )

            if "error" in film_data:
                logger.warning(f"Failed to parse {film_url}: {film_data['error']}")
                return {"processed": 0, "errors": 1}

            # Валидация данных
            if not await self._validate_film_data(film_data):
                logger.warning(f"Invalid data for film: {film_url}")
                return {"processed": 0, "errors": 1}

            # Сохраняем или обновляем фильм
            result_stats = await self._save_or_update_film(film_data, update_existing)

            # Задержка между запросами
            await asyncio.sleep(FILM_DELAY)

            return result_stats

        except asyncio.TimeoutError:
            logger.warning(f"⏱️ Timeout parsing film: {film_url}")
            return {"processed": 0, "errors": 1, "timeouts": 1}

        except GracefulShutdown:
            raise

        except Exception as e:
            logger.error(f"Error parsing film {film_url}: {e}")
            return {"processed": 0, "errors": 1}

    async def _validate_film_data(self, film_data: Dict[str, Any]) -> bool:
        """Validate parsed film data"""
        # Обязательные поля
        required_fields = ['url', 'title']
        for field in required_fields:
            if not film_data.get(field):
                return False

        # Валидация URL
        url = film_data['url']
        if not url.startswith('http'):
            return False

        # Валидация года (если есть)
        if film_data.get('year'):
            try:
                year = int(film_data['year'])
                current_year = datetime.now().year
                if not (1900 <= year <= current_year + 2):  # +2 для анонсов
                    return False
            except (ValueError, TypeError):
                return False

        # Валидация рейтинга (если есть)
        if film_data.get('lf_rating'):
            try:
                rating = float(film_data['lf_rating'])
                if not (0 <= rating <= 10):
                    return False
            except (ValueError, TypeError):
                return False

        return True

    async def _save_or_update_film(self, film_data: Dict[str, Any],
                                  update_existing: bool) -> Dict[str, int]:
        """Save or update film in database"""
        session = db_manager.get_session()
        try:
            # Проверяем существование фильма
            existing_film = session.query(Film).filter(Film.url == film_data['url']).first()

            if existing_film:
                if not update_existing:
                    return {"processed": 1, "updated": 0, "new": 0, "errors": 0}

                # Обновляем существующий фильм
                updated = await self._update_film_in_db(film_data)
                if updated:
                    return {"processed": 1, "updated": 1, "new": 0, "errors": 0}
                else:
                    return {"processed": 1, "updated": 0, "new": 0, "errors": 1}
            else:
                # Добавляем новый фильм
                added = await self._save_new_film_to_db(film_data)
                if added:
                    return {"processed": 1, "updated": 0, "new": 1, "errors": 0}
                else:
                    return {"processed": 1, "updated": 0, "new": 0, "errors": 1}

        except Exception as e:
            logger.error(f"Database error for film {film_data['url']}: {e}")
            return {"processed": 0, "updated": 0, "new": 0, "errors": 1}
        finally:
            session.close()

    async def _save_new_film_to_db(self, film_data: Dict[str, Any]) -> bool:
        """Save new film to database with final rating"""
        session = db_manager.get_session()
        try:
            # Двойная проверка на race condition
            existing = session.query(Film).filter(Film.url == film_data['url']).first()
            if existing:
                logger.debug(f"Film already exists (race condition): {film_data['url']}")
                return False

            # Рассчитываем итоговый рейтинг
            final_rating = self._calculate_final_rating(film_data)

            # Создаем новый фильм
            film = Film(
                url=film_data['url'],
                title=film_data.get('title'),
                original_title=film_data.get('original_title'),
                year=film_data.get('year'),
                country=film_data.get('country'),
                categories=film_data.get('categories', []),
                director=film_data.get('director'),
                actors=film_data.get('actors', []),
                description=film_data.get('description'),
                lf_rating=film_data.get('lf_rating'),
                lf_likes=film_data.get('lf_likes'),
                lf_dislikes=film_data.get('lf_dislikes'),
                kp_rating=film_data.get('kp_rating'),
                imdb_rating=film_data.get('imdb_rating'),
                final_rating=final_rating,                    # <-- ДОБАВЛЕНО
                rating_calculated_at=datetime.now(),          # <-- ДОБАВЛЕНО
                is_active=True,
                first_seen_at=datetime.now(),
                last_updated=datetime.now()
            )

            session.add(film)
            session.commit()

            # Записываем в историю парсинга
            if self.session_id:
                history = ParsingHistory(
                    film_id=film.id,
                    session_id=self.session_id,
                    parsing_type="film_details",
                    success=True,
                    data_processed={"title": film_data.get('title')}
                )
                session.add(history)
                session.commit()

            logger.debug(f"Added new film: {film_data.get('title', 'Unknown')} with rating {final_rating}")
            return True

        except Exception as e:
            session.rollback()
            logger.error(f"Failed to save film {film_data['url']}: {e}")
            return False
        finally:
            session.close()

    async def _update_film_in_db(self, film_data: Dict[str, Any]) -> bool:
        """Update existing film in database with recalculated rating"""
        session = db_manager.get_session()
        try:
            film = session.query(Film).filter(Film.url == film_data['url']).first()
            if not film:
                logger.warning(f"Film not found for update: {film_data['url']}")
                return False

            # Рассчитываем итоговый рейтинг
            final_rating = self._calculate_final_rating(film_data)

            # Обновляем поля
            update_fields = {
                'title': film_data.get('title'),
                'original_title': film_data.get('original_title'),
                'year': film_data.get('year'),
                'country': film_data.get('country'),
                'categories': film_data.get('categories', []),
                'director': film_data.get('director'),
                'actors': film_data.get('actors', []),
                'description': film_data.get('description'),
                'lf_rating': film_data.get('lf_rating'),
                'lf_likes': film_data.get('lf_likes'),
                'lf_dislikes': film_data.get('lf_dislikes'),
                'kp_rating': film_data.get('kp_rating'),
                'imdb_rating': film_data.get('imdb_rating'),
                'final_rating': final_rating,
                'rating_calculated_at': datetime.now(),
                'is_active': True,
                'last_updated': datetime.now()
            }

            for field, value in update_fields.items():
                if value is not None:
                    setattr(film, field, value)

            session.commit()

            # Записываем в историю
            if self.session_id:
                history = ParsingHistory(
                    film_id=film.id,
                    session_id=self.session_id,
                    parsing_type="film_details_update",
                    success=True,
                    data_processed={"title": film_data.get('title')}
                )
                session.add(history)
                session.commit()

            logger.debug(f"Updated film: {film_data.get('title', 'Unknown')} with new rating {final_rating}")
            return True

        except Exception as e:
            session.rollback()
            logger.error(f"Failed to update film {film_data['url']}: {e}")
            return False
        finally:
            session.close()

    async def _process_film_batch(self, film_urls: List[str],
                                 update_existing: bool,
                                 check_should_stop: bool) -> Dict[str, int]:
        """
        Process a batch of film URLs (helper for update_existing_ratings)
        """
        return await self._process_film_links(
            film_urls,
            update_existing=update_existing,
            check_should_stop=check_should_stop
        )

    async def _cleanup(self):
        """Cleanup resources"""
        try:
            if self.link_parser:
                self.link_parser.close()
            if self.film_parser:
                self.film_parser.close()
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")


# Global instance and async entry point
_main_parser_instance = None


def get_main_parser() -> MainParser:
    """Get singleton instance of MainParser"""
    global _main_parser_instance
    if _main_parser_instance is None:
        _main_parser_instance = MainParser()
    return _main_parser_instance


async def main(mode: ParseMode, task: ParseTask, **kwargs) -> Dict[str, Any]:
    """
    Async entry point for parser

    Example usage:
        result = await main(ParseMode.DAILY, ParseTask.DISCOVER_FILMS, limit=100)
    """
    parser = get_main_parser()
    return await parser.main(mode, task, **kwargs)
