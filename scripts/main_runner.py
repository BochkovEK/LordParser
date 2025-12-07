#!/usr/bin/env python3
"""
LordFilm Parser - Main Runner Script
Command line interface between systemd and the main parser.
Auto-detects daily/weekly mode based on day of week.
"""

import asyncio
import sys
import os
import argparse
import logging
from typing import Optional, List, Tuple
from datetime import datetime
from enum import Enum

# Add project root to PYTHONPATH
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Project imports
try:
    from src.parser.main_parser import (
        main as parser_main,
        ParseMode,
        ParseTask
    )
    from src.config.config import LOGGING_CONFIG, DEFAULT_URL, SELENIUM_URL
except ImportError as e:
    print(f"❌ Import error: {e}")
    print(f"📁 Project root: {project_root}")
    sys.exit(2)  # Configuration error code

# Configure logging
logging.basicConfig(
    level=LOGGING_CONFIG.get('level', 'INFO'),
    format=LOGGING_CONFIG.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
    handlers=[
        logging.StreamHandler(sys.stdout),  # for journalctl
        logging.StreamHandler(sys.stderr)  # for errors
    ]
)

logger = logging.getLogger(__name__)


class ExitCodes:
    """Exit codes for systemd"""
    SUCCESS = 0
    GENERAL_ERROR = 1
    CONFIG_ERROR = 2
    DB_ERROR = 3
    PARSER_ERROR = 4
    USER_INTERRUPT = 130


class RunnerMode(Enum):
    """Runner operation modes"""
    AUTO = "auto"  # Auto-detect based on day of week
    DAILY = "daily"  # Force daily mode
    WEEKLY = "weekly"  # Force weekly mode


async def _check_database() -> bool:
    """Check PostgreSQL database connectivity"""
    try:
        from src.database.connection import db_manager
        session = db_manager.get_session()
        session.execute("SELECT 1")
        session.close()
        logger.info("✅ Database: OK")
        return True
    except Exception as e:
        logger.error(f"❌ Database: {e}")
        return False


async def _check_website_via_selenium() -> bool:
    """Check website accessibility via Selenium container"""
    try:
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC

        # Remote Selenium container
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')

        driver = webdriver.Remote(
            command_executor=SELENIUM_URL,
            options=options
        )

        try:
            # Check website availability
            driver.get(DEFAULT_URL)

            # Wait for page load
            wait = WebDriverWait(driver, 10)
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))

            # Verify site responds
            title = driver.title
            if title and len(title) > 0:
                logger.info(f"✅ Website via Selenium: OK, title: '{title[:50]}...'")
                return True
            else:
                logger.error("❌ Website: loaded but no title")
                return False

        finally:
            driver.quit()

    except ImportError:
        logger.info("ℹ️ Selenium: not installed, skipping")
        return True  # Skip if Selenium not available
    except Exception as e:
        logger.error(f"❌ Website via Selenium: {e}")
        return False


async def perform_health_checks(force: bool = False) -> bool:
    """
    Perform all health checks before execution

    Args:
        force: If True, continue even if checks fail

    Returns:
        True if all checks pass or force=True
    """
    logger.info("🔍 Running health checks...")

    checks_passed = 0
    total_checks = 2  # DB + website via Selenium

    # 1. Database check
    db_ok = await _check_database()
    checks_passed += 1 if db_ok else 0

    # 2. Website check via Selenium
    selenium_ok = await _check_website_via_selenium()
    checks_passed += 1 if selenium_ok else 0

    all_ok = checks_passed == total_checks

    if all_ok:
        logger.info("✅ All health checks passed")
        return True
    else:
        logger.warning(f"⚠️ Health checks: {checks_passed}/{total_checks} passed")

        if not force:
            logger.error("🛑 Health checks failed. Use --force to override.")
            return False
        else:
            logger.warning("⚠️ Force mode: Continuing despite health check failures")
            return True


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='LordFilm Parser - CLI interface for parsing tasks',
        epilog='Examples:\n'
               '  python main_runner.py --auto    # Auto-detect daily/weekly\n'
               '  python main_runner.py --mode daily --force\n'
               '  python main_runner.py --mode manual --task update_ratings --limit 100'
    )

    # Mode selection (mutually exclusive group)
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument(
        '--auto',
        action='store_true',
        help='Auto-detect mode based on day of week (for systemd timer)'
    )
    mode_group.add_argument(
        '--mode',
        type=str,
        choices=['daily', 'weekly', 'manual'],
        help='Force specific mode (daily, weekly, or manual)'
    )

    # Task selection (for manual mode)
    parser.add_argument(
        '--task',
        type=str,
        choices=['update_ratings', 'discover_films', 'parse_catalog'],
        help='Specific task to execute (required for manual mode)'
    )

    # Control parameters
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Limit number of films/pages to parse (for testing)'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Check configuration without actual parsing'
    )

    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Verbose logging (DEBUG level)'
    )

    parser.add_argument(
        '--force',
        action='store_true',
        help='Force execution (ignore locks, continue on errors)'
    )

    return parser.parse_args()


def auto_detect_mode() -> ParseMode:
    """
    Auto-detect parsing mode based on current day of week.

    Returns:
        ParseMode.WEEKLY on Sundays, ParseMode.DAILY otherwise
    """
    today = datetime.now()
    weekday = today.weekday()  # 0=Monday, 6=Sunday

    if weekday == 6:  # Sunday
        logger.info("📅 Today is Sunday - running WEEKLY full parse")
        return ParseMode.WEEKLY
    else:
        logger.info(
            f"📅 Today is {['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'][weekday]} - running DAILY tasks"
        )
        return ParseMode.DAILY


def get_tasks_for_mode(mode: ParseMode, manual_task: Optional[str] = None) -> List[Tuple[ParseMode, ParseTask]]:
    """
    Get list of tasks to execute based on mode.

    Args:
        mode: Parsing mode
        manual_task: Specific task for manual mode

    Returns:
        List of (mode, task) tuples to execute sequentially

    Raises:
        ValueError: If manual mode without task or invalid configuration
    """
    if mode == ParseMode.MANUAL:
        if not manual_task:
            raise ValueError("Manual mode requires --task argument")

        try:
            task = ParseTask(manual_task)
        except ValueError:
            valid_tasks = [t.value for t in ParseTask]
            raise ValueError(f"Invalid task '{manual_task}'. Valid tasks: {valid_tasks}")

        return [(ParseMode.MANUAL, task)]

    elif mode == ParseMode.DAILY:
        # Daily: execute both tasks sequentially
        return [
            (ParseMode.DAILY, ParseTask.UPDATE_RATINGS),
            (ParseMode.DAILY, ParseTask.DISCOVER_FILMS)
        ]

    elif mode == ParseMode.WEEKLY:
        # Weekly: only full catalog parse
        return [(ParseMode.WEEKLY, ParseTask.FULL_PARSE)]

    else:
        raise ValueError(f"Unknown mode: {mode}")


async def run_single_task(mode: ParseMode, task: ParseTask,
                          limit: Optional[int] = None,
                          dry_run: bool = False,
                          force: bool = False) -> Tuple[int, dict]:
    """
    Execute a single parsing task.

    Args:
        mode: Parsing mode
        task: Specific task to execute
        limit: Limit on number of items to process
        dry_run: Configuration check only
        force: Force execution despite warnings

    Returns:
        Tuple of (exit_code, result_dict)
    """
    task_start = datetime.now()
    logger.info(f"▶️ Starting task: {task.value} (mode: {mode.value})")

    try:
        # Call main parser
        result = await parser_main(
            mode=mode,
            task=task,
            limit=limit,
            dry_run=dry_run,
            force=force
        )

        duration = (datetime.now() - task_start).total_seconds()

        if result.get('success', False):
            stats = result.get('statistics', {})
            logger.info(f"✅ Task {task.value} completed in {duration:.2f}s")

            if stats:
                logger.debug(f"📊 Task statistics: {stats}")

            return ExitCodes.SUCCESS, result
        else:
            error_msg = result.get('error', 'Unknown error')
            logger.error(f"❌ Task {task.value} failed: {error_msg}")

            # Determine error type for exit code
            error_lower = error_msg.lower()
            if 'database' in error_lower or 'db' in error_lower or 'postgres' in error_lower:
                return ExitCodes.DB_ERROR, result
            elif 'parser' in error_lower or 'parse' in error_lower or 'selenium' in error_lower:
                return ExitCodes.PARSER_ERROR, result
            else:
                return ExitCodes.GENERAL_ERROR, result

    except Exception as e:
        logger.error(f"💥 Critical error in task {task.value}: {e}", exc_info=True)
        return ExitCodes.GENERAL_ERROR, {"error": str(e), "success": False}


async def run_all_tasks(tasks_config: List[Tuple[ParseMode, ParseTask]],
                        limit: Optional[int] = None,
                        dry_run: bool = False,
                        verbose: bool = False,
                        force: bool = False) -> int:
    """
    Execute all configured parsing tasks sequentially.

    Args:
        tasks_config: List of (mode, task) tuples to execute
        limit: Limit on number of items to process
        dry_run: Configuration check only
        verbose: Enable debug logging
        force: Continue execution despite errors

    Returns:
        Exit code for systemd
    """
    start_time = datetime.now()

    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("🔍 Verbose mode enabled (DEBUG logging)")

    if dry_run:
        logger.info("✅ Dry run: Configuration check passed")
        return ExitCodes.SUCCESS

    # Perform health checks before execution
    if not await perform_health_checks(force):
        return ExitCodes.CONFIG_ERROR

    logger.info(f"🚀 Starting parsing pipeline")
    logger.info(f"📋 Tasks scheduled: {len(tasks_config)}")

    # Log each scheduled task
    for i, (mode, task) in enumerate(tasks_config, 1):
        logger.info(f"  {i}. {task.value} ({mode.value})")

    # Execution statistics
    stats = {
        "total_tasks": len(tasks_config),
        "completed_tasks": 0,
        "failed_tasks": 0,
        "total_time": 0.0,
        "errors": []
    }

    # Execute tasks sequentially
    for i, (task_mode, task) in enumerate(tasks_config, 1):
        logger.info(f"--- Task {i}/{len(tasks_config)}: {task.value} ---")

        exit_code, result = await run_single_task(
            mode=task_mode,
            task=task,
            limit=limit,
            dry_run=dry_run,
            force=force
        )

        if exit_code == ExitCodes.SUCCESS:
            stats["completed_tasks"] += 1
        else:
            stats["failed_tasks"] += 1
            stats["errors"].append({
                "task": task.value,
                "error": result.get("error", "Unknown error"),
                "exit_code": exit_code
            })

            # Stop execution on critical errors unless forced
            if not force and exit_code in [ExitCodes.DB_ERROR, ExitCodes.CONFIG_ERROR]:
                logger.error(f"🛑 Critical error detected. Stopping execution.")
                break

    # Calculate total execution time
    stats["total_time"] = (datetime.now() - start_time).total_seconds()

    # Log final statistics
    logger.info(f"📈 Execution summary:")
    logger.info(f"   Total tasks: {stats['total_tasks']}")
    logger.info(f"   Completed: {stats['completed_tasks']}")
    logger.info(f"   Failed: {stats['failed_tasks']}")
    logger.info(f"   Total time: {stats['total_time']:.2f}s")

    # Determine final exit code
    if stats["failed_tasks"] == 0:
        logger.info(f"🎉 All tasks completed successfully")
        return ExitCodes.SUCCESS
    elif stats["completed_tasks"] > 0 and stats["failed_tasks"] > 0:
        logger.warning(f"⚠️ Partial success: {stats['completed_tasks']}/{stats['total_tasks']} tasks completed")
        for error in stats["errors"]:
            logger.warning(f"   Failed: {error['task']} - {error['error']}")
        return ExitCodes.PARSER_ERROR  # Partial failure
    else:
        logger.error(f"💥 All tasks failed")
        for error in stats["errors"]:
            logger.error(f"   Failed: {error['task']} - {error['error']}")
        return ExitCodes.GENERAL_ERROR


def main() -> int:
    """
    Main entry point for the runner.

    Returns:
        Exit code for systemd
    """
    try:
        # Parse command line arguments
        args = parse_arguments()

        # Determine running mode
        if args.auto:
            mode = auto_detect_mode()
            logger.info(f"🔄 Auto-detected mode: {mode.value}")
        else:
            try:
                mode = ParseMode(args.mode)
            except ValueError:
                logger.error(f"❌ Invalid mode: {args.mode}")
                return ExitCodes.CONFIG_ERROR

        # Get list of tasks to execute
        try:
            tasks_to_run = get_tasks_for_mode(mode, args.task)
        except ValueError as e:
            logger.error(f"❌ Task configuration error: {e}")
            return ExitCodes.CONFIG_ERROR

        # Execute all tasks
        exit_code = asyncio.run(
            run_all_tasks(
                tasks_config=tasks_to_run,
                limit=args.limit,
                dry_run=args.dry_run,
                verbose=args.verbose,
                force=args.force
            )
        )

        return exit_code

    except KeyboardInterrupt:
        logger.info("⚠️ Received interrupt signal. Shutting down...")
        return ExitCodes.USER_INTERRUPT

    except Exception as e:
        logger.error(f"💥 Unhandled error in main runner: {e}", exc_info=True)
        return ExitCodes.GENERAL_ERROR


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)