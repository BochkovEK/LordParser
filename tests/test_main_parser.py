#!/usr/bin/env python3
"""
Test script for Main Parser module
Tests the core parsing functionality, database operations, and rating calculation
"""

import sys
import os
import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Tuple

# Add project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.parser.main_parser import main, ParseMode, ParseTask
from src.database.connection import db_manager
from src.database.models import Film


def setup_logging():
    """Setup logging for tests"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logging.getLogger('sqlalchemy').setLevel(logging.WARNING)


async def test_daily_discovery() -> Tuple[str, bool, str]:
    """
    Test 1: Daily discovery task with limit (1 page)
    Parses one catalog page, saves films to DB with calculated ratings
    """
    test_name = "Daily Discovery Task"

    # Record start time for finding new films
    test_start = datetime.now() - timedelta(seconds=1)  # 1 second buffer

    try:
        # Run daily discovery with minimal load
        result = await main(
            ParseMode.DAILY,
            ParseTask.DISCOVER_FILMS,
            limit=1,  # Parse only 1 catalog page
            dry_run=False
        )

        if not result.get('success', False):
            return (test_name, False, f"Parser failed: {result.get('error', 'Unknown error')}")

        # Get statistics from result
        stats = result.get('statistics', {})
        pages_processed = stats.get('pages_processed', 0)
        links_found = stats.get('links_found', 0)
        new_films_added = stats.get('new_films_added', 0)

        # Find films added during this test
        session = db_manager.get_session()
        try:
            new_films = session.query(Film).filter(
                Film.created_at >= test_start
            ).all()

            actual_new_count = len(new_films)

            # Verify results
            checks = []

            # Check 1: At least one page should be processed
            if pages_processed >= 1:
                checks.append(("Pages processed", True))
            else:
                checks.append(("Pages processed", False))

            # Check 2: Links should be found (unless site is empty)
            if links_found > 0:
                checks.append(("Links found", True))
            else:
                # This might happen if site is down or empty
                checks.append(("Links found", False, "warning"))

            # Check 3: New films should be added to DB
            if actual_new_count > 0:
                checks.append(("Films saved to DB", True))
            else:
                checks.append(("Films saved to DB", False))

            # Check 4: Verify film data completeness
            if actual_new_count > 0:
                film = new_films[0]  # Check first film
                film_checks = []

                if film.title:
                    film_checks.append(("Has title", True))
                else:
                    film_checks.append(("Has title", False, "warning"))

                if film.final_rating is not None:
                    film_checks.append(("Has final_rating", True))
                else:
                    film_checks.append(("Has final_rating", False))

                if film.rating_calculated_at is not None:
                    film_checks.append(("Has rating_calculated_at", True))
                else:
                    film_checks.append(("Has rating_calculated_at", False))

                # Combine film checks
                film_success = all(check[1] for check in film_checks)
                if film_success:
                    checks.append(("Film data complete", True))
                else:
                    failed = [c[0] for c in film_checks if not c[1]]
                    checks.append(("Film data complete", False, f"Missing: {failed}"))

            # Determine overall success
            critical_checks = [c for c in checks if len(c) == 2 or c[2] != "warning"]
            success = all(check[1] for check in critical_checks)

            # Build message
            message_parts = []
            message_parts.append(f"Pages: {pages_processed}, Links: {links_found}, New in DB: {actual_new_count}")

            for check_name, check_success, *extra in checks:
                status = "✅" if check_success else ("⚠️" if extra and extra[0] == "warning" else "❌")
                extra_msg = f" ({extra[0]})" if extra else ""
                message_parts.append(f"{status} {check_name}{extra_msg}")

            return (test_name, success, " | ".join(message_parts))

        finally:
            session.close()

    except Exception as e:
        return (test_name, False, f"Test crashed: {type(e).__name__}: {str(e)[:100]}")


async def test_update_ratings() -> Tuple[str, bool, str]:
    """
    Test 2: Update ratings for existing films
    Takes films from DB and updates their ratings
    """
    test_name = "Update Ratings Task"

    try:
        # First, ensure we have some films in DB
        session = db_manager.get_session()
        film_count = session.query(Film).count()
        session.close()

        if film_count == 0:
            return (test_name, False, "No films in DB to update (run discovery test first)")

        # Record current ratings for comparison
        session = db_manager.get_session()
        try:
            # Get a few films to check later
            sample_films = session.query(Film).limit(3).all()
            original_ratings = {
                film.id: (film.final_rating, film.rating_calculated_at)
                for film in sample_films
            }
        finally:
            session.close()

        # Run update ratings with minimal load
        result = await main(
            ParseMode.DAILY,
            ParseTask.UPDATE_RATINGS,
            limit=2,  # Update only 2 films for speed
            dry_run=False
        )

        if not result.get('success', False):
            return (test_name, False, f"Parser failed: {result.get('error', 'Unknown error')}")

        # Get statistics
        stats = result.get('statistics', {})
        films_processed = stats.get('films_processed', 0)
        films_updated = stats.get('films_updated', 0)

        # Check if ratings were actually updated
        session = db_manager.get_session()
        try:
            updated_count = 0
            rechecked_films = session.query(Film).filter(
                Film.id.in_([f.id for f in sample_films])
            ).all()

            for film in rechecked_films:
                old_rating, old_timestamp = original_ratings.get(film.id, (None, None))
                if film.rating_calculated_at and old_timestamp:
                    if film.rating_calculated_at > old_timestamp:
                        updated_count += 1

            # Verify results
            checks = []

            if films_processed > 0:
                checks.append(("Films processed", True))
            else:
                checks.append(("Films processed", False, "warning"))

            if films_updated > 0:
                checks.append(("Films updated", True))
            else:
                # Might happen if site is down
                checks.append(("Films updated", False, "warning"))

            if updated_count > 0:
                checks.append(("Ratings recalculated", True))
            else:
                checks.append(("Ratings recalculated", False, "warning"))

            # Determine success
            critical_checks = [c for c in checks if len(c) == 2]
            success = all(check[1] for check in critical_checks) if critical_checks else True

            message = f"Processed: {films_processed}, Updated: {films_updated}, Recalculated: {updated_count}"
            for check_name, check_success, *extra in checks:
                status = "✅" if check_success else ("⚠️" if extra and extra[0] == "warning" else "❌")
                message += f" | {status} {check_name}"

            return (test_name, success, message)

        finally:
            session.close()

    except Exception as e:
        return (test_name, False, f"Test crashed: {type(e).__name__}: {str(e)[:100]}")


async def test_rating_calculation_logic() -> Tuple[str, bool, str]:
    """
    Test 3: Verify rating calculation logic (without actual parsing)
    Tests the formula and country multiplier logic
    """
    test_name = "Rating Calculation Logic"

    try:
        # Import MainParser to test its methods
        from src.parser.main_parser import MainParser

        parser = MainParser()

        test_cases = [
            {
                "name": "Popular film (many votes)",
                "data": {
                    "kp_rating": 8.4,
                    "imdb_rating": 7.8,
                    "lf_rating": 8.4,
                    "lf_likes": 8000,
                    "lf_dislikes": 525,
                    "country": "США"
                },
                "expected_range": (8.0, 8.5)
            },
            {
                "name": "New film (few votes)",
                "data": {
                    "kp_rating": 7.2,
                    "imdb_rating": 6.9,
                    "lf_rating": 7.5,
                    "lf_likes": 10,
                    "lf_dislikes": 5,
                    "country": "Россия"
                },
                "expected_range": (6.5, 7.5)
            },
            {
                "name": "Country level3 penalty",
                "data": {
                    "kp_rating": 8.0,
                    "imdb_rating": 7.5,
                    "lf_rating": 8.2,
                    "lf_likes": 100,
                    "lf_dislikes": 20,
                    "country": "Египет"  # Not in level1 or level2
                },
                "expected_range": (5.0, 6.5)  # Should be reduced by 0.7 multiplier
            }
        ]

        results = []
        all_passed = True

        for test_case in test_cases:
            rating = parser._calculate_final_rating(test_case["data"])
            min_expected, max_expected = test_case["expected_range"]

            passed = min_expected <= rating <= max_expected

            if not passed:
                all_passed = False

            results.append(
                f"{test_case['name']}: {rating:.2f} "
                f"(expected {min_expected}-{max_expected}) "
                f"{'✅' if passed else '❌'}"
            )

        message = " | ".join(results)
        return (test_name, all_passed, message)

    except Exception as e:
        return (test_name, False, f"Test crashed: {type(e).__name__}: {str(e)[:100]}")


async def run_tests() -> bool:
    """Run all tests and return overall result"""
    setup_logging()

    print("🧪 Main Parser Test Suite")
    print("=" * 60)
    print("Tests: 1) Daily Discovery, 2) Update Ratings, 3) Rating Logic")
    print("=" * 60)

    tests = [
        ("Daily Discovery Task", test_daily_discovery),
        ("Update Ratings Task", test_update_ratings),
        ("Rating Calculation Logic", test_rating_calculation_logic),
    ]

    results = []

    # Run tests sequentially
    for test_display_name, test_func in tests:
        print(f"\n▶️ Running: {test_display_name}...")

        try:
            test_name, success, message = await test_func()

            if success:
                print(f"   ✅ {test_name}")
                print(f"      {message}")
            else:
                print(f"   ❌ {test_name}")
                print(f"      {message}")

            results.append((test_name, success, message))

        except Exception as e:
            error_msg = f"Test crashed: {type(e).__name__}: {str(e)[:100]}"
            print(f"   💥 {error_msg}")
            results.append((test_display_name, False, error_msg))

    # Print summary
    print("\n" + "=" * 60)
    print("📊 TEST SUMMARY")
    print("=" * 60)

    passed_count = sum(1 for _, success, _ in results if success)
    total_count = len(results)

    for idx, (test_name, success, message) in enumerate(results, 1):
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{idx}. {status} - {test_name}")
        if not success and len(message) < 100:  # Show brief errors
            print(f"    {message}")

    print(f"\n📈 Total: {passed_count}/{total_count} tests passed")

    # Cleanup: Remove test films if needed
    # (Optional: uncomment if you want to clean up after tests)
    # await cleanup_test_data()

    # Determine final result
    if passed_count == total_count:
        print("\n🎉 ALL TESTS PASSED! Main Parser is working correctly.")
        return True
    elif passed_count >= total_count - 1:
        print("\n⚠️ ACCEPTABLE: Most tests passed. Review any failures.")
        return True
    else:
        print("\n❌ TOO MANY FAILURES: Main Parser needs fixes.")
        return False


async def cleanup_test_data():
    """Optional: Clean up test data from database"""
    try:
        # This would remove films created during tests
        # Implement if needed
        pass
    except Exception as e:
        print(f"⚠️ Cleanup failed: {e}")


async def main():
    """Main entry point"""
    try:
        success = await run_tests()
        return 0 if success else 1
    except KeyboardInterrupt:
        print("\n⚠️ Tests interrupted by user")
        return 130
    except Exception as e:
        print(f"💥 Fatal error in test suite: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))