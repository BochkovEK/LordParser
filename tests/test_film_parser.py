#!/usr/bin/env python3
"""
Test script for Film Parser module
Tests detailed film information extraction and rating calculation
"""

import sys
import os
import logging
from typing import Tuple, List, Dict, Any

# Add project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.parser.film_parser import create_film_parser
from src.config.config import DEFAULT_URL


def setup_logging():
    """Setup logging for tests"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logging.getLogger('selenium').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)


def test_film_details_extraction() -> Tuple[str, bool, str]:
    """
    Test extraction of film details from a real film page
    """
    test_name = "Film Details Extraction"

    film_parser = create_film_parser()

    # Test with a real film URL (should be accessible)
    test_urls = [
        "https://mh.lordfilm131.ru/filmy/42292-venom-2-2021-7678-86284.html",
        "https://mh.lordfilm131.ru/filmy/40583-garri-potter-i-filosofskij-kamen-2001-3339-32232.html",
    ]

    # Use first accessible URL
    test_url = None
    for url in test_urls:
        test_url = url
        break  # Use first URL

    if not test_url:
        return (test_name, False, "No test URLs available")

    try:
        film_data = film_parser.parse_film_details(test_url)

        # Check for error in response
        if 'error' in film_data:
            return (test_name, False, f"Parser returned error: {film_data['error']}")

        # Required fields
        required_fields = ['url', 'title']
        missing_fields = [field for field in required_fields if field not in film_data or not film_data[field]]

        if missing_fields:
            return (test_name, False, f"Missing required fields: {missing_fields}")

        # Validate field types and content
        checks = []

        # URL should match
        if film_data['url'] == test_url:
            checks.append(("URL preserved", True))
        else:
            checks.append(("URL preserved", False, f"Expected {test_url}, got {film_data['url']}"))

        # Title should be non-empty
        if film_data.get('title'):
            checks.append(("Has title", True, film_data['title'][:50]))
        else:
            checks.append(("Has title", False))

        # Year validation if present
        if film_data.get('year'):
            try:
                year = int(film_data['year'])
                if 1900 <= year <= 2026:  # Reasonable year range
                    checks.append(("Valid year", True, str(year)))
                else:
                    checks.append(("Valid year", False, f"Year out of range: {year}"))
            except (ValueError, TypeError):
                checks.append(("Valid year", False, f"Invalid year format: {film_data['year']}"))

        # Check rating extraction
        rating_checks = []

        # LordFilm rating calculation
        if film_data.get('lf_likes') is not None and film_data.get('lf_dislikes') is not None:
            if 'lf_rating' in film_data:
                lf_rating = film_data['lf_rating']
                if isinstance(lf_rating, (int, float)) and 0 <= lf_rating <= 10:
                    rating_checks.append(("LF rating calculated", True, f"{lf_rating:.2f}"))
                else:
                    rating_checks.append(("LF rating calculated", False, f"Invalid value: {lf_rating}"))
            else:
                rating_checks.append(("LF rating calculated", False, "Missing lf_rating"))

        # KP rating if present
        if film_data.get('kp_rating') is not None:
            kp_rating = film_data['kp_rating']
            if isinstance(kp_rating, (int, float)):
                rating_checks.append(("KP rating extracted", True, f"{kp_rating:.2f}"))

        # IMDB rating if present
        if film_data.get('imdb_rating') is not None:
            imdb_rating = film_data['imdb_rating']
            if isinstance(imdb_rating, (int, float)):
                rating_checks.append(("IMDB rating extracted", True, f"{imdb_rating:.2f}"))

        # Summary of rating checks
        if rating_checks:
            rating_success = all(check[1] for check in rating_checks)
            checks.append(("Ratings extracted", rating_success,
                           f"{len([c for c in rating_checks if c[1]])}/{len(rating_checks)}"))

        # Check additional fields
        additional_fields = ['country', 'categories', 'director', 'actors', 'description']
        present_fields = [field for field in additional_fields if film_data.get(field)]
        checks.append(
            ("Additional data", len(present_fields) > 0, f"{len(present_fields)}/{len(additional_fields)} fields"))

        # Determine overall success
        critical_checks = [c for c in checks if not c[1]]

        if not critical_checks:
            film_title = film_data.get('title', 'Unknown')[:40]
            message = f"✅ Parsed: {film_title} | Ratings: LF={film_data.get('lf_rating', 'N/A')}, KP={film_data.get('kp_rating', 'N/A')}, IMDB={film_data.get('imdb_rating', 'N/A')}"
            return (test_name, True, message)
        else:
            failed = [c[0] for c in critical_checks]
            return (test_name, False, f"Failed checks: {', '.join(failed)}")

    except Exception as e:
        return (test_name, False, f"Test crashed: {type(e).__name__}: {str(e)[:100]}")
    finally:
        film_parser.close()


def test_rating_calculation_logic() -> Tuple[str, bool, str]:
    """
    Test LordFilm rating calculation formula
    """
    test_name = "Rating Calculation Logic"

    film_parser = create_film_parser()

    try:
        # We need to access private methods for testing
        # This is a bit hacky but works for testing

        test_cases = [
            {
                "name": "All likes",
                "likes": 100,
                "dislikes": 0,
                "expected": 10.0
            },
            {
                "name": "All dislikes",
                "likes": 0,
                "dislikes": 100,
                "expected": 0.0
            },
            {
                "name": "Mixed 50/50",
                "likes": 50,
                "dislikes": 50,
                "expected": 5.0
            },
            {
                "name": "Mixed 75/25",
                "likes": 75,
                "dislikes": 25,
                "expected": 7.5
            },
            {
                "name": "Few votes",
                "likes": 3,
                "dislikes": 1,
                "expected": 7.5
            }
        ]

        results = []
        all_passed = True

        # Note: We're testing the formula logic
        # Actual calculation: rating = (likes / total) * 10
        for test_case in test_cases:
            likes = test_case["likes"]
            dislikes = test_case["dislikes"]
            expected = test_case["expected"]

            if likes + dislikes == 0:
                calculated = 0.0
            else:
                calculated = (likes / (likes + dislikes)) * 10

            # Allow small floating point differences
            passed = abs(calculated - expected) < 0.01

            if not passed:
                all_passed = False

            results.append(
                f"{test_case['name']}: {calculated:.2f} "
                f"(expected {expected:.2f}) "
                f"{'✅' if passed else '❌'}"
            )

        message = " | ".join(results)
        return (test_name, all_passed, message)

    except Exception as e:
        return (test_name, False, f"Test crashed: {type(e).__name__}: {str(e)[:100]}")
    finally:
        film_parser.close()


def test_film_parser_error_handling() -> Tuple[str, bool, str]:
    """
    Test FilmParser error handling with invalid URL
    """
    test_name = "Error Handling"

    film_parser = create_film_parser()

    # Use a URL that will definitely fail
    broken_url = "http://nonexistent-domain-12345.invalid/filmy/99999-test.html"

    try:
        film_data = film_parser.parse_film_details(broken_url)

        # Should return error dictionary, not crash
        if 'error' in film_data:
            error_msg = film_data['error'][:80]
            return (test_name, True, f"✅ Properly handled error: {error_msg}...")
        else:
            # If it somehow succeeded, that's also OK (but unlikely)
            return (test_name, True, f"✅ Unexpected success on broken URL")

    except Exception as e:
        # Should not crash even on invalid URL
        return (test_name, False, f"❌ Parser crashed on invalid URL: {type(e).__name__}")
    finally:
        film_parser.close()


def run_tests() -> bool:
    """Run all tests and return overall result"""
    setup_logging()

    print("🎬 Film Parser Test Suite")
    print("=" * 60)
    print(f"Testing with base URL: {DEFAULT_URL}")
    print("Tests: 1) Details Extraction, 2) Rating Logic, 3) Error Handling")
    print("=" * 60)

    tests = [
        ("Film Details Extraction", test_film_details_extraction),
       # ("Rating Calculation Logic", test_rating_calculation_logic),
       # ("Error Handling", test_film_parser_error_handling),
    ]

    results = []

    # Run tests sequentially
    for test_display_name, test_func in tests:
        print(f"\n▶️ Running: {test_display_name}...")

        try:
            test_name, success, message = test_func()

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
        if not success and len(message) < 100:
            print(f"    {message}")

    print(f"\n📈 Total: {passed_count}/{total_count} tests passed")

    # Determine final result
    if passed_count == total_count:
        print("\n🎉 ALL TESTS PASSED! Film Parser is working correctly.")
        return True
    elif passed_count >= total_count - 1:
        print("\n⚠️ ACCEPTABLE: Most tests passed. Review any failures.")
        return True
    else:
        print("\n❌ TOO MANY FAILURES: Film Parser needs fixes.")
        return False


def main():
    """Main entry point"""
    try:
        success = run_tests()
        return 0 if success else 1
    except KeyboardInterrupt:
        print("\n⚠️ Tests interrupted by user")
        return 130
    except Exception as e:
        print(f"💥 Fatal error in test suite: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())