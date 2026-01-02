#!/usr/bin/env python3
"""
LordFilm LinkParser Test Suite
Comprehensive tests for LinkParser functionality
"""

import sys
import os
import logging
import re
from typing import List, Tuple
from urllib.parse import urlparse

# Add project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.parser.link_parser import create_link_parser
from src.parser.url_generator import URLGenerator
from src.config.config import DEFAULT_URL


def setup_logging():
    """Setup logging for tests"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logging.getLogger('selenium').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)


def test_site_availability() -> Tuple[str, bool, str]:
    """
    URLGenerator + LinkParser integration with real site
    Should find film links on catalog page
    """
    test_name = "Site Availability & Integration"

    url_gen = URLGenerator()
    link_parser = create_link_parser()

    # Generate one test page
    test_urls = list(url_gen.generate_from_template(
        template_key='main_catalog',
        pages=1
    ))

    if not test_urls:
        return (test_name, False, "URLGenerator didn't generate any URLs")

    test_url = test_urls[0]

    try:
        links = link_parser.parse_links_from_page(test_url)

        if links:
            return (
                test_name,
                True,
                f"Found {len(links)} links on catalog page {test_url}"
            )
        else:
            return (
                test_name,
                False,
                f"No links found on catalog page {test_url}. Check selectors or site structure."
            )

    except ConnectionError as e:
        return (
            test_name,
            False,
            f"Site unavailable (ConnectionError): {str(e)[:100]}..."
        )

    except Exception as e:
        return (
            test_name,
            False,
            f"Unexpected error {type(e).__name__}: {str(e)[:100]}..."
        )

    finally:
        link_parser.close()


def test_site_unavailability() -> Tuple[str, bool, str]:
    """
    LinkParser with broken URL should raise ConnectionError after retries
    """
    test_name = "Site Unavailability Handling"

    link_parser = create_link_parser()

    # Create intentionally broken URL
    broken_domain = "nonexistent-domain-" + str(hash("test"))[:8] + ".invalid"
    broken_url = f"http://{broken_domain}/filmy/page/1/"

    try:
        links = link_parser.parse_links_from_page(broken_url)

        return (
            test_name,
            False,
            f"LinkParser returned {len(links)} links for broken URL (should raise ConnectionError)"
        )

    except ConnectionError as e:
        error_msg = str(e)
        if any(word in error_msg.lower() for word in ['attempt', 'retry', 'try']):
            return (
                test_name,
                True,
                f"ConnectionError raised after retries: {error_msg[:120]}..."
            )
        else:
            return (
                test_name,
                True,
                f"ConnectionError raised: {error_msg[:120]}..."
            )

    except TimeoutError as e:
        return (
            test_name,
            True,
            f"TimeoutError raised: {str(e)[:120]}..."
        )

    except Exception as e:
        error_msg = str(e).lower()
        if any(err in error_msg for err in ['webdriver', 'connection', 'timeout', 'unreachable']):
            return (
                test_name,
                True,
                f"Network error raised ({type(e).__name__}): {str(e)[:120]}..."
            )
        else:
            return (
                test_name,
                False,
                f"Wrong exception type {type(e).__name__}: {str(e)[:120]}..."
            )

    finally:
        link_parser.close()


def test_link_validation() -> Tuple[str, bool, str]:
    """
    Validate that extracted links match film URL patterns
    Uses same validation logic as link_parser._is_film_url()
    """
    test_name = "Link Format Validation"

    url_gen = URLGenerator()
    link_parser = create_link_parser()

    # Generate one test page
    test_urls = list(url_gen.generate_from_template(
        template_key='main_catalog',
        pages=1
    ))

    if not test_urls:
        return (test_name, False, "Cannot get test URL from URLGenerator")

    test_url = test_urls[0]

    try:
        links = link_parser.parse_links_from_page(test_url)

        if not links:
            return (
                test_name,
                False,
                f"No links found to validate on {test_url}"
            )

        # Same validation logic as in link_parser._is_film_url()
        def is_valid_film_url(url: str) -> bool:
            if not url:
                return False

            if '/filmy/' not in url:
                return False

            exclude_patterns = [
                r'/filmy/$',
                r'/filmy/[^/]+/$',
                r'/filmy/\?',
                r'/filmy/\d{4}/page/',
                r'top-50', 'news', 'netflix', 'marvel', 'serialy', 'multfilmy'
            ]

            if any(re.search(pattern, url) for pattern in exclude_patterns):
                return False

            film_pattern = r'/filmy/\d+-[^/]+-\d{4}\.html$'
            film_pattern_alt = r'/filmy/\d+-[^/]+\.html$'

            is_valid_film = (
                    re.search(film_pattern, url) is not None or
                    re.search(film_pattern_alt, url) is not None
            )

            has_id = re.search(r'/filmy/(\d+)', url) is not None

            return is_valid_film and has_id

        valid_count = 0
        invalid_examples = []

        for link in links:
            if is_valid_film_url(link):
                valid_count += 1
            else:
                if len(invalid_examples) < 3:
                    invalid_examples.append(os.path.basename(link))

        total_links = len(links)
        valid_percentage = (valid_count / total_links) * 100 if total_links > 0 else 0

        if valid_percentage >= 90:
            return (
                test_name,
                True,
                f"Excellent: {valid_count}/{total_links} valid film links ({valid_percentage:.1f}%)"
            )
        elif valid_percentage >= 70:
            return (
                test_name,
                True,
                f"Good: {valid_count}/{total_links} valid film links ({valid_percentage:.1f}%)"
            )
        elif valid_percentage >= 50:
            return (
                test_name,
                False,
                f"Poor: Only {valid_count}/{total_links} valid film links ({valid_percentage:.1f}%)"
            )
        else:
            invalid_info = f" Invalid examples: {invalid_examples}" if invalid_examples else ""
            return (
                test_name,
                False,
                f"Bad: Only {valid_count}/{total_links} valid film links ({valid_percentage:.1f}%){invalid_info}"
            )

    except ConnectionError as e:
        return (
            test_name,
            False,
            f"Site unavailable: {str(e)[:100]}..."
        )

    except Exception as e:
        return (
            test_name,
            False,
            f"Unexpected error {type(e).__name__}: {str(e)[:100]}..."
        )

    finally:
        link_parser.close()


def run_test_suite() -> bool:
    """Run all tests and return overall result"""
    setup_logging()

    print("🚀 LordFilm LinkParser Test Suite")
    print("=" * 60)
    print(f"Testing with base URL: {DEFAULT_URL}")

    # Define all tests with their display names
    tests = [
        ("Site Availability & Integration", test_site_availability),
        ("Site Unavailability Handling", test_site_unavailability),
        ("Link Format Validation", test_link_validation),
    ]

    test_names = [name for name, _ in tests]
    print(f"Tests: {', '.join(test_names)}")
    print("=" * 60)

    results = []

    # Run all tests
    for test_display_name, test_func in tests:
        print(f"\n▶️ Running: {test_display_name}...")

        try:
            test_name, success, message = test_func()

            if success:
                print(f"   ✅ {message}")
            else:
                print(f"   ❌ {message}")

            results.append((test_display_name, success, message))

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

        # Show brief message for failures
        if not success and message:
            # Extract the main part of the message (before first colon if exists)
            if ":" in message:
                brief_msg = message.split(":", 1)[1].strip()[:60]
            else:
                brief_msg = message[:60]

            if brief_msg:
                print(f"    {brief_msg}...")

    print(f"\n📈 Total: {passed_count}/{total_count} tests passed")

    # Determine final result
    if passed_count == total_count:
        print("\n🎉 ALL TESTS PASSED! LinkParser is working correctly.")
        return True
    elif passed_count >= total_count - 1:
        print("\n⚠️ ACCEPTABLE: Most tests passed. Review any failures.")
        return True
    else:
        print("\n❌ TOO MANY FAILURES: LinkParser needs fixes.")
        return False


def main():
    """Main entry point"""
    try:
        success = run_test_suite()
        return 0 if success else 1
    except KeyboardInterrupt:
        print("\n⚠️ Tests interrupted by user")
        return 130
    except Exception as e:
        print(f"💥 Fatal error in test suite: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
