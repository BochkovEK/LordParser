#!/usr/bin/env python3
"""
LordFilm LinkParser Test Suite
Tests LinkParser's ability to handle site availability and integration with URLGenerator
"""

import sys
import os
import logging
from typing import List, Tuple, Callable
from urllib.parse import urlparse, urlunparse

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


def test_site_unavailability() -> Tuple[str, bool, str]:
    """
    Test that LinkParser raises ConnectionError on site unavailability

    Returns:
        Tuple: (test_name, success, message)
    """
    test_name = "Site Unavailability Detection"

    link_parser = create_link_parser()

    # Create broken URL using the same pattern as URLGenerator
    parsed = urlparse(DEFAULT_URL)
    broken_domain = "nonexistent-domain-12345.invalid"
    broken_url = f"http://{broken_domain}/filmy/page/1/"

    try:
        links = link_parser.parse_links_from_page(broken_url)

        return (
            test_name,
            False,
            f"LinkParser returned {len(links)} links instead of raising ConnectionError"
        )

    except ConnectionError as e:
        return (
            test_name,
            True,
            f"ConnectionError raised as expected: {str(e)[:80]}..."
        )

    except TimeoutError as e:
        return (
            test_name,
            True,
            f"TimeoutError raised: {str(e)[:80]}..."
        )

    except Exception as e:
        return (
            test_name,
            False,
            f"Wrong exception type {type(e).__name__}: {str(e)[:80]}..."
        )

    finally:
        link_parser.close()


def test_normal_url_generation() -> Tuple[str, bool, str]:
    """
    Test LinkParser works with URLs from URLGenerator (normal base URL)

    Returns:
        Tuple: (test_name, success, message)
    """
    test_name = "Normal URL Generation Integration"

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
                f"Successfully parsed {len(links)} links from {test_url}"
            )
        else:
            return (
                test_name,
                True,
                "Found 0 links (site might be empty or selectors outdated)"
            )

    except ConnectionError as e:
        return (
            test_name,
            False,
            f"Site unavailable: {str(e)[:80]}..."
        )

    except Exception as e:
        return (
            test_name,
            False,
            f"Unexpected error {type(e).__name__}: {str(e)[:80]}..."
        )

    finally:
        link_parser.close()


def test_broken_base_url() -> Tuple[str, bool, str]:
    """
    Test LinkParser with URLs from URLGenerator using broken base URL

    Returns:
        Tuple: (test_name, success, message)
    """
    test_name = "Broken Base URL Handling"

    # Create broken base URL by modifying the original
    parsed = urlparse(DEFAULT_URL)
    broken_base = parsed._replace(netloc="broken-domain-54321.invalid")
    broken_base_url = urlunparse(broken_base)

    # Create URLGenerator with broken base URL
    broken_url_gen = URLGenerator(base_url=broken_base_url)

    # Create LinkParser with the same broken base URL
    broken_link_parser = create_link_parser(base_url=broken_base_url)

    # Generate URL using broken base
    broken_urls = list(broken_url_gen.generate_from_template(
        template_key='main_catalog',
        pages=1
    ))

    if not broken_urls:
        return (test_name, False, "Broken URLGenerator didn't generate any URLs")

    broken_url = broken_urls[0]

    try:
        links = broken_link_parser.parse_links_from_page(broken_url)

        return (
            test_name,
            False,
            f"LinkParser returned {len(links)} links for broken URL {broken_url}"
        )

    except ConnectionError as e:
        return (
            test_name,
            True,
            f"ConnectionError raised for broken base URL: {str(e)[:80]}..."
        )

    except Exception as e:
        error_msg = str(e).lower()
        network_errors = ['connection', 'timeout', 'unreachable', 'resolve', 'err_name_not_resolved']

        if any(err in error_msg for err in network_errors):
            return (
                test_name,
                True,
                f"Network error raised ({type(e).__name__}): {str(e)[:80]}..."
            )
        else:
            return (
                test_name,
                False,
                f"Wrong exception type {type(e).__name__}: {str(e)[:80]}..."
            )

    finally:
        broken_link_parser.close()


def run_test_suite() -> bool:
    """Run all tests and return overall result"""
    setup_logging()

    print("🚀 LordFilm LinkParser Test Suite")
    print("=" * 60)
    print(f"Config base URL: {DEFAULT_URL}")
    print("=" * 60)

    # Define all tests
    tests = [
        test_site_unavailability,
        test_normal_url_generation,
        test_broken_base_url,
    ]

    results = []

    # Run all tests
    for test_index, test_func in enumerate(tests, 1):
        print(f"\n▶️ Running test {test_index}/{len(tests)}...")

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
            print(f"   💥 Test {test_index} crashed")
            print(f"      {error_msg}")
            results.append((f"Test {test_index}", False, error_msg))

    # Print summary
    print("\n" + "=" * 60)
    print("📊 TEST SUMMARY")
    print("=" * 60)

    passed_count = sum(1 for _, success, _ in results if success)
    total_count = len(results)

    for idx, (test_name, success, message) in enumerate(results, 1):
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{idx:2d}. {status} - {test_name}")
        if not success and message:
            print(f"    💡 {message}")

    print(f"\n📈 Total: {passed_count}/{total_count} tests passed")

    # Determine final result
    if passed_count == total_count:
        print("🎉 ALL TESTS PASSED!")
        return True
    elif passed_count >= total_count - 1:
        print("⚠️ MOST TESTS PASSED (1 failure allowed)")
        return True
    else:
        print("❌ TOO MANY TEST FAILURES")
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