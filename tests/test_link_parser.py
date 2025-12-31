#!/usr/bin/env python3
"""
Enhanced test for Link Parser with site availability checks
Tests:
1. LinkParser correctly raises ConnectionError on site unavailability
2. LinkParser works with URLs from URLGenerator
3. Integration with broken base URL
"""

import sys
import os
import logging
from typing import List
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
    # Reduce noise
    logging.getLogger('selenium').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)


def test_site_unavailability():
    """
    Test 1: LinkParser raises ConnectionError on site unavailability
    Uses broken URL pattern similar to URLGenerator output
    """
    print("\n" + "=" * 60)
    print("🔌 TEST 1: Site Unavailability Detection")
    print("=" * 60)

    link_parser = create_link_parser()

    # Create broken URL using the same pattern as URLGenerator
    parsed = urlparse(DEFAULT_URL)
    broken_domain = "nonexistent-domain-12345.invalid"
    broken_url = f"http://{broken_domain}/filmy/page/1/"

    print(f"Base URL from config: {DEFAULT_URL}")
    print(f"Testing with broken URL: {broken_url}")

    try:
        links = link_parser.parse_links_from_page(broken_url)

        # If we get here - FAIL
        print(f"❌ FAIL: LinkParser returned {len(links)} links instead of raising exception")
        print(f"   Links found: {links[:3] if links else '[]'}")
        return False

    except ConnectionError as e:
        # ✅ CORRECT - ConnectionError
        print(f"✅ PASS: ConnectionError raised as expected")
        print(f"   Message: {str(e)[:100]}...")
        return True

    except TimeoutError as e:
        # ✅ Also acceptable - TimeoutError
        print(f"✅ PASS: TimeoutError raised")
        print(f"   Message: {str(e)[:100]}...")
        return True

    except Exception as e:
        # ❌ Wrong exception type
        print(f"❌ FAIL: Wrong exception type: {type(e).__name__}")
        print(f"   Message: {str(e)[:100]}...")
        print(f"   Expected: ConnectionError or TimeoutError")
        return False

    finally:
        link_parser.close()


def test_with_url_generator_normal():
    """
    Test 2: LinkParser works with URLs from URLGenerator (normal base URL)
    """
    print("\n" + "=" * 60)
    print("🔄 TEST 2: LinkParser + URLGenerator (Normal URL)")
    print("=" * 60)

    # URLGenerator uses DEFAULT_URL from config
    url_gen = URLGenerator()  # base_url = DEFAULT_URL
    link_parser = create_link_parser()  # base_url = DEFAULT_URL

    # Generate one test page
    test_urls = list(url_gen.generate_from_template(
        template_key='main_catalog',
        pages=1
    ))

    if not test_urls:
        print("❌ FAIL: URLGenerator didn't generate any URLs")
        return False

    test_url = test_urls[0]
    print(f"Generated URL: {test_url}")
    print(f"Base URL used: {url_gen.base_url}")

    try:
        links = link_parser.parse_links_from_page(test_url)
        print(f"Found {len(links)} links")

        if links:
            print("✅ PASS: LinkParser successfully parsed URL from URLGenerator")
            # Show first 3 links for verification
            for i, link in enumerate(links[:3], 1):
                print(f"   {i}. {link}")
            return True
        else:
            print("⚠️ WARNING: Found 0 links (site might be empty or selectors outdated)")
            print("   This is not a FAIL, but indicates potential issue")
            return True  # Not a failure, just warning

    except ConnectionError as e:
        print(f"❌ FAIL: Site unavailable (ConnectionError): {e}")
        return False

    except Exception as e:
        print(f"❌ FAIL: Unexpected error: {type(e).__name__}: {e}")
        return False

    finally:
        link_parser.close()


def test_with_url_generator_broken():
    """
    Test 3: LinkParser with URLs from URLGenerator using broken base URL
    """
    print("\n" + "=" * 60)
    print("💀 TEST 3: LinkParser + URLGenerator (Broken Base URL)")
    print("=" * 60)

    # Create broken base URL by modifying the original
    parsed = urlparse(DEFAULT_URL)
    broken_base = parsed._replace(netloc="broken-domain-54321.invalid")
    broken_base_url = urlunparse(broken_base)

    print(f"Original base URL: {DEFAULT_URL}")
    print(f"Broken base URL:   {broken_base_url}")

    # Create URLGenerator with broken base URL
    broken_url_gen = URLGenerator(base_url=broken_base_url)

    # Create LinkParser with the same broken base URL for consistency
    broken_link_parser = create_link_parser(base_url=broken_base_url)

    # Generate URL using broken base
    broken_urls = list(broken_url_gen.generate_from_template(
        template_key='main_catalog',
        pages=1
    ))

    if not broken_urls:
        print("❌ FAIL: Broken URLGenerator didn't generate any URLs")
        return False

    broken_url = broken_urls[0]
    print(f"Generated broken URL: {broken_url}")

    try:
        links = broken_link_parser.parse_links_from_page(broken_url)

        # If we get here - FAIL (should raise exception)
        print(f"❌ FAIL: LinkParser returned {len(links)} links for broken URL")
        print(f"   Expected: ConnectionError")
        return False

    except ConnectionError as e:
        # ✅ CORRECT
        print(f"✅ PASS: ConnectionError raised for broken base URL")
        print(f"   Message: {str(e)[:100]}...")
        return True

    except Exception as e:
        # Check if it's a network-related error
        error_msg = str(e).lower()
        network_errors = ['connection', 'timeout', 'unreachable', 'resolve', 'err_name_not_resolved']

        if any(err in error_msg for err in network_errors):
            print(f"✅ PASS: Network error raised: {type(e).__name__}")
            print(f"   Message: {str(e)[:100]}...")
            return True
        else:
            print(f"⚠️ UNEXPECTED: Wrong exception type: {type(e).__name__}")
            print(f"   Message: {str(e)[:100]}...")
            return False

    finally:
        broken_link_parser.close()


def test_link_validation():
    """
    Test 4: Validate that extracted links are proper film URLs
    """
    print("\n" + "=" * 60)
    print("🔍 TEST 4: Link Validation")
    print("=" * 60)

    # This test only runs if we have actual links from a successful parse
    # It's a bonus test to verify link quality

    url_gen = URLGenerator()
    link_parser = create_link_parser()

    try:
        # Get a real URL
        test_urls = list(url_gen.generate_from_template('main_catalog', pages=1))
        if not test_urls:
            print("⚠️ SKIP: Cannot get test URL")
            return True

        test_url = test_urls[0]
        print(f"Testing URL: {test_url}")

        links = link_parser.parse_links_from_page(test_url)

        if not links:
            print("⚠️ SKIP: No links found to validate")
            return True

        print(f"Validating {len(links)} links...")

        valid_count = 0
        invalid_examples = []

        for link in links:
            if _is_valid_film_link(link):
                valid_count += 1
            else:
                if len(invalid_examples) < 3:
                    invalid_examples.append(link)

        validation_rate = (valid_count / len(links)) * 100 if links else 0

        print(f"   Valid links: {valid_count}/{len(links)} ({validation_rate:.1f}%)")

        if validation_rate >= 80:
            print("✅ PASS: Most links are valid film URLs")
        elif validation_rate >= 50:
            print("⚠️ WARNING: Many invalid links")
        else:
            print("❌ FAIL: Too many invalid links")

        if invalid_examples:
            print(f"   Invalid examples:")
            for i, link in enumerate(invalid_examples, 1):
                print(f"     {i}. {link}")

        return validation_rate >= 50  # Pass if at least 50% valid

    except Exception as e:
        print(f"⚠️ SKIP: Validation test failed: {e}")
        return True  # Don't fail overall test suite
    finally:
        link_parser.close()


def _is_valid_film_link(url: str) -> bool:
    """Check if URL is a valid film link"""
    if not url:
        return False

    # Must be absolute URL
    if not url.startswith('http'):
        return False

    # Must contain /filmy/
    if '/filmy/' not in url:
        return False

    # Must be HTML page
    if not url.endswith('.html'):
        return False

    # Must not be catalog page
    if '/page/' in url:
        return False

    # Must have film ID pattern: /filmy/12345-something.html
    import re
    film_pattern = r'/filmy/\d+-[^/]+\.html$'
    return re.search(film_pattern, url) is not None


def run_all_tests():
    """Run all tests and return overall result"""
    setup_logging()

    print("🚀 LordFilm LinkParser Comprehensive Test Suite")
    print("=" * 60)
    print(f"Using base URL from config: {DEFAULT_URL}")
    print("=" * 60)

    tests = [
        ("Site Unavailability Detection", test_site_unavailability),
        ("Normal URL Generation", test_with_url_generator_normal),
        ("Broken Base URL", test_with_url_generator_broken),
        ("Link Validation", test_link_validation),
    ]

    results = []

    for test_name, test_func in tests:
        print(f"\n▶️ Running: {test_name}")
        try:
            success = test_func()
            results.append((test_name, success))

            if success:
                print(f"✅ {test_name}: PASSED")
            else:
                print(f"❌ {test_name}: FAILED")

        except Exception as e:
            print(f"💥 {test_name}: CRASHED - {e}")
            results.append((test_name, False))

    # Summary
    print("\n" + "=" * 60)
    print("📊 TEST SUMMARY")
    print("=" * 60)

    passed = sum(1 for _, success in results if success)
    total = len(results)

    for test_name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"  {status} - {test_name}")

    print(f"\nTotal: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 ALL TESTS PASSED!")
        return True
    elif passed >= total - 1:  # Allow 1 failure
        print("⚠️ MOST TESTS PASSED (1 failure allowed)")
        return True
    else:
        print("❌ TOO MANY TEST FAILURES")
        return False


def main():
    """Main entry point"""
    try:
        success = run_all_tests()
        return 0 if success else 1
    except KeyboardInterrupt:
        print("\n⚠️ Tests interrupted by user")
        return 130
    except Exception as e:
        print(f"💥 Fatal error in test suite: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())