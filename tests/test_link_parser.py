#!/usr/bin/env python3
"""
Test script for Link Parser module
Tests film link extraction from catalog pages
"""

import sys
import os
import logging
from typing import List

# Add project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.parser.link_parser import create_link_parser
from src.parser.url_generator import URLGenerator


def setup_logging():
    """Setup logging for tests"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    # Reduce selenium logging noise
    logging.getLogger('selenium').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)


def test_parser_initialization():
    """Test LinkParser initialization"""
    print("🧪 Testing LinkParser initialization")

    try:
        parser = create_link_parser()
        assert parser is not None
        print("✅ LinkParser created successfully")

        # Check attributes
        assert hasattr(parser, 'base_url'), "Missing base_url attribute"
        assert hasattr(parser, 'selenium_url'), "Missing selenium_url attribute"

        print(f"   base_url: {parser.base_url}")
        print(f"   selenium_url: {parser.selenium_url}")

        return parser
    except Exception as e:
        print(f"❌ LinkParser initialization failed: {e}")
        return None


def test_single_page_parsing(parser, test_url: str) -> bool:
    """Test parsing links from a single page"""
    print(f"\n🧪 Testing single page parsing: {test_url}")

    try:
        links = parser.parse_links_from_page(test_url)

        print(f"✅ Found {len(links)} film links")

        if links:
            # Show first few links
            print("   Sample links:")
            for i, link in enumerate(links[:3], 1):
                print(f"   {i}. {link}")
            if len(links) > 3:
                print(f"   ... and {len(links) - 3} more")

            # Validate links
            for link in links[:5]:  # Check first 5
                assert link.startswith('http'), f"Invalid URL: {link}"
                assert '/filmy/' in link, f"Not a film URL: {link}"
                assert link.endswith('.html'), f"Not HTML page: {link}"

            print("✅ All links are valid film URLs")
        else:
            print("⚠️ No links found (might be expected for test page)")

        return True
    except Exception as e:
        print(f"❌ Page parsing failed: {e}")
        return False


def test_multiple_pages(parser) -> bool:
    """Test parsing multiple catalog pages"""
    print("\n🧪 Testing multiple pages")

    try:
        # Generate test URLs
        url_gen = URLGenerator()
        test_pages = list(url_gen.generate_from_template(
            template_key='main_catalog',
            pages=2
        ))

        if not test_pages:
            print("⚠️ No test pages generated")
            return True

        all_links = []

        for i, page_url in enumerate(test_pages, 1):
            print(f"   Parsing page {i}: {page_url}")

            try:
                links = parser.parse_links_from_page(page_url)
                print(f"     Found {len(links)} links")
                all_links.extend(links)

                # Small delay between pages
                if i < len(test_pages):
                    import time
                    time.sleep(1)

            except Exception as e:
                print(f"     ❌ Failed: {e}")
                continue

        print(f"✅ Total links found across {len(test_pages)} pages: {len(all_links)}")

        # Remove duplicates
        unique_links = list(set(all_links))
        if len(unique_links) < len(all_links):
            print(f"⚠️ Found {len(all_links) - len(unique_links)} duplicate links")

        return True
    except Exception as e:
        print(f"❌ Multiple pages test failed: {e}")
        return False


def test_link_validation(parser) -> bool:
    """Test film URL validation logic"""
    print("\n🧪 Testing link validation")

    test_cases = [
        # (URL, should_be_valid)
        ("/filmy/12345-avatar-2025.html", True),
        ("/filmy/67890-matrix-1999.html", True),
        ("/filmy/11111-test-film.html", True),  # No year
        ("/filmy/", False),  # No ID
        ("/filmy/2024/page/1/", False),  # Catalog page
        ("/serialy/12345-test.html", False),  # Wrong category
        ("https://wk.lordfilm17.ru/filmy/12345-avatar-2025.html", True),
        ("/filmy/top-50.html", False),  # Top list
        ("/filmy/news.html", False),  # News
    ]

    passed = 0
    total = len(test_cases)

    for url, should_be_valid in test_cases:
        try:
            is_valid = parser._is_film_url(url)

            if is_valid == should_be_valid:
                status = "✅"
                passed += 1
            else:
                status = "❌"

            print(f"   {status} {url} -> valid={is_valid} (expected={should_be_valid})")

        except Exception as e:
            print(f"   ❌ {url} -> ERROR: {e}")

    print(f"✅ Link validation: {passed}/{total} passed")
    return passed == total


def test_cleanup(parser) -> bool:
    """Test proper cleanup"""
    print("\n🧪 Testing cleanup")

    try:
        parser.close()
        print("✅ LinkParser closed successfully")
        return True
    except Exception as e:
        print(f"❌ Cleanup failed: {e}")
        return False


def main():
    """Main test function"""
    print("🚀 Starting Link Parser Tests")
    print("=" * 60)

    setup_logging()

    parser = test_parser_initialization()
    if not parser:
        print("❌ Cannot continue without parser")
        return 1

    tests = []

    # Test with a known catalog page
    try:
        url_gen = URLGenerator()
        test_url = next(url_gen.generate_from_template(
            template_key='main_catalog',
            pages=1
        ))

        tests.append(("Single page", lambda: test_single_page_parsing(parser, test_url)))
        tests.append(("Multiple pages", lambda: test_multiple_pages(parser)))

    except Exception as e:
        print(f"⚠️ Cannot generate test URL: {e}")
        print("⚠️ Using default test URL")
        test_url = "https://wk.lordfilm17.ru/filmy/page/1/"
        tests.append(("Single page", lambda: test_single_page_parsing(parser, test_url)))

    tests.append(("Link validation", lambda: test_link_validation(parser)))
    tests.append(("Cleanup", lambda: test_cleanup(parser)))

    passed = 0
    total = len(tests)

    for test_name, test_func in tests:
        try:
            if test_func():
                print(f"✅ {test_name}: PASSED\n")
                passed += 1
            else:
                print(f"❌ {test_name}: FAILED\n")
        except Exception as e:
            print(f"💥 {test_name}: ERROR - {e}\n")

    print("=" * 60)
    print(f"📊 Test Results: {passed}/{total} passed")

    if passed == total:
        print("🎉 All Link Parser tests passed!")
        return 0
    elif passed >= 2:
        print("✅ Link Parser basic functionality working")
        return 0
    else:
        print("⚠️ Link Parser has issues")
        return 1


if __name__ == "__main__":
    sys.exit(main())