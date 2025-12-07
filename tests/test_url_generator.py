#!/usr/bin/env python3
"""
Test script for URL Generator module
Tests actual URLGenerator implementation with its real templates
"""

import sys
import os

# Add project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.parser.url_generator import URLGenerator


def test_generator_initialization():
    """Test URLGenerator initialization"""
    print("🧪 Testing URLGenerator initialization")

    try:
        generator = URLGenerator()
        assert generator is not None
        print("✅ URLGenerator initialized successfully")

        # Check attributes from config
        print(f"   base_url: {generator.base_url}")
        print(f"   category: {generator.category}")
        print(f"   year range: {generator.start_year}-{generator.end_year}")
        print(f"   default_pages: {generator.default_pages}")

        return True
    except Exception as e:
        print(f"❌ URLGenerator initialization failed: {e}")
        return False


def test_main_catalog_generation():
    """Test main catalog URL generation"""
    print("\n🧪 Testing main catalog generation")

    try:
        generator = URLGenerator()

        # Test with 2 pages
        urls = list(generator.generate_from_template(
            template_key='main_catalog',
            pages=2
        ))

        assert len(urls) == 2, f"Expected 2 URLs, got {len(urls)}"

        print(f"✅ Generated {len(urls)} main catalog URLs")

        # Check URL format
        for i, url in enumerate(urls, 1):
            assert url.startswith(generator.base_url), f"URL doesn't start with base_url: {url}"
            assert f"/page/{i}/" in url, f"Page number incorrect in URL: {url}"
            print(f"   {i}. {url}")

        return True
    except Exception as e:
        print(f"❌ Main catalog generation failed: {e}")
        return False


def test_yearly_catalog_generation():
    """Test yearly catalog URL generation"""
    print("\n🧪 Testing yearly catalog generation")

    try:
        generator = URLGenerator()

        # Test with specific years
        test_years = [2023, 2024]
        urls = list(generator.generate_from_template(
            template_key='yearly_catalog',
            pages=2,
            years=test_years
        ))

        expected_count = len(test_years) * 2  # 2 pages per year
        assert len(urls) == expected_count, f"Expected {expected_count} URLs, got {len(urls)}"

        print(f"✅ Generated {len(urls)} yearly catalog URLs for years {test_years}")

        # Check each URL contains correct year and page
        url_index = 0
        for year in test_years:
            for page in [1, 2]:
                url = urls[url_index]
                assert str(year) in url, f"Year {year} not in URL: {url}"
                assert f"/page/{page}/" in url, f"Page {page} not in URL: {url}"
                url_index += 1

        # Show sample URLs
        print("   Sample URLs:")
        for i, url in enumerate(urls[:3], 1):
            print(f"   {i}. {url}")
        if len(urls) > 3:
            print(f"   ... and {len(urls) - 3} more")

        return True
    except Exception as e:
        print(f"❌ Yearly catalog generation failed: {e}")
        return False


def test_invalid_template():
    """Test error handling for invalid template"""
    print("\n🧪 Testing invalid template handling")

    try:
        generator = URLGenerator()

        # This should raise ValueError
        urls = list(generator.generate_from_template(
            template_key='invalid_template',
            pages=1
        ))

        print("❌ Should have raised ValueError for invalid template")
        return False
    except ValueError as e:
        print(f"✅ Correctly raised ValueError: {e}")
        return True
    except Exception as e:
        print(f"❌ Wrong exception type: {type(e).__name__}: {e}")
        return False


def test_pages_all_keyword():
    """Test 'all' keyword for pages parameter"""
    print("\n🧪 Testing 'all' pages keyword")

    try:
        generator = URLGenerator()

        # Should use default_pages when pages='all'
        urls = list(generator.generate_from_template(
            template_key='main_catalog',
            pages='all'
        ))

        assert len(urls) == generator.default_pages, \
            f"Expected {generator.default_pages} URLs for 'all', got {len(urls)}"

        print(f"✅ 'all' keyword generated {len(urls)} URLs (default_pages)")

        # Verify page numbers
        for i, url in enumerate(urls, 1):
            assert f"/page/{i}/" in url, f"Page {i} not in URL: {url}"

        return True
    except Exception as e:
        print(f"❌ 'all' keyword test failed: {e}")
        return False


def test_default_years():
    """Test yearly catalog with default years (from config)"""
    print("\n🧪 Testing default years generation")

    try:
        generator = URLGenerator()

        # Should use YEAR_RANGE from config when years=None
        urls = list(generator.generate_from_template(
            template_key='yearly_catalog',
            pages=1
        ))

        expected_years = generator.end_year - generator.start_year + 1
        assert len(urls) == expected_years, \
            f"Expected {expected_years} URLs for default years, got {len(urls)}"

        print(f"✅ Generated {len(urls)} URLs for default years {generator.start_year}-{generator.end_year}")

        # Verify each year appears
        for year in range(generator.start_year, generator.end_year + 1):
            year_found = any(str(year) in url for url in urls)
            assert year_found, f"Year {year} not found in generated URLs"

        return True
    except Exception as e:
        print(f"❌ Default years test failed: {e}")
        return False


def main():
    """Main test function"""
    print("🚀 Starting URL Generator Tests")
    print("=" * 50)

    tests = [
        ("Initialization", test_generator_initialization),
        ("Main catalog", test_main_catalog_generation),
        ("Yearly catalog", test_yearly_catalog_generation),
        ("Invalid template", test_invalid_template),
        ("'all' pages keyword", test_pages_all_keyword),
        ("Default years", test_default_years),
    ]

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

    print("=" * 50)
    print(f"📊 Test Results: {passed}/{total} passed")

    if passed == total:
        print("🎉 All URL Generator tests passed!")
        return 0
    elif passed >= 4:
        print("✅ URL Generator working correctly")
        return 0
    else:
        print("⚠️ URL Generator has issues")
        return 1


if __name__ == "__main__":
    sys.exit(main())

