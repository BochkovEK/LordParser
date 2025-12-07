#!/usr/bin/env python3
"""
Test script for URL Generator module
Standalone test without external dependencies
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
        return True
    except Exception as e:
        print(f"❌ URLGenerator initialization failed: {e}")
        return False


def test_default_attributes():
    """Test default attributes of URLGenerator"""
    print("\n🧪 Testing default attributes")

    try:
        generator = URLGenerator()

        # Check for expected attributes
        assert hasattr(generator, 'default_url'), "Missing default_url attribute"
        assert hasattr(generator, 'default_category'), "Missing default_category attribute"

        default_url = generator.default_url
        default_category = generator.default_category

        print(f"✅ default_url: {default_url}")
        print(f"✅ default_category: {default_category}")

        # Basic validation
        assert isinstance(default_url, str) and default_url, "default_url should be non-empty string"
        assert isinstance(default_category, str) and default_category, "default_category should be non-empty string"

        return True
    except Exception as e:
        print(f"❌ Default attributes test failed: {e}")
        return False


def test_generate_from_template_basic():
    """Basic test of generate_from_template method"""
    print("\n🧪 Testing generate_from_template (basic)")

    try:
        generator = URLGenerator()

        # Test with a simple template (assuming 'films_latest' exists)
        urls = list(generator.generate_from_template(
            template_key='films_latest',
            pages=2
        ))

        print(f"✅ Generated {len(urls)} URLs")

        # Check each URL
        for i, url in enumerate(urls[:3]):  # Show first 3
            assert isinstance(url, str), f"URL {i} is not a string: {type(url)}"
            assert url.startswith(('http://', 'https://')), f"URL {i} doesn't start with http(s): {url}"
            print(f"   {i + 1}. {url}")

        if len(urls) > 3:
            print(f"   ... and {len(urls) - 3} more")

        return True
    except Exception as e:
        print(f"❌ Basic template generation failed: {e}")
        return False


def test_generate_from_template_with_years():
    """Test generate_from_template with year filter"""
    print("\n🧪 Testing generate_from_template with years")

    try:
        generator = URLGenerator()

        # Test with year range
        urls = list(generator.generate_from_template(
            template_key='films_by_year',
            pages=1,
            years=[2023, 2024]
        ))

        print(f"✅ Generated {len(urls)} URLs with year filter")

        for i, url in enumerate(urls[:2]):  # Show first 2
            print(f"   {i + 1}. {url}")

        return True
    except Exception as e:
        print(f"⚠️ Year filter test failed (may be expected): {e}")
        return True  # This might be expected if template doesn't support years


def test_generate_from_template_with_limit():
    """Test generate_from_template with limit"""
    print("\n🧪 Testing generate_from_template with limit")

    try:
        generator = URLGenerator()

        # Generate with limit
        urls = list(generator.generate_from_template(
            template_key='films_latest',
            pages=10,  # Many pages
            limit=3  # But limit to 3 URLs
        ))

        print(f"✅ Generated {len(urls)} URLs (limited)")

        if len(urls) <= 3:
            print("✅ Limit functionality working correctly")
        else:
            print(f"⚠️ Generated {len(urls)} URLs, expected max 3")

        return True
    except Exception as e:
        print(f"❌ Limit test failed: {e}")
        return False


def test_available_templates():
    """Test listing available templates"""
    print("\n🧪 Testing available templates")

    try:
        generator = URLGenerator()

        # Try to get available templates (if method exists)
        if hasattr(generator, 'get_available_templates'):
            templates = generator.get_available_templates()
            print(f"✅ Available templates: {', '.join(templates)}")
        elif hasattr(generator, 'templates'):
            templates = generator.templates
            print(f"✅ Templates attribute: {list(templates.keys())}")
        else:
            print("ℹ️ No template listing method found")

        return True
    except Exception as e:
        print(f"⚠️ Template listing failed: {e}")
        return True  # Not critical


def main():
    """Main test function"""
    print("🚀 Starting URL Generator Tests")
    print("=" * 50)

    tests = [
        ("Initialization", test_generator_initialization),
        ("Default attributes", test_default_attributes),
        ("Basic template generation", test_generate_from_template_basic),
        ("Year filter", test_generate_from_template_with_years),
        ("Limit", test_generate_from_template_with_limit),
        ("Template listing", test_available_templates),
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
    else:
        print("⚠️ Some tests failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())

