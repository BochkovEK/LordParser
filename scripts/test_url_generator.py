"""
Test URL Generator - emulates usage from future scheduler.py
"""

import sys
import os

# Add src to path to allow imports from any directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.utils.url_generator import URLGenerator


def test_url_generator():
    """Test URL generator with scheduler-like usage patterns"""

    generator = URLGenerator()

    print("🧪 Testing URL Generator for Scheduler Tasks...")

    # Test 1: Daily task - main catalog (50 pages)
    print("\n1. Daily Task - Main Catalog (50 pages):")
    urls = []
    for url in generator.generate_from_template(
        template_key='main_catalog',
        pages=50
    ):
        urls.append(url)
        if len(urls) <= 3:  # Show first 3 examples
            print(f"   {url}")

    print(f"   Total URLs: {len(urls)}")

    # Test 2: Daily task - yearly catalog (recent years, 50 pages)
    print("\n2. Daily Task - Yearly Catalog (2024-2025, 50 pages):")
    urls = []
    for url in generator.generate_from_template(
        template_key='yearly_catalog',
        pages=50,
        years=[2024, 2025]
    ):
        urls.append(url)
        if len(urls) <= 3:  # Show first 3 examples
            print(f"   {url}")

    print(f"   Total URLs: {len(urls)}")

    # Test 3: Weekly task - main catalog (all pages - using default)
    print("\n3. Weekly Task - Main Catalog (all pages - default):")
    urls = []
    for url in generator.generate_from_template(
        template_key='main_catalog',
        pages='all'
    ):
        urls.append(url)
        if len(urls) <= 3:  # Show first 3 examples
            print(f"   {url}")
        if len(urls) == 50:  # Show last one if many
            print(f"   ... and {len(urls)-3} more")
            break

    print(f"   Total URLs: {len(urls)}")

    # Test 4: Weekly task - yearly catalog (all years, all pages)
    print("\n4. Weekly Task - Yearly Catalog (all years, all pages):")
    urls = []
    for url in generator.generate_from_template(
        template_key='yearly_catalog',
        pages='all',
        years=list(range(2016, 2026))
    ):
        urls.append(url)
        if len(urls) <= 3:  # Show first 3 examples
            print(f"   {url}")
        if len(urls) == 100:  # Show sample from middle
            print(f"   ... sample: {url}")
        if len(urls) == 500:  # Stop early for demo
            print(f"   ... and {len(urls)-4} more")
            break

    print(f"   Total URLs generated: {len(urls)}")

    print(f"\n✅ Generator is ready for scheduler!")


if __name__ == "__main__":
    test_url_generator()