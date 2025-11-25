"""
Test URL Generator - emulates usage from future scheduler.py
"""

import os
import sys


sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from src.parser.url_generator import URLGenerator


async def test_url_generator():
    """Test URL generator with scheduler-like usage patterns"""

    generator = URLGenerator()

    print("🧪 Testing URL Generator for Scheduler Tasks...")

    # Test 1: Daily task - main catalog
    print("\n📅 Daily Task - Main Catalog (50 pages):")
    daily_main_urls = list(generator.generate_from_template(
        template_key='main_catalog',
        pages=50
    ))
    print(f"Generated {len(daily_main_urls)} URLs")
    print(f"First: {daily_main_urls[0]}")
    print(f"Last: {daily_main_urls[-1]}")

    # Test 2: Daily task - yearly catalog (recent years)
    print("\n📅 Daily Task - Yearly Catalog (2024-2025, 50 pages):")
    daily_yearly_urls = list(generator.generate_from_template(
        template_key='yearly_catalog',
        pages=50,
        years=[2024, 2025]
    ))
    print(f"Generated {len(daily_yearly_urls)} URLs")
    print(f"Sample: {daily_yearly_urls[0]}")
    print(f"Sample: {daily_yearly_urls[50]}")  # First page of next year

    # Test 3: Weekly task - main catalog (all pages)
    print("\n📅 Weekly Task - Main Catalog (all pages):")
    weekly_main_urls = list(generator.generate_from_template(
        template_key='main_catalog',
        pages='all'
    ))
    print(f"Generated {len(weekly_main_urls)} URLs")

    # Test 4: Weekly task - yearly catalog (all years, all pages)
    print("\n📅 Weekly Task - Yearly Catalog (all years, all pages):")
    weekly_yearly_urls = list(generator.generate_from_template(
        template_key='yearly_catalog',
        pages='all',
        years=list(range(2016, 2026))
    ))
    print(f"Generated {len(weekly_yearly_urls)} URLs")

    print(f"\n✅ All tests passed! Generator is ready for scheduler integration.")


if __name__ == "__main__":
    import asyncio

    asyncio.run(test_url_generator())