"""
LordFilm Parser - Main configuration file
Technical settings for parser, database, and bot
"""

import os

# =============================================================================
# Database Configuration
# =============================================================================
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB', 'lordfilm_db'),
    'user': os.getenv('POSTGRES_USER', 'lordfilm_user'),
    'password': os.getenv('POSTGRES_PASSWORD', ''),
}

# =============================================================================
# Selenium Configuration  
# =============================================================================
SELENIUM_URL = os.getenv('SELENIUM_URL', 'http://localhost:4444/wd/hub')
SELENIUM_TIMEOUT = 30

# =============================================================================
# Telegram Bot Configuration
# =============================================================================
BOT_TOKEN = os.getenv('BOT_TOKEN', '')

# =============================================================================
# Parser Core Settings
# =============================================================================
DEFAULT_URL = "https://wk.lordfilm17.ru"
DEFAULT_CATEGORY = "filmy"

# Year range for parsing
YEAR_RANGE = (2016, 2025)

# =============================================================================
# Parsing Performance Settings
# =============================================================================

# Batch processing
LINKS_BATCH_SIZE = 100      # Number of links to process in one batch
FILMS_BATCH_SIZE = 10       # Number of films to process in one batch

# Delays between operations (in seconds)
BATCH_DELAY = 1             # Delay between link batches
FILM_DELAY = 2              # Delay between film parsing
REQUEST_DELAY = 1.0         # Delay between HTTP requests

# Retry configuration
MAX_RETRIES = 2             # Maximum retry attempts for failed requests
RETRY_DELAY = 5             # Delay between retries (in seconds)

# Timeouts
REQUEST_TIMEOUT = 30        # HTTP request timeout (in seconds)

# =============================================================================
# URL Generation Settings
# =============================================================================
DAILY_PAGES = 50            # Number of pages for daily parsing
WEEKLY_PAGES = 50           # Number of pages for weekly parsing (if not 'all')
PARSE_PAGES = 50            # General number of pages for parsing

# =============================================================================
# User Agent & Headers
# =============================================================================
# USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
#
# HEADERS = {
#     'User-Agent': USER_AGENT,
#     'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
#     'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
#     'Accept-Encoding': 'gzip, deflate, br',
# }

# =============================================================================
# Logging Configuration
# =============================================================================
LOG_LEVEL = 'INFO'
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

# =============================================================================
# Feature Flags
# =============================================================================
# ENABLE_METRICS = True       # Enable performance metrics collection
# ENABLE_HEALTH_CHECKS = True # Enable health monitoring
# SAVE_HTML_DEBUG = False     # Save HTML for debugging purposes