"""
LordFilm Parser - Configuration module
Loads settings from config.ini with environment variable overrides

Structure:
1. Constants with default values
2. Functions for configuration loading
3. Override constants with loaded config
"""

import os
import sys
import configparser
from pathlib import Path
from typing import Dict, Any, List

# =============================================================================
# 1. CONSTANTS WITH DEFAULT VALUES (if config.ini fails to load)
# =============================================================================

# Database Configuration - defaults
DB_HOST = 'localhost'
DB_PORT = 5432
DB_NAME = 'lordfilm_db'
DB_USER = 'lordfilm_user'
DB_PASSWORD = ''

# Selenium Configuration - defaults
SELENIUM_URL = os.getenv('SELENIUM_URL', 'http://localhost:4444/wd/hub')
SELENIUM_TIMEOUT = 30
REQUEST_TIMEOUT = 15
MAX_CONCURRENT_PARSE_TASKS = 5

# Telegram Bot Configuration - defaults
BOT_TOKEN = ''

# Parser Core Settings - defaults
DEFAULT_URL = 'https://sr.lordfilm17.ru'
DEFAULT_CATEGORY = 'filmy'
YEAR_START = 2016
YEAR_END = 2025

# Parsing Performance Settings - defaults
LINKS_BATCH_SIZE = 100
FILMS_BATCH_SIZE = 10
BATCH_DELAY = 1.0
FILM_DELAY = 2.0
REQUEST_DELAY = 1.0
MAX_RETRIES = 2
RETRY_DELAY = 5.0

# URL Generation Settings - defaults
DAILY_PAGES = 50
WEEKLY_PAGES = 50
PARSE_PAGES = 50

# Logging Configuration - defaults
LOG_LEVEL = 'INFO'
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_DATE_FORMAT = 'Y-m-d H:M:S'

# Feature Flags - defaults
ENABLE_METRICS = False
ENABLE_HEALTH_CHECKS = False
SAVE_HTML_DEBUG = False

# Rating Calculation Configuration - defaults (HARDCODED, not from config.ini)
RATING_WEIGHTS = {
    'kp': 1.0,          # КиноПоиск вес
    'imdb': 0.5,        # IMDb вес (в 2 раза меньше KP)
    'lf_base': 2.0,     # Базовый вес LordFilm
    'smoothing_k': 50,  # Константа сглаживания для голосов
}

COUNTRY_MULTIPLIERS = {
    'level1': 1.0,      # Лучшие кинематографии
    'level2': 0.9,      # Средние
    'level3': 0.7,      # Остальные
}

COUNTRY_LISTS = {
    'level1': [
        'США', 'Великобритания', 'Франция', 'Германия', 'Италия',
        'Россия', 'СССР', 'Канада', 'Австралия', 'Испания'
    ],
    'level2': [
        'Польша', 'Чехия', 'Венгрия', 'Швеция', 'Норвегия', 'Дания',
        'Финляндия', 'Нидерланды', 'Бельгия', 'Австрия', 'Швейцария',
        'Гонконг', 'Тайвань', 'Индия', 'Бразилия', 'Аргентина', 'Мексика',
        'Иран', 'Турция', 'Израиль'
    ]
}

# =============================================================================
# 2. FUNCTIONS FOR CONFIGURATION LOADING
# =============================================================================

def find_project_root() -> Path:
    """Find project root directory"""
    current = Path(__file__).parent
    while current.name != 'src' and current.parent != current:
        current = current.parent

    if current.name == 'src':
        return current.parent
    return current


def load_config(config_path: str = None) -> configparser.ConfigParser:
    """
    Load configuration from config.ini file with environment variable overrides

    Priority:
    1. Environment variables (DATABASE_HOST, PARSER_DEFAULT_URL, etc.)
    2. config.ini file
    3. Default values (from constants above)
    """
    config = configparser.ConfigParser()

    # Set defaults matching our constants
    config.read_dict({
        'database': {
            'host': DB_HOST,
            'port': str(DB_PORT),
            'name': DB_NAME,
            'user': DB_USER,
            'password': DB_PASSWORD,
        },
        'telegram': {
            'bot_token': BOT_TOKEN,
        },
        'parser': {
            'default_url': DEFAULT_URL,
            'default_category': DEFAULT_CATEGORY,
            'year_start': str(YEAR_START),
            'year_end': str(YEAR_END),
        },
        'selenium': {
            'timeout': str(SELENIUM_TIMEOUT),
            'request_timeout': str(REQUEST_TIMEOUT),
            'max_concurrent_tasks': str(MAX_CONCURRENT_PARSE_TASKS),
        },
        'performance': {
            'links_batch_size': str(LINKS_BATCH_SIZE),
            'films_batch_size': str(FILMS_BATCH_SIZE),
            'batch_delay': str(BATCH_DELAY),
            'film_delay': str(FILM_DELAY),
            'request_delay': str(REQUEST_DELAY),
            'max_retries': str(MAX_RETRIES),
            'retry_delay': str(RETRY_DELAY),
        },
        'url_generation': {
            'daily_pages': str(DAILY_PAGES),
            'weekly_pages': str(WEEKLY_PAGES),
            'parse_pages': str(PARSE_PAGES),
        },
        'logging': {
            'level': LOG_LEVEL,
            'format': LOG_FORMAT.replace('%', '%%'),  # Escape for configparser
            'date_format': LOG_DATE_FORMAT,
        },
        'features': {
            'enable_metrics': 'true' if ENABLE_METRICS else 'false',
            'enable_health_checks': 'true' if ENABLE_HEALTH_CHECKS else 'false',
            'save_html_debug': 'true' if SAVE_HTML_DEBUG else 'false',
        }
    })

    # Determine config file path
    if config_path:
        config_file = Path(config_path)
    else:
        # Search in standard locations
        possible_paths = [
            Path('/etc/lordfilm-parser/config.ini'),  # System config
            find_project_root() / 'config' / 'config.ini',  # Project config
            Path.home() / '.config' / 'lordfilm-parser' / 'config.ini',  # User config
        ]

        config_file = None
        for path in possible_paths:
            if path.exists():
                config_file = path
                break

    # Load from file if found
    if config_file and config_file.exists():
        config.read(config_file)
        print(f"📁 Loaded config from: {config_file}", file=sys.stderr)

    # Override with environment variables
    for section in config.sections():
        for key in config[section]:
            # Convert section.key to ENV_VAR format
            env_var = f"{section.upper()}_{key.upper()}"
            if env_var in os.environ:
                config[section][key] = os.environ[env_var]

    return config


def get_database_url() -> str:
    """Generate SQLAlchemy database URL"""
    return f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def get_config_dict() -> Dict[str, Any]:
    """Get all configuration as dictionary"""
    return {
        'database': {
            'host': DB_HOST,
            'port': DB_PORT,
            'name': DB_NAME,
            'user': DB_USER,
            'password': DB_PASSWORD,
        },
        'parser': {
            'default_url': DEFAULT_URL,
            'default_category': DEFAULT_CATEGORY,
            'year_start': YEAR_START,
            'year_end': YEAR_END,
        },
        'rating': {
            'weights': RATING_WEIGHTS,
            'country_multipliers': COUNTRY_MULTIPLIERS,
            'country_lists': COUNTRY_LISTS,
        }
    }


def reload_config(config_path: str = None):
    """Reload configuration from file and update constants"""
    # This would need to be implemented to update global constants
    # For now, it's a placeholder
    pass


# =============================================================================
# 3. LOAD CONFIGURATION AND OVERRIDE CONSTANTS
# =============================================================================

try:
    _config = load_config()

    # Override constants with loaded config
    DB_HOST = _config['database']['host']
    DB_PORT = _config['database'].getint('port')
    DB_NAME = _config['database']['name']
    DB_USER = _config['database']['user']
    DB_PASSWORD = _config['database']['password']

    BOT_TOKEN = _config['telegram']['bot_token']

    DEFAULT_URL = _config['parser']['default_url']
    DEFAULT_CATEGORY = _config['parser']['default_category']
    YEAR_START = _config['parser'].getint('year_start')
    YEAR_END = _config['parser'].getint('year_end')

    SELENIUM_TIMEOUT = _config['selenium'].getint('timeout')
    REQUEST_TIMEOUT = _config['selenium'].getint('request_timeout')
    MAX_CONCURRENT_PARSE_TASKS = _config['selenium'].getint('max_concurrent_tasks')

    LINKS_BATCH_SIZE = _config['performance'].getint('links_batch_size')
    FILMS_BATCH_SIZE = _config['performance'].getint('films_batch_size')
    BATCH_DELAY = _config['performance'].getfloat('batch_delay')
    FILM_DELAY = _config['performance'].getfloat('film_delay')
    REQUEST_DELAY = _config['performance'].getfloat('request_delay')
    MAX_RETRIES = _config['performance'].getint('max_retries')
    RETRY_DELAY = _config['performance'].getfloat('retry_delay')

    DAILY_PAGES = _config['url_generation'].getint('daily_pages')
    WEEKLY_PAGES = _config['url_generation'].getint('weekly_pages')
    PARSE_PAGES = _config['url_generation'].getint('parse_pages')

    LOG_LEVEL = _config['logging']['level']
    LOG_FORMAT = _config['logging']['format']
    LOG_DATE_FORMAT = _config['logging']['date_format']

    ENABLE_METRICS = _config['features'].getboolean('enable_metrics')
    ENABLE_HEALTH_CHECKS = _config['features'].getboolean('enable_health_checks')
    SAVE_HTML_DEBUG = _config['features'].getboolean('save_html_debug')

except Exception as e:
    print(f"⚠️ Warning: Failed to load config.ini, using defaults: {e}", file=sys.stderr)


# =============================================================================
# 4. DERIVED CONSTANTS (calculated from other constants)
# =============================================================================

YEAR_RANGE = (YEAR_START, YEAR_END)

LOGGING_CONFIG = {
    'level': LOG_LEVEL,
    'format': LOG_FORMAT,
    'date_format': LOG_DATE_FORMAT,
}

DB_CONFIG = {
    'host': DB_HOST,
    'port': DB_PORT,
    'database': DB_NAME,
    'user': DB_USER,
    'password': DB_PASSWORD,
}

RATING_CONFIG = {
    'weights': RATING_WEIGHTS,
    'country_multipliers': COUNTRY_MULTIPLIERS,
    'country_lists': COUNTRY_LISTS,
}


# =============================================================================
# Module initialization
# =============================================================================
if __name__ == '__main__':
    # Test configuration loading
    print("🔧 Configuration test")
    print(f"Database URL: {get_database_url()}")
    print(f"Parser URL: {DEFAULT_URL}")
    print(f"Daily pages: {DAILY_PAGES}")
    print(f"Log level: {LOG_LEVEL}")
    print(f"Rating weights: {RATING_WEIGHTS}")