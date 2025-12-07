"""
LordFilm Parser - Configuration module
Loads settings from config.ini with environment variable overrides
"""

import os
import sys
import configparser
from pathlib import Path
from typing import Dict, Any


def find_project_root():
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
    3. Default values
    """
    config = configparser.ConfigParser()

    # Set comprehensive defaults
    config.read_dict({
        'database': {
            'host': 'localhost',
            'port': '5432',
            'name': 'lordfilm_db',
            'user': 'lordfilm_user',
            'password': '',
        },
        'telegram': {
            'bot_token': '',
        },
        'parser': {
            'default_url': 'https://wk.lordfilm17.ru',
            'default_category': 'filmy',
            'year_start': '2016',
            'year_end': '2025',
        },
        'selenium': {
            'timeout': '30',
            'request_timeout': '15',
            'max_concurrent_tasks': '5',
        },
        'performance': {
            'links_batch_size': '100',
            'films_batch_size': '10',
            'batch_delay': '1',
            'film_delay': '2',
            'request_delay': '1.0',
            'max_retries': '2',
            'retry_delay': '5',
        },
        'url_generation': {
            'daily_pages': '50',
            'weekly_pages': '50',
            'parse_pages': '50',
        },
        'logging': {
            'level': 'INFO',
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            'date_format': '%Y-%m-%d %H:%M:%S',
        },
        'schedules': {
            'daily_pages': '50',
            'weekly_pages': '50',
        },
        'features': {
            'enable_metrics': 'false',
            'enable_health_checks': 'false',
            'save_html_debug': 'false',
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


# Load configuration
_config = load_config()

# =============================================================================
# Database Configuration (backward compatibility)
# =============================================================================
DB_CONFIG = {
    'host': _config['database']['host'],
    'port': _config['database'].getint('port'),
    'database': _config['database']['name'],
    'user': _config['database']['user'],
    'password': _config['database']['password'],
}

# =============================================================================
# Selenium Configuration
# =============================================================================
SELENIUM_URL = os.getenv('SELENIUM_URL', 'http://localhost:4444/wd/hub')
SELENIUM_TIMEOUT = _config['selenium'].getint('timeout')
REQUEST_TIMEOUT = _config['selenium'].getint('request_timeout')
MAX_CONCURRENT_PARSE_TASKS = _config['selenium'].getint('max_concurrent_tasks')

# =============================================================================
# Telegram Bot Configuration
# =============================================================================
BOT_TOKEN = _config['telegram']['bot_token']

# =============================================================================
# Parser Core Settings
# =============================================================================
DEFAULT_URL = _config['parser']['default_url']
DEFAULT_CATEGORY = _config['parser']['default_category']

# Year range for parsing
YEAR_START = _config['parser'].getint('year_start')
YEAR_END = _config['parser'].getint('year_end')
YEAR_RANGE = (YEAR_START, YEAR_END)

# =============================================================================
# Parsing Performance Settings
# =============================================================================

# Batch processing
LINKS_BATCH_SIZE = _config['performance'].getint('links_batch_size')
FILMS_BATCH_SIZE = _config['performance'].getint('films_batch_size')

# Delays between operations (in seconds)
BATCH_DELAY = _config['performance'].getfloat('batch_delay')
FILM_DELAY = _config['performance'].getfloat('film_delay')
REQUEST_DELAY = _config['performance'].getfloat('request_delay')

# Retry configuration
MAX_RETRIES = _config['performance'].getint('max_retries')
RETRY_DELAY = _config['performance'].getfloat('retry_delay')

# =============================================================================
# URL Generation Settings
# =============================================================================
DAILY_PAGES = _config['url_generation'].getint('daily_pages')
WEEKLY_PAGES = _config['url_generation'].getint('weekly_pages')
PARSE_PAGES = _config['url_generation'].getint('parse_pages')

# Schedule-specific settings
DAILY_PAGES = _config['schedules'].getint('daily_pages')
WEEKLY_PAGES = _config['schedules'].getint('weekly_pages')

# =============================================================================
# Logging Configuration
# =============================================================================
LOG_LEVEL = _config['logging']['level']
LOG_FORMAT = _config['logging']['format']
LOG_DATE_FORMAT = _config['logging']['date_format']

LOGGING_CONFIG = {
    'level': LOG_LEVEL,
    'format': LOG_FORMAT,
    'date_format': LOG_DATE_FORMAT,
}

# =============================================================================
# Feature Flags
# =============================================================================
ENABLE_METRICS = _config['features'].getboolean('enable_metrics')
ENABLE_HEALTH_CHECKS = _config['features'].getboolean('enable_health_checks')
SAVE_HTML_DEBUG = _config['features'].getboolean('save_html_debug')

# =============================================================================
# Helper functions
# =============================================================================
def get_database_url() -> str:
    """Generate SQLAlchemy database URL"""
    db = DB_CONFIG
    return f"postgresql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['database']}"


def get_config_dict() -> Dict[str, Any]:
    """Get all configuration as dictionary"""
    return {
        'database': dict(_config['database']),
        'parser': dict(_config['parser']),
        'performance': dict(_config['performance']),
        'logging': dict(_config['logging']),
    }


def reload_config(config_path: str = None):
    """Reload configuration from file"""
    global _config, DB_CONFIG, SELENIUM_TIMEOUT, REQUEST_TIMEOUT, \
           MAX_CONCURRENT_PARSE_TASKS, BOT_TOKEN, DEFAULT_URL, \
           DEFAULT_CATEGORY, YEAR_START, YEAR_END, YEAR_RANGE, \
           LINKS_BATCH_SIZE, FILMS_BATCH_SIZE, BATCH_DELAY, \
           FILM_DELAY, REQUEST_DELAY, MAX_RETRIES, RETRY_DELAY, \
           DAILY_PAGES, WEEKLY_PAGES, PARSE_PAGES, LOG_LEVEL, \
           LOG_FORMAT, LOG_DATE_FORMAT, LOGGING_CONFIG, \
           ENABLE_METRICS, ENABLE_HEALTH_CHECKS, SAVE_HTML_DEBUG

    _config = load_config(config_path)

    # Reload all variables
    DB_CONFIG = {
        'host': _config['database']['host'],
        'port': _config['database'].getint('port'),
        'database': _config['database']['name'],
        'user': _config['database']['user'],
        'password': _config['database']['password'],
    }

    SELENIUM_TIMEOUT = _config['selenium'].getint('timeout')
    REQUEST_TIMEOUT = _config['selenium'].getint('request_timeout')
    MAX_CONCURRENT_PARSE_TASKS = _config['selenium'].getint('max_concurrent_tasks')
    BOT_TOKEN = _config['telegram']['bot_token']
    DEFAULT_URL = _config['parser']['default_url']
    DEFAULT_CATEGORY = _config['parser']['default_category']
    YEAR_START = _config['parser'].getint('year_start')
    YEAR_END = _config['parser'].getint('year_end')
    YEAR_RANGE = (YEAR_START, YEAR_END)
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

    LOGGING_CONFIG = {
        'level': LOG_LEVEL,
        'format': LOG_FORMAT,
        'date_format': LOG_DATE_FORMAT,
    }

    ENABLE_METRICS = _config['features'].getboolean('enable_metrics')
    ENABLE_HEALTH_CHECKS = _config['features'].getboolean('enable_health_checks')
    SAVE_HTML_DEBUG = _config['features'].getboolean('save_html_debug')


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