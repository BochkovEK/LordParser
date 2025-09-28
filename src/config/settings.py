# page pattern: https://wk.lordfilm17.ru/filmy/2025/page/2/

import os
# from dotenv import load_dotenv

# load_dotenv()  # Загружаем переменные из .env

# Настройки БД
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB', 'movie_db'),
    'user': os.getenv('POSTGRES_USER', 'movie_user'),
    'password': os.getenv('POSTGRES_PASSWORD', ''),
}

# Настройки Selenium
SELENIUM_URL = os.getenv('SELENIUM_URL', 'http://localhost:4444/wd/hub')

# Настройки бота
BOT_TOKEN = os.getenv('BOT_TOKEN', '')

# Основные настройки парсера
DEFAULT_URL = "https://wk.lordfilm17.ru"
DEFAULT_CATEGORY = "filmy"
# DEFAULT_PAGES = 10
# DEFAULT_TOP_LIST = 10
DEFAULT_YEAR = None
DEFAULT_DEBUG = True
YEAR_RANGE = (2016, 2025)
PARSE_PAGES = 50 # Количество страниц для парсинга фильмов определенного года
NONE_RATING_KP = 6

# Парсинг
DAILY_PAGES = 50    # Для 2025
WEEKLY_PAGES = 50   # Для всех лет

# Пачки
LINKS_BATCH_SIZE = 100
FILMS_BATCH_SIZE = 10

# Паузы
BATCH_DELAY = 1     # 1 сек для ссылок
FILM_DELAY = 2      # 2 сек для фильмов

# Повторы
MAX_RETRIES = 2
RETRY_DELAY = 5

