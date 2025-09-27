import logging
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
import psycopg2
from tmp.config import (  # Импортируем все необходимые константы
    BOT_TOKEN,
    DEFAULT_URL,
    DEFAULT_PAGES,
    DEFAULT_TOP_LIST,
    DEFAULT_YEAR,
    DEFAULT_DEBUG,
    DB_NAME,
    DB_USER,
    DB_PASSWORD,
    DB_HOST,
    DB_PORT
)

# Настройка логирования
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)


class BotConfig:
    """Класс для хранения конфигурации бота"""

    def __init__(self):
        self.url = DEFAULT_URL
        self.pages = DEFAULT_PAGES
        self.top_list = DEFAULT_TOP_LIST
        self.year = DEFAULT_YEAR
        self.debug = DEFAULT_DEBUG


def get_db_connection():
    """Создание соединения с PostgreSQL"""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик команды /start"""
    await update.message.reply_text(
        "🎬 Бот для поиска фильмов из базы данных\n\n"
        "Доступные команды:\n"
        "/top - топ фильмов по рейтингу\n"
        "/year <год> - фильмы по году\n"
        "/search <название> - поиск по названию\n"
        "/stats - статистика базы\n"
        "/config - текущие настройки\n"
        "/set_top <N> - размер топа\n"
        "/set_year <год> - фильтр по году\n"
        "/set_debug <on/off> - режим отладки"
    )


async def show_config(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показать текущую конфигурацию"""
    config = context.bot_data.setdefault('config', BotConfig())
    response = (
        f"⚙️ Текущие настройки:\n"
        f"• Топ: {config.top_list}\n"
        f"• Год: {config.year or 'не задан'}\n"
        f"• Отладка: {'вкл' if config.debug else 'выкл'}\n"
        f"• База данных: {DB_NAME}@{DB_HOST}"
    )

    await update.message.reply_text(response)


async def set_parameter(update: Update, context: ContextTypes.DEFAULT_TYPE, param_name: str) -> None:
    """Общий обработчик для установки параметров"""
    if not context.args:
        await update.message.reply_text(f"Укажите значение для {param_name}")
        return

    config = context.bot_data.setdefault('config', BotConfig())
    value = context.args[0]

    try:
        if param_name == 'top':
            config.top_list = int(value)
        elif param_name == 'year':
            config.year = int(value) if value.lower() != 'none' else None
        elif param_name == 'debug':
            config.debug = value.lower() == 'on'

        await update.message.reply_text(f"✅ Параметр {param_name} установлен: {value}")
    except (ValueError, TypeError):
        await update.message.reply_text("❌ Некорректное значение")


async def get_top_movies(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Получить топ фильмов из базы данных"""
    config = context.bot_data.setdefault('config', BotConfig())

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Получаем топ фильмов по среднему рейтингу
        cursor.execute('''
            SELECT title, year, imdb_rating, kp_rating, rating_avg, link
            FROM movies 
            WHERE imdb_rating IS NOT NULL AND kp_rating IS NOT NULL
            ORDER BY rating_avg DESC 
            LIMIT %s
        ''', (config.top_list,))

        movies = cursor.fetchall()
        conn.close()

        if not movies:
            await update.message.reply_text("📭 В базе нет фильмов или данные отсутствуют")
            return

        response = "🏆 Топ фильмов по рейтингу:\n\n"
        for i, (title, year, imdb, kp, avg, link) in enumerate(movies, 1):
            response += f"{i}. {title} ({year})\n"
            response += f"   ★ IMDb: {imdb or 'N/A'} | КП: {kp or 'N/A'} | Средний: {avg or 'N/A'}\n"
            response += f"   🔗 {link}\n\n"

        await update.message.reply_text(response[:4000])  # Ограничение Telegram

    except Exception as e:
        logger.error(f"Ошибка получения топа: {e}")
        await update.message.reply_text(f"❌ Ошибка доступа к базе: {str(e)}")


async def get_movies_by_year(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Получить фильмы по году"""
    if not context.args:
        await update.message.reply_text("Укажите год: /year <год>")
        return

    try:
        year = int(context.args[0])
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT title, imdb_rating, kp_rating, rating_avg, link
            FROM movies 
            WHERE year = %s
            ORDER BY rating_avg DESC NULLS LAST
            LIMIT 20
        ''', (year,))

        movies = cursor.fetchall()
        conn.close()

        if not movies:
            await update.message.reply_text(f"📭 Фильмы за {year} год не найдены")
            return

        response = f"🎬 Фильмы {year} года:\n\n"
        for i, (title, imdb, kp, avg, link) in enumerate(movies, 1):
            response += f"{i}. {title}\n"
            response += f"   ★ IMDb: {imdb or 'N/A'} | КП: {kp or 'N/A'} | Средний: {avg or 'N/A'}\n\n"

        await update.message.reply_text(response[:4000])

    except ValueError:
        await update.message.reply_text("❌ Укажите корректный год")
    except Exception as e:
        logger.error(f"Ошибка поиска по году: {e}")
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")


async def search_movies(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Поиск фильмов по названию"""
    if not context.args:
        await update.message.reply_text("Укажите название: /search <название>")
        return

    search_query = ' '.join(context.args)
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT title, year, imdb_rating, kp_rating, rating_avg, link
            FROM movies 
            WHERE title ILIKE %s
            ORDER BY rating_avg DESC NULLS LAST
            LIMIT 15
        ''', (f'%{search_query}%',))

        movies = cursor.fetchall()
        conn.close()

        if not movies:
            await update.message.reply_text(f"🔍 Фильмы по запросу '{search_query}' не найдены")
            return

        response = f"🔍 Результаты поиска '{search_query}':\n\n"
        for i, (title, year, imdb, kp, avg, link) in enumerate(movies, 1):
            response += f"{i}. {title} ({year})\n"
            response += f"   ★ IMDb: {imdb or 'N/A'} | КП: {kp or 'N/A'} | Средний: {avg or 'N/A'}\n\n"

        await update.message.reply_text(response[:4000])

    except Exception as e:
        logger.error(f"Ошибка поиска: {e}")
        await update.message.reply_text(f"❌ Ошибка поиска: {str(e)}")


async def show_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показать статистику базы данных"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Общая статистика
        cursor.execute('SELECT COUNT(*) FROM movies')
        total_movies = cursor.fetchone()[0]

        cursor.execute('SELECT COUNT(DISTINCT year) FROM movies WHERE year IS NOT NULL')
        unique_years = cursor.fetchone()[0]

        cursor.execute('SELECT MIN(year), MAX(year) FROM movies WHERE year IS NOT NULL')
        min_year, max_year = cursor.fetchone()

        # Топ годов по количеству фильмов
        cursor.execute('''
            SELECT year, COUNT(*) 
            FROM movies 
            WHERE year IS NOT NULL 
            GROUP BY year 
            ORDER BY COUNT(*) DESC 
            LIMIT 5
        ''')
        top_years = cursor.fetchall()

        conn.close()

        response = (
            f"📊 Статистика базы данных:\n\n"
            f"• Всего фильмов: {total_movies}\n"
            f"• Уникальных годов: {unique_years}\n"
            f"• Диапазон: {min_year or 'N/A'} - {max_year or 'N/A'}\n\n"
            f"🏆 Топ годов по количеству фильмов:\n"
        )

        for year, count in top_years:
            response += f"  {year}: {count} фильмов\n"

        response += f"\n🔄 База обновляется автоматически каждые 24 часа"

        await update.message.reply_text(response)

    except Exception as e:
        logger.error(f"Ошибка получения статистики: {e}")
        await update.message.reply_text(f"❌ Ошибка статистики: {str(e)}")


async def get_random_movie(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Получить случайный фильм"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT title, year, imdb_rating, kp_rating, rating_avg, link
            FROM movies 
            WHERE imdb_rating IS NOT NULL 
            ORDER BY RANDOM() 
            LIMIT 1
        ''')

        movie = cursor.fetchone()
        conn.close()

        if not movie:
            await update.message.reply_text("📭 В базе нет фильмов")
            return

        title, year, imdb, kp, avg, link = movie
        response = (
            f"🎲 Случайный фильм:\n\n"
            f"🎬 {title} ({year})\n"
            f"★ IMDb: {imdb or 'N/A'}\n"
            f"★ КиноПоиск: {kp or 'N/A'}\n"
            f"★ Средний: {avg or 'N/A'}\n"
            f"🔗 {link}"
        )

        await update.message.reply_text(response)

    except Exception as e:
        logger.error(f"Ошибка случайного фильма: {e}")
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")


def main() -> None:
    """Запуск бота"""
    try:
        application = Application.builder().token(BOT_TOKEN).build()

        # Регистрация обработчиков команд
        handlers = [
            CommandHandler("start", start),
            CommandHandler("top", get_top_movies),
            CommandHandler("year", get_movies_by_year),
            CommandHandler("search", search_movies),
            CommandHandler("stats", show_stats),
            CommandHandler("random", get_random_movie),
            CommandHandler("set_top", lambda u, c: set_parameter(u, c, 'top')),
            CommandHandler("set_year", lambda u, c: set_parameter(u, c, 'year')),
            CommandHandler("set_debug", lambda u, c: set_parameter(u, c, 'debug')),
            CommandHandler("config", show_config),
        ]

        for handler in handlers:
            application.add_handler(handler)

        logging.info("Бот запущен и работает с базой данных")
        application.run_polling()

    except Exception as e:
        logging.error(f"Ошибка при запуске бота: {e}")


if __name__ == "__main__":
    main()