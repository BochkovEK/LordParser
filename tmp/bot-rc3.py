import logging
from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters, CallbackQueryHandler
import psycopg2
from tmp.config import (
    BOT_TOKEN,
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
        self.top_size = 10  # Размер топа по умолчанию
        self.selected_year = None  # Выбранный год


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
    keyboard = [
        ['🎬 Top', '📊 Stats'],
        ['🔍 Search']
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

    await update.message.reply_text(
        "🎬 Добро пожаловать в Movie Bot!\n\n"
        "Выберите действие:\n"
        "• 🎬 Top - получить топ фильмов\n"
        "• 📊 Stats - статистика базы данных\n"
        "• 🔍 Search - поиск фильмов по названию\n\n"
        "Или просто введите /search <название> для поиска",
        reply_markup=reply_markup
    )


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик текстовых сообщений (кнопок)"""
    text = update.message.text

    if text == '🎬 Top':
        await show_year_buttons(update, context)
    elif text == '📊 Stats':
        await show_stats(update, context)
    elif text == '🔍 Search':
        await update.message.reply_text(
            "Введите название фильма для поиска:\nНапример: '/search Матрица'"
        )


async def show_year_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показать кнопки выбора года"""
    keyboard = []

    # Кнопка "За все года"
    keyboard.append([InlineKeyboardButton("За все года", callback_data='year_all')])

    # Создаем кнопки для годов с 1990 по 2025
    years = list(range(1990, 2026))
    row = []

    for year in years:
        row.append(InlineKeyboardButton(str(year), callback_data=f'year_{year}'))
        if len(row) == 4:  # 4 кнопки в строке
            keyboard.append(row)
            row = []

    if row:  # Добавляем последнюю неполную строку
        keyboard.append(row)

    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await update.callback_query.edit_message_text(
            "📅 Выберите год:",
            reply_markup=reply_markup
        )
    else:
        await update.message.reply_text(
            "📅 Выберите год:",
            reply_markup=reply_markup
        )


async def show_top_size_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE, year: str) -> None:
    """Показать кнопки выбора размера топа"""
    query = update.callback_query
    await query.answer()

    # Сохраняем выбранный год в контексте
    context.user_data['selected_year'] = year

    keyboard = [
        [
            InlineKeyboardButton("Топ 10", callback_data=f'top_10_{year}'),
            InlineKeyboardButton("Топ 25", callback_data=f'top_25_{year}'),
            InlineKeyboardButton("Топ 50", callback_data=f'top_50_{year}')
        ],
        [
            InlineKeyboardButton("Топ 100", callback_data=f'top_100_{year}'),
            InlineKeyboardButton("← Назад к выбору года", callback_data='back_to_years')
        ]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)

    year_text = "все года" if year == "all" else f"{year} год"
    await query.edit_message_text(
        f"📊 Выберите размер топа для {year_text}:",
        reply_markup=reply_markup
    )


async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик callback запросов от inline кнопок"""
    query = update.callback_query
    data = query.data

    if data.startswith('year_'):
        year = data.split('_')[1]
        await show_top_size_buttons(update, context, year)

    elif data.startswith('top_'):
        # Разбираем данные: top_10_1999 → size=10, year=1999
        parts = data.split('_')
        top_size = int(parts[1])
        year = parts[2]

        await query.answer()
        await get_top_movies(update, context, top_size, year)

    elif data == 'back_to_years':
        await show_year_buttons(update, context)


async def get_top_movies(update: Update, context: ContextTypes.DEFAULT_TYPE, top_size: int, year: str) -> None:
    """Получить топ фильмов"""
    query = update.callback_query

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Формируем запрос в зависимости от выбранного года
        if year == 'all':
            sql_query = '''
                SELECT title, year, imdb_rating, kp_rating, rating_avg, link
                FROM movies 
                WHERE rating_avg IS NOT NULL
                ORDER BY rating_avg DESC 
                LIMIT %s
            '''
            params = (top_size,)
            year_text = "за все года"
        else:
            sql_query = '''
                SELECT title, year, imdb_rating, kp_rating, rating_avg, link
                FROM movies 
                WHERE year = %s AND rating_avg IS NOT NULL
                ORDER BY rating_avg DESC 
                LIMIT %s
            '''
            params = (int(year), top_size)
            year_text = f"за {year} год"

        cursor.execute(sql_query, params)
        movies = cursor.fetchall()
        conn.close()

        if not movies:
            await query.edit_message_text(f"📭 Фильмы {year_text} не найдены")
            return

        response = f"🏆 Топ-{top_size} фильмов {year_text}:\n\n"
        for i, (title, year, imdb, kp, avg, link) in enumerate(movies, 1):
            response += f"{i}. 🎬 {title} ({year})\n"
            response += f"   ★ IMDb: {imdb or 'N/A'} | КП: {kp or 'N/A'} | Средний: {avg or 'N/A'}\n"
            response += f"   🔗 {link}\n\n"

        # Добавляем кнопку "Выбрать другой топ"
        keyboard = [[InlineKeyboardButton("← Выбрать другой топ", callback_data='back_to_years')]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await query.edit_message_text(response[:4000], reply_markup=reply_markup, disable_web_page_preview=True)

    except Exception as e:
        logger.error(f"Ошибка получения топа: {e}")
        await query.edit_message_text("❌ Ошибка при получении данных из базы")


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

        await update.message.reply_text(response)

    except Exception as e:
        logger.error(f"Ошибка получения статистики: {e}")
        await update.message.reply_text("❌ Ошибка при получении статистики")


async def search_movies(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Поиск фильмов по названию"""
    search_query = ' '.join(context.args) if context.args else update.message.text

    if not search_query or search_query.strip() == '🔍 Search':
        await update.message.reply_text("Введите название фильма для поиска:\nНапример: 'Матрица'")
        return

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
            response += f"{i}. 🎬 {title} ({year})\n"
            response += f"   ★ IMDb: {imdb or 'N/A'} | КП: {kp or 'N/A'} | Средний: {avg or 'N/A'}\n"
            response += f"   🔗 {link}\n\n"

        await update.message.reply_text(response[:4000], disable_web_page_preview=True)

    except Exception as e:
        logger.error(f"Ошибка поиска: {e}")
        await update.message.reply_text("❌ Ошибка при поиске фильмов")


def main() -> None:
    """Запуск бота"""
    try:
        application = Application.builder().token(BOT_TOKEN).build()

        # Регистрация обработчиков
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("search", search_movies))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
        application.add_handler(CallbackQueryHandler(handle_callback_query))

        logging.info("Бот запущен с обновленным интерфейсом")
        application.run_polling()

    except Exception as e:
        logging.error(f"Ошибка при запуске бота: {e}")


if __name__ == "__main__":
    main()