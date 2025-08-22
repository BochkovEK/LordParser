import logging
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from parser.lordfilm_parser import LordFilmParser
import json
from config import (  # Импортируем все необходимые константы
    BOT_TOKEN,
    DEFAULT_URL,
    DEFAULT_PAGES,
    DEFAULT_TOP_LIST,
    DEFAULT_YEAR,
    DEFAULT_DEBUG
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


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик команды /start"""
    await update.message.reply_text(
        "🎬 Бот для парсинга фильмов с LordFilm\n\n"
        "Доступные команды:\n"
        "/config - текущие настройки\n"
        "/set_url <url> - изменить URL\n"
        "/set_pages <N> - число страниц\n"
        "/set_top <N> - размер топа\n"
        "/set_year <год> - фильтр по году\n"
        "/set_debug <on/off> - режим отладки\n"
        "/parse - запустить парсинг"
    )


async def show_config(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показать текущую конфигурацию"""
    config = context.bot_data.setdefault('config', BotConfig())
    response = (
        f"⚙️ Текущие настройки:\n"
        f"• URL: {config.url}\n"
        f"• Страниц: {config.pages}\n"
        f"• Топ: {config.top_list}\n"
        f"• Год: {config.year or 'не задан'}\n"
        f"• Отладка: {'вкл' if config.debug else 'выкл'}"
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
        if param_name == 'url':
            config.url = value
        elif param_name == 'pages':
            config.pages = int(value)
        elif param_name == 'top':
            config.top_list = int(value)
        elif param_name == 'year':
            config.year = int(value) if value.lower() != 'none' else None
        elif param_name == 'debug':
            config.debug = value.lower() == 'on'

        await update.message.reply_text(f"✅ Параметр {param_name} установлен: {value}")
    except (ValueError, TypeError):
        await update.message.reply_text("❌ Некорректное значение")


async def parse_movies(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Запуск парсинга с текущими настройками"""
    config = context.bot_data.setdefault('config', BotConfig())

    await update.message.reply_text("⏳ Начинаю парсинг...")

    try:
        parser = LordFilmParser(
            base_url=config.url,
            year=config.year,
            debug=config.debug,
            # top_list=config.top_list
        )

        results = {}
        for sort_by in ['kp', 'imdb', 'avg']:
            movies = parser.get_sorted_movies(pages=config.pages, sort_by=sort_by)[:config.top_list]
            results[sort_by] = movies

            # Отправляем спислк в чат
            preview = "\n".join(
                f"{i}. {m['title']} ({m['year']}) - {m.get(f'rating_{sort_by}', m.get('rating_avg', 'N/A'))} {m['link']}"
                for i, m in enumerate(movies[:config.top_list], 1)
            )
            await update.message.reply_text(
                f"🏆 Топ-{config.top_list} по {sort_by.upper()}:\n{preview}"
            )

            # Отправляем полный список как файл
            with open(f'top_{sort_by}.json', 'w', encoding='utf-8') as f:
                json.dump(movies, f, ensure_ascii=False, indent=2)

            with open(f'top_{sort_by}.json', 'rb') as f:
                await update.message.reply_document(
                    document=f,
                    caption=f"Полный список топ {config.top_list} по {sort_by.upper()}"
                )

        await update.message.reply_text("✅ Парсинг завершен успешно!")

    except Exception as e:
        logger.error(f"Ошибка парсинга: {e}")
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")


def main() -> None:
    """Запуск бота"""
    try:
        application = Application.builder().token(BOT_TOKEN).build()

        # Регистрация обработчиков команд
        handlers = [
            CommandHandler("start", start),
            CommandHandler("set_url", lambda u, c: set_parameter(u, c, 'url')),
            CommandHandler("set_pages", lambda u, c: set_parameter(u, c, 'pages')),
            CommandHandler("set_top", lambda u, c: set_parameter(u, c, 'top')),
            CommandHandler("set_year", lambda u, c: set_parameter(u, c, 'year')),
            CommandHandler("set_debug", lambda u, c: set_parameter(u, c, 'debug')),
            CommandHandler("parse", parse_movies),
            CommandHandler("config", show_config),
        ]

        for handler in handlers:
            application.add_handler(handler)

        logging.info("Бот запущен")
        application.run_polling()

    except Exception as e:
        logging.error(f"Ошибка при запуске бота: {e}")

    # # CommandHandler("config", show_config),

    # for handler in handlers:
    #     application.add_handler(handler)
    #
    # application.run_polling()

if __name__ == "__main__":
    main()

