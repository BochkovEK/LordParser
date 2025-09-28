#!/usr/bin/env python3
"""
Скрипт инициализации БД - создает таблицы
"""
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.connection import db_manager
from src.database.models import create_tables


def main():
    print("🔄 Подключение к БД...")
    db_manager.connect()

    print("🗃️ Создание таблиц...")
    create_tables(db_manager.engine)

    print("✅ База данных инициализирована успешно!")


if __name__ == "__main__":
    main()