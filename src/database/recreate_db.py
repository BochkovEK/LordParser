#!/usr/bin/env python3
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.connection import db_manager
from src.database.models import drop_tables, create_tables


def main():
    print("🔄 Пересоздание БД...")
    db_manager.connect()

    print("🗑️ Удаление таблиц...")
    drop_tables(db_manager.engine)

    print("🗃️ Создание таблиц...")
    create_tables(db_manager.engine)

    print("✅ База данных пересоздана успешно!")


if __name__ == "__main__":
    main()