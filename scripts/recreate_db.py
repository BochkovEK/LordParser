#!/usr/bin/env python3
"""
Скрипт ПЕРЕСОЗДАНИЯ БД - УДАЛЯЕТ ВСЕ ДАННЫЕ и создает таблицы заново

⚠️  ВНИМАНИЕ: Этот скрипт УДАЛИТ ВСЕ СУЩЕСТВУЮЩИЕ ДАННЫЕ безвозвратно!
💀 Используйте только для тестов или полного сброса базы данных

Примеры использования:
- 🧪 Тестирование (нужна чистая БД)
- 🔄 Разработка (поменялись модели)
- 🗑️  Полный сброс (база "сломалась")

🚫 НИКОГДА не запускайте на продакшене с важными данными!
"""
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.connection import db_manager
from src.database.models import drop_tables, create_tables


def main():
    print("🚨 ПРЕДУПРЕЖДЕНИЕ: Этот скрипт УДАЛИТ ВСЕ ДАННЫЕ в БД!")
    print("💀 Все таблицы и записи будут безвозвратно удалены!")

    # Запрос подтверждения
    response = input("\n❓ Вы уверены? Введите 'YES' для продолжения: ")
    if response != 'YES':
        print("✅ Отменено пользователем")
        return

    print("\n🔄 Пересоздание БД...")
    db_manager.connect()

    print("🗑️ Удаление таблиц...")
    drop_tables(db_manager.engine)

    print("🗃️ Создание таблиц...")
    create_tables(db_manager.engine)

    print("✅ База данных пересоздана успешно!")


if __name__ == "__main__":
    main()