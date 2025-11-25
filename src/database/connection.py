from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from .models import Base
from src.config.config import DB_CONFIG


class DatabaseManager:
    def __init__(self):
        self.engine = None
        self.SessionLocal = None

    def connect(self):
        """Устанавливает соединение с БД"""
        connection_string = (
            f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
            f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )

        self.engine = create_engine(connection_string)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)

    def get_session(self):
        """Возвращает сессию БД"""
        if not self.SessionLocal:
            self.connect()
        return self.SessionLocal()


# Глобальный экземпляр менеджера БД
db_manager = DatabaseManager()