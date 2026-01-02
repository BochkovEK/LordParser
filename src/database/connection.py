from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
import logging
from .models import Base
from src.config.config import DB_CONFIG

# Логгер для этого модуля
logger = logging.getLogger(__name__)


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

        logger.info(f"Connecting to database: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")

        try:
            self.engine = create_engine(connection_string)
            self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
            logger.info("✅ Database engine created successfully")
        except Exception as e:
            logger.error(f"❌ Failed to create database engine: {e}")
            raise

    def get_session(self):
        """Возвращает сессию БД"""
        if not self.SessionLocal:
            self.connect()
        return self.SessionLocal()

    def test_connection(self) -> bool:
        """
        Test database connection

        Returns:
            True if connection successful, False otherwise
        """
        try:
            if not self.SessionLocal:
                self.connect()

            session = self.SessionLocal()

            # Простая проверка запросом
            result = session.execute(text("SELECT 1")).scalar()
            session.close()

            if result == 1:
                logger.info(
                    f"✅ Database connection test passed: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
                return True
            else:
                logger.error(f"❌ Database test query returned unexpected result: {result}")
                return False

        except Exception as e:
            logger.error(f"❌ Database connection test failed: {type(e).__name__}")
            logger.debug(f"Connection error details: {e}", exc_info=True)
            return False

    def get_connection_info(self) -> dict:
        """Get database connection info (without password)"""
        return {
            'host': DB_CONFIG['host'],
            'port': DB_CONFIG['port'],
            'database': DB_CONFIG['database'],
            'user': DB_CONFIG['user'],
            'password_set': bool(DB_CONFIG['password'])  # Only indicate if password is set
        }


# Глобальный экземпляр менеджера БД
db_manager = DatabaseManager()

