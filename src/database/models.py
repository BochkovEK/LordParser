from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, Text, JSON, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime
import json

Base = declarative_base()


class Film(Base):
    """Модель фильма"""
    __tablename__ = 'films'

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Основная информация
    url = Column(String(500), unique=True, nullable=False, index=True)
    title = Column(String(255))
    original_title = Column(String(255))
    year = Column(Integer)
    country = Column(String(100))
    categories = Column(JSON)  # Список категорий ['Боевик', 'Фантастика']
    director = Column(String(255))
    actors = Column(JSON)  # Список актеров
    description = Column(Text)

    # Рейтинги (сырые данные)
    lf_rating = Column(Float)  # LordFilm рейтинг
    lf_likes = Column(Integer)  # Лайки LordFilm
    lf_dislikes = Column(Integer)  # Дизлайки LordFilm
    kp_rating = Column(Float)  # КиноПоиск рейтинг
    imdb_rating = Column(Float)  # IMDB рейтинг

    # Итоговый рейтинг (вычисленный)
    final_rating = Column(Float, nullable=True)  # Вычисленный итоговый рейтинг 0-10
    rating_calculated_at = Column(DateTime, nullable=True)  # Когда был вычислен рейтинг

    # Системные поля
    is_active = Column(Boolean, default=True)
    first_seen_at = Column(DateTime, default=datetime.utcnow)
    last_updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Связи
    parsing_history = relationship("ParsingHistory", back_populates="film")

    def __repr__(self):
        return f"<Film(id={self.id}, title='{self.title}', year={self.year}, final_rating={self.final_rating})>"


class ParsingSession(Base):
    """Модель сессии парсинга"""
    __tablename__ = 'parsing_sessions'

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Параметры сессии
    session_type = Column(String(20), nullable=False)  # 'daily' | 'weekly'
    years_parsed = Column(String(100))  # '2025' или '2016-2025'
    pages_per_year = Column(Integer)  # 50 или 10
    status = Column(String(20), default='running')  # 'running' | 'completed' | 'failed'

    # Статистика
    links_found = Column(Integer, default=0)
    films_processed = Column(Integer, default=0)
    new_films_added = Column(Integer, default=0)
    existing_films_updated = Column(Integer, default=0)

    # Временные метки
    started_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime)

    # Связи
    parsing_history = relationship("ParsingHistory", back_populates="session")

    def __repr__(self):
        return f"<ParsingSession(id={self.id}, type='{self.session_type}', status='{self.status}')>"


class ParsingHistory(Base):
    """Детальная история парсинга"""
    __tablename__ = 'parsing_history'

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Внешние ключи
    film_id = Column(Integer, ForeignKey('films.id'), index=True)
    session_id = Column(Integer, ForeignKey('parsing_sessions.id'), index=True)

    # Результат парсинга
    parsing_type = Column(String(20))  # 'links' | 'film_details'
    success = Column(Boolean, default=True)
    error_message = Column(Text)

    # Данные
    data_processed = Column(JSON)  # Сырые данные для дебага

    # Временная метка
    parsed_at = Column(DateTime, default=datetime.utcnow)

    # Связи
    film = relationship("Film", back_populates="parsing_history")
    session = relationship("ParsingSession", back_populates="parsing_history")

    def __repr__(self):
        return f"<ParsingHistory(id={self.id}, film_id={self.film_id}, type='{self.parsing_type}')>"


# Вспомогательные функции
def create_tables(engine):
    """Создает все таблицы в БД"""
    Base.metadata.create_all(engine)


def drop_tables(engine):
    """Удаляет все таблицы из БД (для тестов)"""
    Base.metadata.drop_all(engine)
