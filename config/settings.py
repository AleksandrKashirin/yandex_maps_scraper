"""
Настройки конфигурации для системы скрапинга Яндекс.Карт
"""

import os
from pathlib import Path
from typing import Dict, List, Optional

from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Базовые пути
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"

# Создаем необходимые директории
DATA_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)


class Settings:
    """Класс настроек приложения"""

    # Основные настройки
    DEBUG: bool = os.getenv("DEBUG", "False").lower() == "true"
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    # Настройки скрапинга
    MIN_DELAY: float = float(
        os.getenv("MIN_DELAY", "2.0")
    )  # Минимальная задержка между запросами
    MAX_DELAY: float = float(
        os.getenv("MAX_DELAY", "5.0")
    )  # Максимальная задержка между запросами
    PAGE_LOAD_TIMEOUT: int = int(
        os.getenv("PAGE_LOAD_TIMEOUT", "30")
    )  # Таймаут загрузки страницы
    ELEMENT_WAIT_TIMEOUT: int = int(
        os.getenv("ELEMENT_WAIT_TIMEOUT", "3")
    )  # Таймаут ожидания элемента

    # Retry настройки
    MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "3"))
    RETRY_MULTIPLIER: int = int(os.getenv("RETRY_MULTIPLIER", "2"))

    # Rate limiting
    MAX_REQUESTS_PER_HOUR: int = int(os.getenv("MAX_REQUESTS_PER_HOUR", "5"))

    # User-Agent ротация
    ROTATE_USER_AGENT: bool = os.getenv("ROTATE_USER_AGENT", "True").lower() == "true"

    # Настройки Chrome
    HEADLESS: bool = os.getenv("HEADLESS", "True").lower() == "true"
    WINDOW_SIZE: str = os.getenv("WINDOW_SIZE", "1920,1080")

    # Настройки вывода
    OUTPUT_FORMAT: str = os.getenv("OUTPUT_FORMAT", "json")  # json, csv, database
    OUTPUT_PATH: str = os.getenv("OUTPUT_PATH", str(DATA_DIR))

    # База данных (если используется)
    DATABASE_URL: Optional[str] = os.getenv("DATABASE_URL")

    # Яндекс.Карты специфичные настройки
    YANDEX_MAPS_DOMAINS: List[str] = [
        "yandex.ru",
        "yandex.com",
        "yandex.com.ge",
        "yandex.by",
        "yandex.kz",
        "yandex.uz",
    ]

    # Домены для определения валидных URL
    ALLOWED_URL_PATTERNS: List[str] = [
        r"https://yandex\.[a-z\.]+/maps/org/",
        r"https://yandex\.[a-z\.]+/maps/-/",
    ]


# Создаем экземпляр настроек
settings = Settings()
