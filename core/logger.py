"""
Система логирования для проекта скрапинга Яндекс.Карт
"""

import sys
from pathlib import Path

from loguru import logger

from config.settings import LOGS_DIR, settings


def setup_logger():
    """Настройка системы логирования"""

    # Удаляем стандартные обработчики
    logger.remove()

    # Формат логов
    log_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
        "<level>{message}</level>"
    )

    # Консольный вывод
    logger.add(
        sys.stdout,
        format=log_format,
        level=settings.LOG_LEVEL,
        colorize=True,
        enqueue=True,
    )

    # Основной лог файл
    logger.add(
        LOGS_DIR / "scraper.log",
        format=log_format,
        level="DEBUG",
        rotation="10 MB",
        retention="30 days",
        compression="zip",
        enqueue=True,
    )

    # Файл только для ошибок
    logger.add(
        LOGS_DIR / "errors.log",
        format=log_format,
        level="ERROR",
        rotation="5 MB",
        retention="90 days",
        compression="zip",
        enqueue=True,
    )

    # Файл для статистики скрапинга
    logger.add(
        LOGS_DIR / "scraping_stats.log",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
        level="INFO",
        filter=lambda record: "STATS" in record["extra"],
        rotation="1 day",
        retention="365 days",
        enqueue=True,
    )


def get_logger(name: str = __name__):
    """
    Получить именованный логгер

    Args:
        name: Имя логгера

    Returns:
        Logger: Настроенный логгер
    """
    return logger.bind(name=name)


def log_scraping_stats(
    url: str,
    success: bool,
    processing_time: float,
    data_extracted: int = 0,
    error: str = None,
):
    """
    Логирование статистики скрапинга

    Args:
        url: URL который скрапили
        success: Успешность операции
        processing_time: Время обработки в секундах
        data_extracted: Количество извлеченных элементов данных
        error: Описание ошибки (если была)
    """
    stats_logger = logger.bind(STATS=True)

    stats_data = {
        "url": url,
        "success": success,
        "processing_time": processing_time,
        "data_extracted": data_extracted,
        "error": error,
    }

    if success:
        stats_logger.info(f"SCRAPING_SUCCESS: {stats_data}")
    else:
        stats_logger.error(f"SCRAPING_FAILED: {stats_data}")


class ScrapingMetrics:
    """Класс для сбора метрик скрапинга"""

    def __init__(self):
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.total_processing_time = 0.0
        self.errors = []

    def record_request(self, success: bool, processing_time: float, error: str = None):
        """Записать результат запроса"""
        self.total_requests += 1
        self.total_processing_time += processing_time

        if success:
            self.successful_requests += 1
        else:
            self.failed_requests += 1
            if error:
                self.errors.append(error)

    def get_success_rate(self) -> float:
        """Получить процент успешных запросов"""
        if self.total_requests == 0:
            return 0.0
        return (self.successful_requests / self.total_requests) * 100

    def get_average_processing_time(self) -> float:
        """Получить среднее время обработки"""
        if self.total_requests == 0:
            return 0.0
        return self.total_processing_time / self.total_requests

    def log_summary(self):
        """Вывести сводную статистику"""
        stats_logger = logger.bind(STATS=True)

        summary = {
            "total_requests": self.total_requests,
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,
            "success_rate": round(self.get_success_rate(), 2),
            "average_processing_time": round(self.get_average_processing_time(), 2),
            "total_errors": len(self.errors),
            "unique_errors": len(set(self.errors)),
        }

        stats_logger.info(f"SCRAPING_SUMMARY: {summary}")

        # Логируем наиболее частые ошибки
        if self.errors:
            from collections import Counter

            common_errors = Counter(self.errors).most_common(5)
            stats_logger.info(f"COMMON_ERRORS: {common_errors}")


# Инициализируем логгер при импорте модуля
setup_logger()

# Создаем основной логгер для модуля
module_logger = get_logger(__name__)

# Глобальный экземпляр метрик
scraping_metrics = ScrapingMetrics()
