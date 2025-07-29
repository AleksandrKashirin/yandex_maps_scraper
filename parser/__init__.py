"""
Парсеры данных для системы скрапинга Яндекс.Карт

Этот модуль содержит специализированные парсеры для извлечения и очистки
различных типов данных с веб-страниц предприятий.
"""

from .base_parser import BaseParser, ParseResult
from .contact_parser import ContactParser
from .review_parser import ReviewParser
from .schedule_parser import ScheduleParser
from .service_parser import ServiceParser

# Версия парсеров
PARSERS_VERSION = "1.0"

__all__ = [
    "BaseParser",
    "ParseResult",
    "ServiceParser",
    "ReviewParser",
    "ContactParser",
    "ScheduleParser",
    "PARSERS_VERSION",
]


class UnifiedDataParser:
    """Унифицированный парсер для всех типов данных"""

    def __init__(self):
        self.service_parser = ServiceParser()
        self.review_parser = ReviewParser()
        self.contact_parser = ContactParser()
        self.schedule_parser = ScheduleParser()

    def parse_services(self, raw_data: str) -> list:
        """Парсинг услуг"""
        return self.service_parser.parse(raw_data)

    def parse_reviews(self, raw_data: str) -> list:
        """Парсинг отзывов"""
        return self.review_parser.parse(raw_data)

    def parse_contacts(self, raw_data: dict) -> dict:
        """Парсинг контактной информации"""
        return self.contact_parser.parse(raw_data)

    def parse_schedule(self, raw_data: str) -> dict:
        """Парсинг расписания работы"""
        return self.schedule_parser.parse(raw_data)


# Глобальный экземпляр парсера
data_parser = UnifiedDataParser()


# Удобные функции для быстрого доступа
def parse_price(price_text: str) -> dict:
    """Быстрый парсинг цены"""
    return ServiceParser().parse_price(price_text)


def parse_phone(phone_text: str) -> str:
    """Быстрый парсинг телефона"""
    return ContactParser().parse_phone(phone_text)


def parse_date(date_text: str) -> str:
    """Быстрый парсинг даты"""
    return ReviewParser().parse_date(date_text)


def clean_text(text: str) -> str:
    """Быстрая очистка текста"""
    return BaseParser().clean_text(text)
