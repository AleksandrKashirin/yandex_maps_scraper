"""
Модуль скрапинга Яндекс.Карт
"""

from .base_scrapper import BusinessData, ReviewData, ServiceData, YandexMapsScraper
from .navigation import YandexMapsNavigator
from .selectors import YandexMapsSelectors

# Создаем экземпляр селекторов для использования
selectors = YandexMapsSelectors()

__all__ = [
    "YandexMapsScraper",
    "BusinessData",
    "ServiceData",
    "ReviewData",
    "YandexMapsNavigator",
    "selectors",
    "YandexMapsSelectors",
]
