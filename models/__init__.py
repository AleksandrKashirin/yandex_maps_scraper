"""
Модели данных для системы скрапинга Яндекс.Карт

Этот модуль содержит все Pydantic модели для валидации и структурирования
данных о предприятиях, извлекаемых с Яндекс.Карт.
"""

from .enterprise import Enterprise
from .review import Review
from .service import Service
from .social_networks import SocialNetworks
from .working_hours import WorkingHours

# Версия схемы данных
SCHEMA_VERSION = "1.0"

__all__ = [
    "Enterprise",
    "Service",
    "Review",
    "SocialNetworks",
    "WorkingHours",
    "SCHEMA_VERSION",
]

# Примеры использования и валидации


def create_sample_enterprise() -> Enterprise:
    """Создать пример предприятия для тестирования"""
    return Enterprise(
        name="Пример Салона Красоты",
        category="Салон красоты",
        address="г. Москва, ул. Примерная, д. 1",
        phone="+7 (999) 123-45-67",
        website="https://example-salon.ru",
        rating=4.5,
        reviews_count=25,
        services=[
            Service(
                name="Стрижка женская",
                price="2000 ₽",
                description="Классическая женская стрижка",
            ),
            Service(
                name="Окрашивание",
                price_from="3000",
                price_to="8000",
                description="Окрашивание волос",
            ),
        ],
        social_networks=SocialNetworks(
            whatsapp="https://wa.me/79991234567",
        ),
        working_hours=WorkingHours(
            current_status="Открыто до 20:00",
            schedule={
                "monday": "09:00-20:00",
                "tuesday": "09:00-20:00",
                "wednesday": "09:00-20:00",
                "thursday": "09:00-20:00",
                "friday": "09:00-20:00",
                "saturday": "10:00-18:00",
                "sunday": "Выходной",
            },
        ),
        reviews=[
            Review(
                author="Анна К.",
                rating=5,
                date="10 января 2024",
                text="Отличный салон! Мастера профессиональные.",
            )
        ],
    )


def validate_enterprise_data(data: dict) -> Enterprise:
    """
    Валидация данных предприятия

    Args:
        data: Словарь с данными предприятия

    Returns:
        Enterprise: Валидированная модель предприятия

    Raises:
        ValidationError: При ошибках валидации
    """
    return Enterprise.model_validate(data)


def export_enterprise_schema() -> dict:
    """Экспорт JSON Schema для модели предприятия"""
    return Enterprise.model_json_schema()
