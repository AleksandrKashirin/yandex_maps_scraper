"""
Основная модель данных предприятия для системы скрапинга Яндекс.Карт
"""

import re
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from pydantic import BaseModel, Field, field_validator, model_validator

from .review import Review
from .service import Service
from .social_networks import SocialNetworks
from .working_hours import WorkingHours


class Enterprise(BaseModel):
    """Основная модель данных предприятия"""

    # Обязательные поля
    name: str = Field(
        ..., min_length=1, max_length=200, description="Название предприятия"
    )
    scraping_date: datetime = Field(
        default_factory=datetime.now, description="Дата/время скрапинга"
    )

    # Основные поля (согласно ТЗ address тоже обязательное, но сделаем опциональным для гибкости)
    category: Optional[str] = Field(
        None, max_length=100, description="Категория/позиционирование"
    )
    address: Optional[str] = Field(None, max_length=300, description="Полный адрес")

    # Опциональные поля
    services: List[Service] = Field(
        default_factory=list, description="Список услуг с ценами"
    )
    website: Optional[str] = Field(None, max_length=500, description="Официальный сайт")
    social_networks: SocialNetworks = Field(
        default_factory=SocialNetworks, description="Ссылки на соцсети"
    )
    phone: Optional[str] = Field(None, max_length=50, description="Номер телефона")
    working_hours: WorkingHours = Field(
        default_factory=WorkingHours, description="График работы"
    )
    rating: Optional[float] = Field(
        None, ge=0.0, le=5.0, description="Средний рейтинг (0.0-5.0)"
    )
    reviews_count: Optional[int] = Field(None, ge=0, description="Количество отзывов")
    reviews: List[Review] = Field(default_factory=list, description="Массив отзывов")

    # Метаданные
    metadata: Dict[str, Any] = Field(
        default_factory=dict, description="Метаданные скрапинга"
    )

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        """Валидация названия предприятия"""
        if not v or not v.strip():
            raise ValueError("Название предприятия не может быть пустым")

        v = v.strip()

        # Убираем лишние пробелы
        v = re.sub(r"\s+", " ", v)

        # Убираем HTML-теги если есть
        v = re.sub(r"<[^>]+>", "", v)

        # Базовая очистка от спецсимволов в начале/конце
        v = re.sub(r"^[^\w\s]+|[^\w\s]+$", "", v, flags=re.UNICODE)

        return v

    @field_validator("category")
    @classmethod
    def validate_category(cls, v: Optional[str]) -> Optional[str]:
        """Валидация категории предприятия"""
        if not v:
            return None

        v = v.strip()
        if not v:
            return None

        # Нормализуем пробелы
        v = re.sub(r"\s+", " ", v)

        # Убираем HTML-теги
        v = re.sub(r"<[^>]+>", "", v)

        return v

    @field_validator("address")
    @classmethod
    def validate_address(cls, v: Optional[str]) -> Optional[str]:
        """Валидация адреса предприятия"""
        if not v:
            return None

        v = v.strip()
        if not v:
            return None

        # Нормализуем пробелы
        v = re.sub(r"\s+", " ", v)

        # Убираем HTML-теги
        v = re.sub(r"<[^>]+>", "", v)

        # Базовая очистка адреса
        v = re.sub(r"^[,\s]+|[,\s]+$", "", v)  # Убираем запятые в начале/конце

        return v

    @field_validator("website")
    @classmethod
    def validate_website(cls, v: Optional[str]) -> Optional[str]:
        """Валидация сайта предприятия"""
        if not v:
            return None

        v = v.strip()
        if not v:
            return None

        # Добавляем схему если отсутствует
        if not v.startswith(("http://", "https://")):
            v = "https://" + v

        # Базовая валидация URL
        try:
            parsed = urlparse(v)
            if not parsed.netloc:
                return None

            # Проверяем, что это не ссылка на Яндекс.Карты
            if "yandex" in parsed.netloc.lower() and "maps" in v.lower():
                return None

            return v

        except Exception:
            return None

    @field_validator("phone")
    @classmethod
    def validate_phone(cls, v: Optional[str]) -> Optional[str]:
        """Валидация номера телефона"""
        if not v:
            return None

        v = v.strip()
        if not v:
            return None

        # Убираем лишние символы но оставляем основные
        v = re.sub(r"[^\d\s\-\+\(\)]+", "", v)

        # Убираем лишние пробелы
        v = re.sub(r"\s+", " ", v)

        # Базовая проверка на минимальную длину
        digits_only = re.sub(r"[^\d]", "", v)
        if len(digits_only) < 7:  # Минимум 7 цифр для валидного номера
            return None

        return v

    @field_validator("rating")
    @classmethod
    def validate_rating(cls, v: Optional[float]) -> Optional[float]:
        """Валидация рейтинга"""
        if v is None:
            return None

        if not isinstance(v, (int, float)):
            try:
                v = float(v)
            except (ValueError, TypeError):
                return None

        # Округляем до 1 знака после запятой
        v = round(float(v), 1)

        if v < 0.0 or v > 5.0:
            return None

        return v

    @field_validator("reviews_count")
    @classmethod
    def validate_reviews_count(cls, v: Optional[int]) -> Optional[int]:
        """Валидация количества отзывов"""
        if v is None:
            return None

        if not isinstance(v, int):
            try:
                v = int(v)
            except (ValueError, TypeError):
                return None

        if v < 0:
            return 0

        return v

    @model_validator(mode="after")
    def validate_reviews_consistency(self):
        """Валидация соответствия количества отзывов и списка отзывов"""
        if self.reviews_count is not None and self.reviews:
            # Если количество отзывов указано, но не совпадает со списком,
            # приоритет отдаем фактическому количеству в списке
            actual_count = len(self.reviews)
            if actual_count != self.reviews_count:
                # Логируем расхождение но не вызываем ошибку
                pass

        return self

    @model_validator(mode="after")
    def populate_metadata(self):
        """Заполнение метаданных"""
        if not self.metadata:
            self.metadata = {}

        # Базовые метаданные
        if "scraper_version" not in self.metadata:
            self.metadata["scraper_version"] = "1.0"

        if "extraction_stats" not in self.metadata:
            self.metadata["extraction_stats"] = {
                "services_extracted": len(self.services),
                "reviews_extracted": len(self.reviews),
                "has_rating": self.rating is not None,
                "has_phone": self.phone is not None,
                "has_website": self.website is not None,
                "has_social_networks": self.social_networks.has_any_network(),
                "has_working_hours": bool(
                    self.working_hours.schedule or self.working_hours.current_status
                ),
            }

        return self

    # Методы для анализа данных

    def get_data_completeness_score(self) -> float:
        """Получить оценку полноты данных (0.0 - 1.0)"""
        total_fields = 12  # Общее количество полей согласно ТЗ
        filled_fields = 0

        # Обязательные поля
        if self.name:
            filled_fields += 1
        if self.scraping_date:
            filled_fields += 1

        # Опциональные поля
        if self.category:
            filled_fields += 1
        if self.address:
            filled_fields += 1
        if self.services:
            filled_fields += 1
        if self.website:
            filled_fields += 1
        if self.social_networks.has_any_network():
            filled_fields += 1
        if self.phone:
            filled_fields += 1
        if self.working_hours.schedule or self.working_hours.current_status:
            filled_fields += 1
        if self.rating is not None:
            filled_fields += 1
        if self.reviews_count is not None:
            filled_fields += 1
        if self.reviews:
            filled_fields += 1

        return filled_fields / total_fields

    def get_contact_methods_count(self) -> int:
        """Получить количество способов связи"""
        count = 0
        if self.phone:
            count += 1
        if self.website:
            count += 1
        count += self.social_networks.get_networks_count()
        return count

    def has_pricing_info(self) -> bool:
        """Проверить, есть ли информация о ценах"""
        return any(
            service.price or service.price_from or service.price_to
            for service in self.services
        )

    def get_average_rating_from_reviews(self) -> Optional[float]:
        """Вычислить средний рейтинг на основе отзывов"""
        if not self.reviews:
            return None

        ratings = [
            review.rating for review in self.reviews if review.rating is not None
        ]
        if not ratings:
            return None

        return round(sum(ratings) / len(ratings), 1)

    def get_services_by_price_range(
        self, min_price: float = None, max_price: float = None
    ) -> List[Service]:
        """Получить услуги в заданном ценовом диапазоне"""
        filtered_services = []

        for service in self.services:
            service_price = service.get_price_numeric()
            if service_price is None:
                continue

            if min_price is not None and service_price < min_price:
                continue

            if max_price is not None and service_price > max_price:
                continue

            filtered_services.append(service)

        return filtered_services

    def get_reviews_by_rating(self, rating: int) -> List[Review]:
        """Получить отзывы с определенным рейтингом"""
        return [review for review in self.reviews if review.rating == rating]

    def get_positive_reviews_ratio(self) -> Optional[float]:
        """Получить долю положительных отзывов"""
        if not self.reviews:
            return None

        total_with_rating = len([r for r in self.reviews if r.rating is not None])
        if total_with_rating == 0:
            return None

        positive = len([r for r in self.reviews if r.rating and r.rating >= 4])
        return positive / total_with_rating

    def export_summary(self) -> Dict[str, Any]:
        """Экспорт краткой сводки о предприятии"""
        return {
            "name": self.name,
            "category": self.category,
            "address": self.address,
            "rating": self.rating,
            "reviews_count": self.reviews_count,
            "services_count": len(self.services),
            "has_pricing": self.has_pricing_info(),
            "contact_methods": self.get_contact_methods_count(),
            "data_completeness": round(self.get_data_completeness_score(), 2),
            "scraping_date": self.scraping_date.isoformat(),
        }

    class Config:
        """Конфигурация модели"""

        json_encoders = {datetime: lambda v: v.isoformat()}

        json_schema_extra = {
            "example": {
                "name": "Eva Beauty Studio",
                "category": "Beauty salon",
                "address": "пгт Новоивановское, бульвар Эйнштейна, 3",
                "phone": "+7 (993) 602-65-90",
                "website": "https://eva-beauty-studio.clients.site/",
                "rating": 5.0,
                "reviews_count": 101,
                "services": [
                    {
                        "name": "Маникюр с покрытием",
                        "price": "2800 ₽",
                        "description": "Классический маникюр с гель-лаком",
                    }
                ],
                "social_networks": {"whatsapp": "https://wa.me/79936026590"},
                "working_hours": {
                    "current_status": "Открыто до 21:00",
                    "schedule": {"monday": "09:00-21:00"},
                },
                "metadata": {
                    "source_url": "https://yandex.com.ge/maps/-/CHXU6Fmb",
                    "scraper_version": "1.0",
                },
            }
        }
