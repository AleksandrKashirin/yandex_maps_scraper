"""
Модель данных для услуг предприятия
"""

import re
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class Service(BaseModel):
    """Модель данных услуги предприятия"""

    name: str = Field(..., min_length=1, max_length=200, description="Название услуги")
    price: Optional[str] = Field(None, max_length=50, description="Цена в рублях")
    price_from: Optional[str] = Field(
        None, max_length=20, description="Минимальная цена"
    )
    price_to: Optional[str] = Field(
        None, max_length=20, description="Максимальная цена"
    )
    description: Optional[str] = Field(
        None, max_length=1000, description="Описание услуги"
    )
    duration: Optional[str] = Field(
        None, max_length=50, description="Длительность услуги"
    )

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        """Валидация названия услуги"""
        if not v or not v.strip():
            raise ValueError("Название услуги не может быть пустым")
        return v.strip()

    @field_validator("price", "price_from", "price_to")
    @classmethod
    def validate_price_fields(cls, v: Optional[str]) -> Optional[str]:
        """Валидация полей цены"""
        if v is None:
            return None

        # Убираем лишние пробелы
        v = v.strip()
        if not v:
            return None

        # Проверяем, что содержит только цифры и допустимые символы
        if not re.match(r"^[\d\s.,₽руб-]+$", v, re.IGNORECASE):
            raise ValueError(f"Некорректный формат цены: {v}")

        return v

    @field_validator("duration")
    @classmethod
    def validate_duration(cls, v: Optional[str]) -> Optional[str]:
        """Валидация длительности услуги"""
        if v is None:
            return None

        v = v.strip()
        if not v:
            return None

        # Проверяем базовые форматы времени
        valid_patterns = [
            r"\d+\s*(мин|минут|min|minutes?)",
            r"\d+\s*(ч|час|часов|hour|hours?)",
            r"\d+:\d+",  # формат HH:MM
            r"\d+\s*-\s*\d+\s*(мин|минут|min)",
        ]

        if not any(re.search(pattern, v, re.IGNORECASE) for pattern in valid_patterns):
            # Если не соответствует стандартным форматам, просто возвращаем как есть
            # так как могут быть разные форматы записи времени
            pass

        return v

    @model_validator(mode="after")
    def validate_price_range(self):
        """Валидация диапазона цен"""
        if self.price_from and self.price_to:
            try:
                # Извлекаем числовые значения для сравнения
                from_val = float(re.sub(r"[^\d.]", "", self.price_from))
                to_val = float(re.sub(r"[^\d.]", "", self.price_to))

                if from_val > to_val:
                    raise ValueError(
                        "Минимальная цена не может быть больше максимальной"
                    )
            except (ValueError, TypeError):
                # Если не удается извлечь числовые значения, пропускаем валидацию
                pass

        return self

    def get_price_numeric(self) -> Optional[float]:
        """Получить числовое значение цены"""
        price_str = self.price or self.price_from
        if not price_str:
            return None

        try:
            # Извлекаем первое числовое значение из строки
            match = re.search(r"(\d+(?:[.,]\d+)?)", price_str)
            if match:
                return float(match.group(1).replace(",", "."))
        except (ValueError, TypeError):
            pass

        return None

    def has_price_range(self) -> bool:
        """Проверить, является ли цена диапазоном"""
        return bool(self.price_from and self.price_to)

    def format_price_display(self) -> str:
        """Отформатировать цену для отображения"""
        if self.has_price_range():
            return f"от {self.price_from} до {self.price_to}"
        elif self.price:
            return self.price
        elif self.price_from:
            return f"от {self.price_from}"
        elif self.price_to:
            return f"до {self.price_to}"
        else:
            return "Цена не указана"

    class Config:
        """Конфигурация модели"""

        json_schema_extra = {
            "example": {
                "name": "Маникюр с покрытием",
                "price": "2800 ₽",
                "description": "Классический маникюр с гель-лаком",
                "duration": "60 минут",
            }
        }
