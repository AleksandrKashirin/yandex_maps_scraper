"""
Модель данных для отзывов о предприятии
"""

import re
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class Review(BaseModel):
    """Модель данных отзыва о предприятии"""

    author: str = Field(
        ..., min_length=1, max_length=100, description="Имя автора отзыва"
    )
    rating: Optional[int] = Field(None, ge=1, le=5, description="Оценка от 1 до 5")
    date: Optional[str] = Field(None, max_length=50, description="Дата отзыва")
    text: Optional[str] = Field(None, max_length=5000, description="Текст отзыва")
    response: Optional[str] = Field(
        None, max_length=3000, description="Ответ владельца"
    )

    @field_validator("author")
    @classmethod
    def validate_author(cls, v: str) -> str:
        """Валидация имени автора"""
        if not v or not v.strip():
            raise ValueError("Имя автора не может быть пустым")

        v = v.strip()

        # Убираем лишние пробелы между словами
        v = re.sub(r"\s+", " ", v)

        # Базовая проверка на подозрительные символы
        if len(re.sub(r"[а-яёa-z\s\-\.]+", "", v, flags=re.IGNORECASE)) > 3:
            # Если слишком много "подозрительных" символов, оставляем как есть
            # но логируем для проверки
            pass

        return v

    @field_validator("rating")
    @classmethod
    def validate_rating(cls, v: Optional[int]) -> Optional[int]:
        """Валидация рейтинга"""
        if v is None:
            return None

        if not isinstance(v, int):
            # Пробуем конвертировать
            try:
                v = int(v)
            except (ValueError, TypeError):
                raise ValueError("Рейтинг должен быть числом от 1 до 5")

        if v < 1 or v > 5:
            raise ValueError("Рейтинг должен быть от 1 до 5")

        return v

    @field_validator("date")
    @classmethod
    def validate_date(cls, v: Optional[str]) -> Optional[str]:
        """Валидация даты отзыва"""
        if not v:
            return None

        v = v.strip()
        if not v:
            return None

        # Нормализуем дату
        v = re.sub(r"\s+", " ", v)

        # Попробуем распознать различные форматы дат
        date_patterns = [
            # Полные даты
            (
                r"(\d{1,2})\s*(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\s*(\d{4})",
                "ru_full",
            ),
            (
                r"(\d{1,2})\s*(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s*(\d{4})",
                "en_full",
            ),
            # Сокращенные даты
            (
                r"(\d{1,2})\s*(янв|фев|мар|апр|мая|июн|июл|авг|сен|окт|ноя|дек)\.?\s*(\d{4})?",
                "ru_short",
            ),
            (
                r"(\d{1,2})\s*(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.?\s*(\d{4})?",
                "en_short",
            ),
            # Числовые форматы
            (r"(\d{1,2})\.(\d{1,2})\.(\d{2,4})", "numeric_dots"),
            (r"(\d{1,2})/(\d{1,2})/(\d{2,4})", "numeric_slash"),
            (r"(\d{4})-(\d{1,2})-(\d{1,2})", "iso_format"),
            # Относительные даты
            (r"(сегодня|вчера|позавчера)", "relative_ru"),
            (r"(today|yesterday)", "relative_en"),
            (r"(\d+)\s*(дн|дня|дней)\s*назад", "days_ago_ru"),
            (r"(\d+)\s*(day|days)\s*ago", "days_ago_en"),
        ]

        # Проверяем паттерны
        for pattern, format_type in date_patterns:
            match = re.search(pattern, v, re.IGNORECASE)
            if match:
                # Для большинства случаев просто возвращаем исходную строку
                # так как формат даты может быть разным в разных локалях
                return v

        # Если ничего не подошло, возвращаем как есть
        return v

    @field_validator("text")
    @classmethod
    def validate_text(cls, v: Optional[str]) -> Optional[str]:
        """Валидация текста отзыва"""
        if not v:
            return None

        v = v.strip()
        if not v:
            return None

        # Нормализуем пробелы и переносы строк
        v = re.sub(r"\s+", " ", v)
        v = re.sub(r"\n\s*\n", "\n\n", v)  # Убираем лишние пустые строки

        # Базовая очистка от HTML-тегов если есть
        v = re.sub(r"<[^>]+>", "", v)

        # Убираем лишние знаки препинания
        v = re.sub(r"[.]{3,}", "...", v)
        v = re.sub(r"[!]{2,}", "!", v)
        v = re.sub(r"[?]{2,}", "?", v)

        return v

    @field_validator("response")
    @classmethod
    def validate_response(cls, v: Optional[str]) -> Optional[str]:
        """Валидация ответа владельца"""
        if not v:
            return None

        v = v.strip()
        if not v:
            return None

        # Применяем ту же нормализацию что и для текста
        v = re.sub(r"\s+", " ", v)
        v = re.sub(r"\n\s*\n", "\n\n", v)
        v = re.sub(r"<[^>]+>", "", v)

        return v

    def get_rating_stars(self) -> str:
        """Получить рейтинг в виде звездочек"""
        if self.rating is None:
            return "Рейтинг не указан"

        full_stars = "★" * self.rating
        empty_stars = "☆" * (5 - self.rating)
        return full_stars + empty_stars

    def is_positive(self) -> Optional[bool]:
        """Определить, является ли отзыв положительным"""
        if self.rating is None:
            return None

        return self.rating >= 4

    def is_negative(self) -> Optional[bool]:
        """Определить, является ли отзыв отрицательным"""
        if self.rating is None:
            return None

        return self.rating <= 2

    def has_owner_response(self) -> bool:
        """Проверить, есть ли ответ владельца"""
        return bool(self.response and self.response.strip())

    def get_text_length(self) -> int:
        """Получить длину текста отзыва"""
        if not self.text:
            return 0
        return len(self.text)

    def get_text_preview(self, max_length: int = 100) -> str:
        """Получить краткий предварительный просмотр текста"""
        if not self.text:
            return ""

        if len(self.text) <= max_length:
            return self.text

        # Обрезаем по словам
        words = self.text.split()
        preview = ""

        for word in words:
            if len(preview + " " + word) > max_length:
                break
            preview += " " + word if preview else word

        return preview + "..." if len(self.text) > len(preview) else preview

    def get_sentiment_score(self) -> Optional[float]:
        """Получить примерную оценку тональности (на основе рейтинга)"""
        if self.rating is None:
            return None

        # Простая линейная шкала от -1 (очень плохо) до 1 (очень хорошо)
        return (self.rating - 3) / 2

    class Config:
        """Конфигурация модели"""

        json_schema_extra = {
            "example": {
                "author": "Анна К.",
                "rating": 5,
                "date": "15 января 2024",
                "text": "Отличный сервис! Мастера профессиональные, результат превзошел ожидания.",
                "response": "Спасибо за отзыв! Рады, что остались довольны.",
            }
        }
