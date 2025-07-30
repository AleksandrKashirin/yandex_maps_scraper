"""
Модель данных для графика работы предприятия
"""

import re
from datetime import datetime, time
from typing import ClassVar, Dict, List, Optional, Tuple

from pydantic import BaseModel, Field, field_validator, model_validator


class WorkingHours(BaseModel):
    """Модель данных графика работы предприятия"""

    current_status: Optional[str] = Field(
        None, max_length=100, description="Текущий статус работы"
    )
    schedule: Optional[Dict[str, str]] = Field(
        None, description="График работы по дням недели"
    )
    notes: Optional[str] = Field(
        None, max_length=500, description="Особые условия работы"
    )

    # Стандартные дни недели - помечаем как ClassVar
    WEEKDAYS: ClassVar[Dict[str, str]] = {
        "monday": "понедельник",
        "tuesday": "вторник",
        "wednesday": "среда",
        "thursday": "четверг",
        "friday": "пятница",
        "saturday": "суббота",
        "sunday": "воскресенье",
    }

    # Альтернативные названия дней недели - помечаем как ClassVar
    WEEKDAY_ALIASES: ClassVar[Dict[str, str]] = {
        "пн": "monday",
        "понедельник": "monday",
        "пон": "monday",
        "вт": "tuesday",
        "вторник": "tuesday",
        "втор": "tuesday",
        "ср": "wednesday",
        "среда": "wednesday",
        "сред": "wednesday",
        "чт": "thursday",
        "четверг": "thursday",
        "четв": "thursday",
        "пт": "friday",
        "пятница": "friday",
        "пят": "friday",
        "сб": "saturday",
        "суббота": "saturday",
        "суб": "saturday",
        "вс": "sunday",
        "воскресенье": "sunday",
        "воск": "sunday",
    }

    @field_validator("current_status")
    @classmethod
    def validate_current_status(cls, v: Optional[str]) -> Optional[str]:
        """Валидация текущего статуса работы"""
        if not v:
            return None

        v = v.strip()
        if not v:
            return None

        # Нормализуем статус
        v = re.sub(r"\s+", " ", v)  # Убираем лишние пробелы

        return v

    @field_validator("schedule")
    @classmethod
    def validate_schedule(cls, v: Optional[Dict[str, str]]) -> Optional[Dict[str, str]]:
        """Валидация расписания работы"""
        if not v:
            return None

        normalized_schedule = {}

        for day, hours in v.items():
            if not day or not isinstance(hours, str):
                continue

            # Нормализуем название дня
            normalized_day = cls._normalize_weekday(day.lower().strip())
            if not normalized_day:
                continue

            # Нормализуем время работы
            normalized_hours = cls._normalize_working_hours(hours.strip())
            if normalized_hours:
                normalized_schedule[normalized_day] = normalized_hours

        return normalized_schedule if normalized_schedule else None

    @classmethod
    def _normalize_weekday(cls, day: str) -> Optional[str]:
        """Нормализация названия дня недели"""
        day = day.lower().strip()

        # Прямое соответствие
        if day in cls.WEEKDAYS:
            return day

        # Поиск по алиасам
        if day in cls.WEEKDAY_ALIASES:
            return cls.WEEKDAY_ALIASES[day]

        # Поиск по частичному совпадению
        for alias, standard in cls.WEEKDAY_ALIASES.items():
            if day.startswith(alias) or alias.startswith(day):
                return standard

        return None

    @classmethod
    def _normalize_working_hours(cls, hours: str) -> Optional[str]:
        """Нормализация времени работы"""
        if not hours:
            return None

        hours = hours.strip()
        if not hours:
            return None

        # Стандартные значения
        closed_patterns = ["выходной", "закрыт", "не работает", "closed"]
        if any(pattern in hours.lower() for pattern in closed_patterns):
            return "Выходной"

        # Круглосуточно
        if any(
            pattern in hours.lower()
            for pattern in ["24", "круглосуточно", "24/7", "всегда"]
        ):
            return "Круглосуточно"

        # Паттерны времени
        time_patterns = [
            r"(\d{1,2}):(\d{2})\s*[-–—]\s*(\d{1,2}):(\d{2})",  # 09:00-21:00
            r"(\d{1,2})\s*[-–—]\s*(\d{1,2})",  # 9-21
            r"с\s*(\d{1,2}):?(\d{0,2})\s*до\s*(\d{1,2}):?(\d{0,2})",  # с 9 до 21
            r"от\s*(\d{1,2}):?(\d{0,2})\s*до\s*(\d{1,2}):?(\d{0,2})",  # от 9:00 до 21:00
        ]

        for pattern in time_patterns:
            match = re.search(pattern, hours)
            if match:
                groups = match.groups()
                try:
                    if len(groups) == 4:  # Полный формат с минутами
                        start_h, start_m, end_h, end_m = groups
                        start_m = start_m or "00"
                        end_m = end_m or "00"
                    elif len(groups) == 2:  # Только часы
                        start_h, end_h = groups
                        start_m = end_m = "00"
                    else:
                        continue

                    # Валидация времени
                    start_h, start_m = int(start_h), int(start_m)
                    end_h, end_m = int(end_h), int(end_m)

                    if (
                        0 <= start_h <= 23
                        and 0 <= start_m <= 59
                        and 0 <= end_h <= 23
                        and 0 <= end_m <= 59
                    ):
                        return f"{start_h:02d}:{start_m:02d}-{end_h:02d}:{end_m:02d}"

                except (ValueError, TypeError):
                    continue

        # Если не удалось распарсить, возвращаем как есть
        return hours

    @field_validator("notes")
    @classmethod
    def validate_notes(cls, v: Optional[str]) -> Optional[str]:
        """Валидация заметок о работе"""
        if not v:
            return None

        v = v.strip()
        if not v:
            return None

        # Нормализуем пробелы
        v = re.sub(r"\s+", " ", v)

        return v

    def is_open_now(self) -> Optional[bool]:
        """Проверить, открыто ли сейчас (на основе расписания)"""
        if not self.schedule:
            return None

        now = datetime.now()
        current_weekday = now.strftime("%A").lower()

        # Преобразуем английские названия дней в наши ключи
        english_to_our = {
            "monday": "monday",
            "tuesday": "tuesday",
            "wednesday": "wednesday",
            "thursday": "thursday",
            "friday": "friday",
            "saturday": "saturday",
            "sunday": "sunday",
        }

        weekday_key = english_to_our.get(current_weekday)
        if not weekday_key or weekday_key not in self.schedule:
            return None

        today_hours = self.schedule[weekday_key]

        if today_hours.lower() in ["выходной", "закрыт", "не работает"]:
            return False

        if today_hours.lower() in ["круглосуточно", "24/7"]:
            return True

        # Парсим время работы
        time_match = re.search(r"(\d{1,2}):(\d{2})-(\d{1,2}):(\d{2})", today_hours)
        if time_match:
            try:
                start_h, start_m, end_h, end_m = map(int, time_match.groups())
                start_time = time(start_h, start_m)
                end_time = time(end_h, end_m)
                current_time = now.time()

                if start_time <= end_time:
                    # Обычный день (не переходит через полночь)
                    return start_time <= current_time <= end_time
                else:
                    # Работа через полночь
                    return current_time >= start_time or current_time <= end_time

            except (ValueError, TypeError):
                pass

        return None

    def get_working_days_count(self) -> int:
        """Получить количество рабочих дней в неделю"""
        if not self.schedule:
            return 0

        working_days = 0
        for hours in self.schedule.values():
            if hours.lower() not in ["выходной", "закрыт", "не работает"]:
                working_days += 1

        return working_days

    def get_working_hours_today(self) -> Optional[str]:
        """Получить часы работы на сегодня"""
        if not self.schedule:
            return None

        now = datetime.now()
        current_weekday = now.strftime("%A").lower()

        # Преобразуем английские названия дней
        english_to_our = {
            "monday": "monday",
            "tuesday": "tuesday",
            "wednesday": "wednesday",
            "thursday": "thursday",
            "friday": "friday",
            "saturday": "saturday",
            "sunday": "sunday",
        }

        weekday_key = english_to_our.get(current_weekday)
        if weekday_key and weekday_key in self.schedule:
            return self.schedule[weekday_key]

        return None

    def format_schedule_display(self) -> List[str]:
        """Отформатировать расписание для отображения"""
        if not self.schedule:
            return []

        display_lines = []

        # Порядок дней недели
        day_order = [
            "monday",
            "tuesday",
            "wednesday",
            "thursday",
            "friday",
            "saturday",
            "sunday",
        ]

        for day in day_order:
            if day in self.schedule:
                day_name = self.WEEKDAYS[day].capitalize()
                hours = self.schedule[day]
                display_lines.append(f"{day_name}: {hours}")

        return display_lines

    class Config:
        """Конфигурация модели"""

        json_schema_extra = {
            "example": {
                "current_status": "Открыто до 21:00",
                "schedule": {
                    "monday": "09:00-21:00",
                    "tuesday": "09:00-21:00",
                    "wednesday": "09:00-21:00",
                    "thursday": "09:00-21:00",
                    "friday": "09:00-21:00",
                    "saturday": "10:00-20:00",
                    "sunday": "Выходной",
                },
                "notes": "В праздничные дни график может изменяться",
            }
        }
