"""
Парсер для извлечения графика работы предприятий
"""

import re
from datetime import datetime, time
from typing import Dict, List, Optional, Tuple

from .base_parser import BaseParser, ParseResult, TextNormalizer


class ScheduleParser(BaseParser):
    """Парсер для графика работы предприятий"""

    # Паттерны для времени работы
    TIME_PATTERNS = [
        # Полный формат времени
        r"(\d{1,2}):(\d{2})\s*[-–—]\s*(\d{1,2}):(\d{2})",
        # Упрощенный формат
        r"(\d{1,2})\s*[-–—]\s*(\d{1,2})",
        # С указанием "с" и "до"
        r"с\s*(\d{1,2}):?(\d{0,2})\s*до\s*(\d{1,2}):?(\d{0,2})",
        # Круглосуточно
        r"круглосуточно|24/7|24\s*часа|всегда\s*открыт",
        # Выходной
        r"выходной|закрыт|не\s*работа|closed",
    ]

    # Дни недели
    WEEKDAYS = {
        # Полные названия
        "понедельник": "monday",
        "вторник": "tuesday",
        "среда": "wednesday",
        "четверг": "thursday",
        "пятница": "friday",
        "суббота": "saturday",
        "воскресенье": "sunday",
        # Сокращения
        "пн": "monday",
        "вт": "tuesday",
        "ср": "wednesday",
        "чт": "thursday",
        "пт": "friday",
        "сб": "saturday",
        "вс": "sunday",
        # Английские названия
        "monday": "monday",
        "tuesday": "tuesday",
        "wednesday": "wednesday",
        "thursday": "thursday",
        "friday": "friday",
        "saturday": "saturday",
        "sunday": "sunday",
        # Английские сокращения
        "mon": "monday",
        "tue": "tuesday",
        "wed": "wednesday",
        "thu": "thursday",
        "fri": "friday",
        "sat": "saturday",
        "sun": "sunday",
    }

    # Паттерны для определения статуса работы
    STATUS_PATTERNS = [
        (r"открыт[оа]?\s*до\s*(\d{1,2}):?(\d{0,2})", "open_until"),
        (r"открыт[оа]?\s*с\s*(\d{1,2}):?(\d{0,2})", "open_from"),
        (r"закрыт[оа]?\s*до\s*(\d{1,2}):?(\d{0,2})", "closed_until"),
        (r"работает\s*до\s*(\d{1,2}):?(\d{0,2})", "working_until"),
        (r"открыт[оа]?", "open"),
        (r"закрыт[оа]?", "closed"),
        (r"круглосуточно|24/7", "always_open"),
    ]

    def parse(self, raw_data: str) -> ParseResult:
        """
        Основной метод парсинга расписания

        Args:
            raw_data: Сырые данные о расписании

        Returns:
            ParseResult: Результат парсинга
        """
        if not raw_data:
            return self.create_result({}, success=False, confidence=0.0)

        try:
            # Очищаем входные данные
            clean_data = self.clean_text(raw_data)

            schedule_data = {"current_status": None, "schedule": {}, "notes": None}

            confidence = 0.0

            # Парсим текущий статус
            status_result = self._parse_current_status(clean_data)
            if status_result:
                schedule_data["current_status"] = status_result
                confidence += 0.3

            # Парсим еженедельное расписание
            weekly_schedule = self._parse_weekly_schedule(clean_data)
            if weekly_schedule:
                schedule_data["schedule"] = weekly_schedule
                confidence += 0.5

            # Извлекаем заметки
            notes = self._extract_notes(clean_data)
            if notes:
                schedule_data["notes"] = notes
                confidence += 0.2

            return self.create_result(
                data=schedule_data,
                success=confidence > 0.3,
                confidence=min(confidence, 1.0),
            )

        except Exception as e:
            result = self.create_result({}, success=False, confidence=0.0)
            result.add_error(f"Ошибка парсинга расписания: {str(e)}")
            return result

    def _parse_current_status(self, text: str) -> Optional[str]:
        """Парсинг текущего статуса работы"""

        if not text:
            return None

        text_lower = text.lower()

        # Ищем паттерны статуса
        for pattern, status_type in self.STATUS_PATTERNS:
            match = re.search(pattern, text_lower)
            if match:
                if status_type == "open_until":
                    hour = match.group(1)
                    minute = match.group(2) if match.group(2) else "00"
                    return f"Открыто до {hour}:{minute.zfill(2)}"

                elif status_type == "open_from":
                    hour = match.group(1)
                    minute = match.group(2) if match.group(2) else "00"
                    return f"Открыто с {hour}:{minute.zfill(2)}"

                elif status_type == "closed_until":
                    hour = match.group(1)
                    minute = match.group(2) if match.group(2) else "00"
                    return f"Закрыто до {hour}:{minute.zfill(2)}"

                elif status_type == "working_until":
                    hour = match.group(1)
                    minute = match.group(2) if match.group(2) else "00"
                    return f"Работает до {hour}:{minute.zfill(2)}"

                elif status_type == "open":
                    return "Открыто"

                elif status_type == "closed":
                    return "Закрыто"

                elif status_type == "always_open":
                    return "Круглосуточно"

        return None

    def _parse_weekly_schedule(self, text: str) -> Dict[str, str]:
        """Парсинг еженедельного расписания"""

        schedule = {}

        if not text:
            return schedule

        # Разделяем текст на строки
        lines = text.split("\n")

        # Пробуем различные форматы расписания

        # Формат 1: Каждый день на отдельной строке
        for line in lines:
            day_schedule = self._parse_daily_schedule(line)
            if day_schedule:
                schedule.update(day_schedule)

        # Формат 2: Диапазоны дней
        range_schedule = self._parse_day_ranges(text)
        if range_schedule:
            schedule.update(range_schedule)

        # Формат 3: Компактный формат в одной строке
        compact_schedule = self._parse_compact_schedule(text)
        if compact_schedule:
            schedule.update(compact_schedule)

        return schedule

    def _parse_daily_schedule(self, line: str) -> Dict[str, str]:
        """Парсинг расписания для одного дня"""

        if not line:
            return {}

        line_lower = line.lower().strip()

        # Ищем день недели в строке
        found_day = None
        for day_name, day_key in self.WEEKDAYS.items():
            if day_name in line_lower:
                found_day = day_key
                break

        if not found_day:
            return {}

        # Извлекаем время работы
        working_hours = self._extract_working_hours(line)

        if working_hours:
            return {found_day: working_hours}

        return {}

    def _parse_day_ranges(self, text: str) -> Dict[str, str]:
        """Парсинг диапазонов дней (например, пн-пт)"""

        schedule = {}
        text_lower = text.lower()

        # Паттерны для диапазонов дней
        range_patterns = [
            r"(пн|понедельник)\s*[-–—]\s*(пт|пятница)\s*:?\s*([^,\n]+)",
            r"(сб|суббота)\s*[-–—]\s*(вс|воскресенье)\s*:?\s*([^,\n]+)",
            r"будни\s*:?\s*([^,\n]+)",
            r"выходные\s*:?\s*([^,\n]+)",
            r"рабочие\s*дни\s*:?\s*([^,\n]+)",
        ]

        for pattern in range_patterns:
            matches = re.finditer(pattern, text_lower)
            for match in matches:
                groups = match.groups()

                if (
                    "пн" in pattern
                    or "понедельник" in pattern
                    or "будни" in pattern
                    or "рабочие" in pattern
                ):
                    # Рабочие дни (пн-пт)
                    working_hours = self._extract_working_hours(groups[-1])
                    if working_hours:
                        for day in [
                            "monday",
                            "tuesday",
                            "wednesday",
                            "thursday",
                            "friday",
                        ]:
                            schedule[day] = working_hours

                elif "сб" in pattern or "суббота" in pattern or "выходные" in pattern:
                    # Выходные дни (сб-вс)
                    working_hours = self._extract_working_hours(groups[-1])
                    if working_hours:
                        for day in ["saturday", "sunday"]:
                            schedule[day] = working_hours

        return schedule

    def _parse_compact_schedule(self, text: str) -> Dict[str, str]:
        """Парсинг компактного формата расписания"""

        schedule = {}

        # Ищем паттерны типа "Пн-Пт 9:00-18:00, Сб 10:00-16:00, Вс выходной"
        compact_pattern = r"([а-яa-z\-]+)\s+([^,]+)"

        matches = re.finditer(compact_pattern, text.lower(), re.UNICODE)

        for match in matches:
            day_part = match.group(1).strip()
            time_part = match.group(2).strip()

            # Определяем дни
            days = self._parse_day_specification(day_part)

            # Извлекаем время
            working_hours = self._extract_working_hours(time_part)

            if days and working_hours:
                for day in days:
                    schedule[day] = working_hours

        return schedule

    def _parse_day_specification(self, day_spec: str) -> List[str]:
        """Парсинг спецификации дней"""

        days = []

        # Проверяем диапазоны
        if "-" in day_spec or "–" in day_spec or "—" in day_spec:
            parts = re.split(r"[-–—]", day_spec)
            if len(parts) == 2:
                start_day = self._normalize_day_name(parts[0].strip())
                end_day = self._normalize_day_name(parts[1].strip())

                if start_day and end_day:
                    days = self._get_day_range(start_day, end_day)

        # Проверяем отдельные дни
        else:
            day = self._normalize_day_name(day_spec)
            if day:
                days = [day]

        return days

    def _normalize_day_name(self, day_name: str) -> Optional[str]:
        """Нормализация названия дня недели"""

        if not day_name:
            return None

        day_lower = day_name.lower().strip()
        return self.WEEKDAYS.get(day_lower)

    def _get_day_range(self, start_day: str, end_day: str) -> List[str]:
        """Получение диапазона дней"""

        day_order = [
            "monday",
            "tuesday",
            "wednesday",
            "thursday",
            "friday",
            "saturday",
            "sunday",
        ]

        try:
            start_index = day_order.index(start_day)
            end_index = day_order.index(end_day)

            if start_index <= end_index:
                return day_order[start_index : end_index + 1]
            else:
                # Диапазон через неделю (например, сб-пн)
                return day_order[start_index:] + day_order[: end_index + 1]

        except ValueError:
            return []

    def _extract_working_hours(self, time_text: str) -> Optional[str]:
        """Извлечение времени работы из текста"""

        if not time_text:
            return None

        time_text = time_text.strip()

        # Проверяем специальные случаи
        if re.search(r"выходной|закрыт|не\s*работа", time_text, re.IGNORECASE):
            return "Выходной"

        if re.search(r"круглосуточно|24/7|24\s*часа", time_text, re.IGNORECASE):
            return "Круглосуточно"

        # Ищем время в различных форматах
        time_patterns = [
            r"(\d{1,2}):(\d{2})\s*[-–—]\s*(\d{1,2}):(\d{2})",  # 09:00-18:00
            r"(\d{1,2})\s*[-–—]\s*(\d{1,2})",  # 9-18
            r"с\s*(\d{1,2}):?(\d{0,2})\s*до\s*(\d{1,2}):?(\d{0,2})",  # с 9 до 18
        ]

        for pattern in time_patterns:
            match = re.search(pattern, time_text)
            if match:
                groups = match.groups()

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
                try:
                    start_h, start_m = int(start_h), int(start_m)
                    end_h, end_m = int(end_h), int(end_m)

                    if (
                        0 <= start_h <= 23
                        and 0 <= start_m <= 59
                        and 0 <= end_h <= 23
                        and 0 <= end_m <= 59
                    ):
                        return f"{start_h:02d}:{start_m:02d}-{end_h:02d}:{end_m:02d}"

                except ValueError:
                    continue

        return None

    def _extract_notes(self, text: str) -> Optional[str]:
        """Извлечение заметок о графике работы"""

        if not text:
            return None

        # Ключевые фразы для заметок
        note_indicators = [
            "примечание",
            "внимание",
            "обратите внимание",
            "важно",
            "уточнение",
            "дополнительно",
            "в праздничные дни",
            "в праздники",
            "летний режим",
            "зимний режим",
            "может изменяться",
            "уточняйте",
        ]

        text_lower = text.lower()

        # Ищем предложения с ключевыми фразами
        sentences = re.split(r"[.!?]", text)

        notes = []
        for sentence in sentences:
            sentence_stripped = sentence.strip()
            sentence_lower = sentence_stripped.lower()

            if any(indicator in sentence_lower for indicator in note_indicators):
                if len(sentence_stripped) > 10:
                    notes.append(sentence_stripped)

        if notes:
            return ". ".join(notes[:3])  # Ограничиваем количество заметок

        return None

    def validate_schedule_consistency(self, schedule: Dict[str, str]) -> Dict[str, any]:
        """Валидация согласованности расписания"""

        validation = {
            "is_valid": True,
            "warnings": [],
            "working_days": 0,
            "total_hours": 0,
        }

        if not schedule:
            return validation

        for day, hours in schedule.items():
            if hours.lower() not in ["выходной", "закрыт"]:
                validation["working_days"] += 1

                # Подсчитываем часы работы
                if hours.lower() == "круглосуточно":
                    validation["total_hours"] += 24
                else:
                    daily_hours = self._calculate_daily_hours(hours)
                    if daily_hours:
                        validation["total_hours"] += daily_hours

        # Проверяем разумность
        if validation["working_days"] == 0:
            validation["warnings"].append("Нет рабочих дней")

        if validation["total_hours"] > 168:  # Больше недели
            validation["warnings"].append("Слишком много рабочих часов в неделю")

        if validation["warnings"]:
            validation["is_valid"] = False

        return validation

    def _calculate_daily_hours(self, hours_str: str) -> Optional[float]:
        """Подсчет количества часов в дне"""

        if not hours_str:
            return None

        # Ищем паттерн времени
        match = re.search(r"(\d{1,2}):(\d{2})-(\d{1,2}):(\d{2})", hours_str)
        if match:
            try:
                start_h, start_m, end_h, end_m = map(int, match.groups())

                start_time = time(start_h, start_m)
                end_time = time(end_h, end_m)

                # Вычисляем разность
                start_minutes = start_h * 60 + start_m
                end_minutes = end_h * 60 + end_m

                if end_minutes > start_minutes:
                    return (end_minutes - start_minutes) / 60.0
                else:
                    # Работа через полночь
                    return (24 * 60 - start_minutes + end_minutes) / 60.0

            except ValueError:
                pass

        return None

    def get_current_day_status(self, schedule: Dict[str, str]) -> Optional[str]:
        """Получение статуса работы на текущий день"""

        if not schedule:
            return None

        # Определяем текущий день недели
        current_day = datetime.now().strftime("%A").lower()

        # Преобразуем английское название в наш ключ
        day_mapping = {
            "monday": "monday",
            "tuesday": "tuesday",
            "wednesday": "wednesday",
            "thursday": "thursday",
            "friday": "friday",
            "saturday": "saturday",
            "sunday": "sunday",
        }

        day_key = day_mapping.get(current_day)

        if day_key and day_key in schedule:
            return schedule[day_key]

        return None
