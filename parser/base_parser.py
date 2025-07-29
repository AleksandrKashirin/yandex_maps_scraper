"""
Базовый парсер с общей функциональностью для всех типов данных
"""

import re
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel


class ParseResult(BaseModel):
    """Результат парсинга"""

    success: bool = True
    data: Any = None
    errors: List[str] = []
    warnings: List[str] = []
    confidence: float = 1.0  # Уверенность в результате (0.0 - 1.0)

    def add_error(self, error: str):
        """Добавить ошибку"""
        self.errors.append(error)
        self.success = False

    def add_warning(self, warning: str):
        """Добавить предупреждение"""
        self.warnings.append(warning)
        if self.confidence > 0.7:
            self.confidence = 0.7


class BaseParser(ABC):
    """Базовый класс для всех парсеров"""

    # Регулярные выражения для общих задач
    HTML_TAG_PATTERN = re.compile(r"<[^>]+>")
    WHITESPACE_PATTERN = re.compile(r"\s+")
    SPECIAL_CHARS_PATTERN = re.compile(r'[^\w\s\-.,!?;:()\[\]"]', re.UNICODE)

    # Паттерны для определения языка
    CYRILLIC_PATTERN = re.compile(r"[а-яё]", re.IGNORECASE)
    LATIN_PATTERN = re.compile(r"[a-z]", re.IGNORECASE)

    def __init__(self):
        self.debug_mode = False

    @abstractmethod
    def parse(self, raw_data: Any) -> ParseResult:
        """Основной метод парсинга (должен быть переопределен в дочерних классах)"""
        pass

    def clean_text(
        self,
        text: str,
        remove_html: bool = True,
        normalize_whitespace: bool = True,
        remove_special_chars: bool = False,
    ) -> str:
        """
        Очистка текста от различных артефактов

        Args:
            text: Исходный текст
            remove_html: Удалить HTML теги
            normalize_whitespace: Нормализовать пробелы
            remove_special_chars: Удалить специальные символы

        Returns:
            str: Очищенный текст
        """
        if not text or not isinstance(text, str):
            return ""

        result = text.strip()

        if remove_html:
            # Удаляем HTML теги
            result = self.HTML_TAG_PATTERN.sub("", result)

        if normalize_whitespace:
            # Нормализуем пробелы
            result = self.WHITESPACE_PATTERN.sub(" ", result)
            # Убираем множественные переносы строк
            result = re.sub(r"\n\s*\n", "\n\n", result)

        if remove_special_chars:
            # Убираем специальные символы (осторожно с Unicode)
            result = self.SPECIAL_CHARS_PATTERN.sub("", result)

        return result.strip()

    def detect_language(self, text: str) -> str:
        """
        Определение языка текста

        Args:
            text: Текст для анализа

        Returns:
            str: 'ru', 'en', 'mixed' или 'unknown'
        """
        if not text:
            return "unknown"

        cyrillic_count = len(self.CYRILLIC_PATTERN.findall(text))
        latin_count = len(self.LATIN_PATTERN.findall(text))
        total_letters = cyrillic_count + latin_count

        if total_letters == 0:
            return "unknown"

        cyrillic_ratio = cyrillic_count / total_letters

        if cyrillic_ratio > 0.7:
            return "ru"
        elif cyrillic_ratio < 0.3:
            return "en"
        else:
            return "mixed"

    def normalize_currency(self, text: str) -> str:
        """
        Нормализация валютных символов

        Args:
            text: Текст с возможными валютными символами

        Returns:
            str: Нормализованный текст
        """
        if not text:
            return ""

        # Словарь замен валютных символов
        currency_replacements = {
            "₽": "руб",
            "₨": "руб",
            "рублей": "руб",
            "рубля": "руб",
            "p.": "руб",
            "р.": "руб",
            "$": "долл",
            "€": "евро",
            "£": "фунт",
        }

        result = text
        for old, new in currency_replacements.items():
            result = result.replace(old, new)

        return result

    def extract_numbers(self, text: str) -> List[float]:
        """
        Извлечение всех чисел из текста

        Args:
            text: Исходный текст

        Returns:
            List[float]: Список найденных чисел
        """
        if not text:
            return []

        # Паттерн для поиска чисел (включая дробные)
        number_pattern = re.compile(r"\d+(?:[.,]\d+)?")
        matches = number_pattern.findall(text)

        numbers = []
        for match in matches:
            try:
                # Заменяем запятую на точку для дробных чисел
                normalized = match.replace(",", ".")
                numbers.append(float(normalized))
            except ValueError:
                continue

        return numbers

    def validate_text_quality(self, text: str) -> Dict[str, Any]:
        """
        Оценка качества текста

        Args:
            text: Текст для анализа

        Returns:
            Dict: Метрики качества текста
        """
        if not text:
            return {
                "length": 0,
                "has_content": False,
                "language": "unknown",
                "quality_score": 0.0,
            }

        clean_text = self.clean_text(text)

        metrics = {
            "length": len(clean_text),
            "has_content": len(clean_text.strip()) > 0,
            "language": self.detect_language(clean_text),
            "word_count": len(clean_text.split()),
            "sentence_count": len(re.split(r"[.!?]+", clean_text)),
            "special_chars_ratio": (
                len(re.sub(r"[\w\s]", "", text)) / len(text) if text else 0
            ),
        }

        # Вычисляем общий балл качества
        quality_score = 0.0

        # Проверяем длину
        if 10 <= metrics["length"] <= 1000:
            quality_score += 0.3
        elif metrics["length"] > 0:
            quality_score += 0.1

        # Проверяем наличие содержимого
        if metrics["has_content"]:
            quality_score += 0.2

        # Проверяем определимость языка
        if metrics["language"] in ["ru", "en"]:
            quality_score += 0.2
        elif metrics["language"] == "mixed":
            quality_score += 0.1

        # Проверяем разумность количества слов
        if 2 <= metrics["word_count"] <= 200:
            quality_score += 0.2
        elif metrics["word_count"] > 0:
            quality_score += 0.1

        # Проверяем количество специальных символов
        if metrics["special_chars_ratio"] < 0.2:
            quality_score += 0.1

        metrics["quality_score"] = min(quality_score, 1.0)

        return metrics

    def split_compound_text(self, text: str, separators: List[str] = None) -> List[str]:
        """
        Разделение составного текста на части

        Args:
            text: Исходный текст
            separators: Список разделителей

        Returns:
            List[str]: Список частей текста
        """
        if not text:
            return []

        if separators is None:
            separators = [";", ",", "/", "|", "\n", "•", "–", "-"]

        # Создаем паттерн из разделителей
        pattern = "|".join(re.escape(sep) for sep in separators)

        # Разделяем текст
        parts = re.split(pattern, text)

        # Очищаем каждую часть
        cleaned_parts = []
        for part in parts:
            cleaned = self.clean_text(part)
            if cleaned and len(cleaned) > 1:  # Игнорируем слишком короткие части
                cleaned_parts.append(cleaned)

        return cleaned_parts

    def extract_with_context(
        self, text: str, target_pattern: str, context_size: int = 50
    ) -> List[Dict[str, str]]:
        """
        Извлечение данных с контекстом

        Args:
            text: Исходный текст
            target_pattern: Паттерн для поиска
            context_size: Размер контекста в символах

        Returns:
            List[Dict]: Список найденных совпадений с контекстом
        """
        if not text or not target_pattern:
            return []

        results = []

        for match in re.finditer(target_pattern, text, re.IGNORECASE):
            start_pos = max(0, match.start() - context_size)
            end_pos = min(len(text), match.end() + context_size)

            result = {
                "match": match.group(),
                "context": text[start_pos:end_pos],
                "position": match.start(),
                "confidence": 1.0,  # Базовая уверенность
            }

            results.append(result)

        return results

    def fuzzy_match(
        self, text: str, patterns: List[str], threshold: float = 0.8
    ) -> Optional[str]:
        """
        Нечеткое сопоставление текста с паттернами

        Args:
            text: Текст для сопоставления
            patterns: Список паттернов
            threshold: Порог сходства

        Returns:
            Optional[str]: Наиболее подходящий паттерн или None
        """
        if not text or not patterns:
            return None

        text_lower = text.lower().strip()
        best_match = None
        best_score = 0.0

        for pattern in patterns:
            pattern_lower = pattern.lower().strip()

            # Простая метрика сходства на основе общих слов
            text_words = set(text_lower.split())
            pattern_words = set(pattern_lower.split())

            if not pattern_words:
                continue

            intersection = text_words.intersection(pattern_words)
            score = len(intersection) / len(pattern_words)

            if score > best_score and score >= threshold:
                best_score = score
                best_match = pattern

        return best_match

    def create_result(
        self, data: Any = None, success: bool = True, confidence: float = 1.0
    ) -> ParseResult:
        """
        Создание результата парсинга

        Args:
            data: Данные результата
            success: Флаг успеха
            confidence: Уверенность в результате

        Returns:
            ParseResult: Объект результата
        """
        return ParseResult(success=success, data=data, confidence=confidence)

    def log_debug(self, message: str):
        """Отладочное логирование"""
        if self.debug_mode:
            print(f"[DEBUG] {self.__class__.__name__}: {message}")


class TextNormalizer:
    """Специализированный класс для нормализации текста"""

    # Словари для нормализации
    MONTH_NAMES_RU = {
        "января": "01",
        "февраля": "02",
        "марта": "03",
        "апреля": "04",
        "мая": "05",
        "июня": "06",
        "июля": "07",
        "августа": "08",
        "сентября": "09",
        "октября": "10",
        "ноября": "11",
        "декабря": "12",
        "янв": "01",
        "фев": "02",
        "мар": "03",
        "апр": "04",
        "май": "05",
        "июн": "06",
        "июл": "07",
        "авг": "08",
        "сен": "09",
        "окт": "10",
        "ноя": "11",
        "дек": "12",
    }

    MONTH_NAMES_EN = {
        "january": "01",
        "february": "02",
        "march": "03",
        "april": "04",
        "may": "05",
        "june": "06",
        "july": "07",
        "august": "08",
        "september": "09",
        "october": "10",
        "november": "11",
        "december": "12",
        "jan": "01",
        "feb": "02",
        "mar": "03",
        "apr": "04",
        "may": "05",
        "jun": "06",
        "jul": "07",
        "aug": "08",
        "sep": "09",
        "oct": "10",
        "nov": "11",
        "dec": "12",
    }

    WEEKDAY_NAMES_RU = {
        "понедельник": "monday",
        "вторник": "tuesday",
        "среда": "wednesday",
        "четверг": "thursday",
        "пятница": "friday",
        "суббота": "saturday",
        "воскресенье": "sunday",
        "пн": "monday",
        "вт": "tuesday",
        "ср": "wednesday",
        "чт": "thursday",
        "пт": "friday",
        "сб": "saturday",
        "вс": "sunday",
    }

    @classmethod
    def normalize_month_name(
        cls, month_text: str, language: str = "auto"
    ) -> Optional[str]:
        """Нормализация названия месяца"""
        if not month_text:
            return None

        month_lower = month_text.lower().strip()

        if language == "auto":
            # Автоопределение языка
            if any(char in "абвгдеёжзийклмнопрстуфхцчшщъыьэюя" for char in month_lower):
                language = "ru"
            else:
                language = "en"

        if language == "ru":
            return cls.MONTH_NAMES_RU.get(month_lower)
        else:
            return cls.MONTH_NAMES_EN.get(month_lower)

    @classmethod
    def normalize_weekday_name(cls, weekday_text: str) -> Optional[str]:
        """Нормализация названия дня недели"""
        if not weekday_text:
            return None

        weekday_lower = weekday_text.lower().strip()
        return cls.WEEKDAY_NAMES_RU.get(weekday_lower)

    @classmethod
    def normalize_price_text(cls, price_text: str) -> str:
        """Нормализация текста с ценой"""
        if not price_text:
            return ""

        # Убираем лишние пробелы
        normalized = re.sub(r"\s+", " ", price_text.strip())

        # Нормализуем валютные символы
        currency_map = {
            "₽": " руб",
            "рублей": " руб",
            "рубля": " руб",
            "руб.": " руб",
            "р.": " руб",
        }

        for old, new in currency_map.items():
            normalized = normalized.replace(old, new)

        return normalized.strip()
