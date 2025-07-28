"""
Дополнительные валидаторы и утилиты для моделей данных
"""

import re
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse

from pydantic import ValidationError


class DataValidationUtils:
    """Утилиты для валидации данных"""

    # Множества стоп-слов для различных языков
    STOP_WORDS_RU = {
        "и",
        "в",
        "не",
        "на",
        "с",
        "по",
        "для",
        "от",
        "до",
        "при",
        "без",
        "под",
        "над",
    }

    STOP_WORDS_EN = {
        "a",
        "an",
        "and",
        "or",
        "but",
        "in",
        "on",
        "at",
        "to",
        "for",
        "of",
        "with",
        "by",
    }

    # Паттерны для определения языка
    CYRILLIC_PATTERN = re.compile(r"[а-яё]", re.IGNORECASE)
    LATIN_PATTERN = re.compile(r"[a-z]", re.IGNORECASE)

    @classmethod
    def detect_language(cls, text: str) -> str:
        """
        Определить язык текста

        Args:
            text: Текст для анализа

        Returns:
            str: 'ru', 'en' или 'mixed'
        """
        if not text:
            return "unknown"

        cyrillic_count = len(cls.CYRILLIC_PATTERN.findall(text))
        latin_count = len(cls.LATIN_PATTERN.findall(text))

        if cyrillic_count > latin_count:
            return "ru"
        elif latin_count > cyrillic_count:
            return "en"
        else:
            return "mixed"

    @classmethod
    def clean_text(
        cls, text: str, remove_html: bool = True, normalize_spaces: bool = True
    ) -> str:
        """
        Очистка текста

        Args:
            text: Исходный текст
            remove_html: Удалить HTML теги
            normalize_spaces: Нормализовать пробелы

        Returns:
            str: Очищенный текст
        """
        if not text:
            return ""

        result = text.strip()

        if remove_html:
            # Удаляем HTML теги
            result = re.sub(r"<[^>]+>", "", result)

        if normalize_spaces:
            # Нормализуем пробелы
            result = re.sub(r"\s+", " ", result)
            result = re.sub(r"\n\s*\n", "\n\n", result)

        return result.strip()

    @classmethod
    def validate_url_domain(
        cls, url: str, allowed_domains: Optional[List[str]] = None
    ) -> bool:
        """
        Валидация домена URL

        Args:
            url: URL для проверки
            allowed_domains: Список разрешенных доменов

        Returns:
            bool: True если домен валиден
        """
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()

            if not domain:
                return False

            if allowed_domains:
                return any(
                    allowed_domain in domain for allowed_domain in allowed_domains
                )

            return True

        except Exception:
            return False

    @classmethod
    def extract_phone_digits(cls, phone: str) -> str:
        """
        Извлечь только цифры из номера телефона

        Args:
            phone: Номер телефона

        Returns:
            str: Только цифры
        """
        if not phone:
            return ""
        return re.sub(r"[^\d]", "", phone)

    @classmethod
    def validate_phone_format(cls, phone: str) -> bool:
        """
        Базовая валидация формата телефона

        Args:
            phone: Номер телефона

        Returns:
            bool: True если формат корректен
        """
        if not phone:
            return False

        # Извлекаем только цифры
        digits = cls.extract_phone_digits(phone)

        # Базовые проверки
        if len(digits) < 7 or len(digits) > 15:
            return False

        # Российские номера
        if digits.startswith("7") and len(digits) == 11:
            return True

        # Другие форматы
        if len(digits) >= 10:
            return True

        return False


class BusinessDataValidator:
    """Валидатор для данных предприятий"""

    # Подозрительные паттерны в названиях
    SUSPICIOUS_NAME_PATTERNS = [
        r"^[a-z\s]+$",  # Только строчные латинские буквы
        r"^[A-Z\s]+$",  # Только заглавные латинские буквы
        r"^\d+$",  # Только цифры
        r"^[^\w\s]+$",  # Только специальные символы
    ]

    # Валидные категории (примеры)
    VALID_CATEGORIES = {
        "beauty_salon",
        "restaurant",
        "cafe",
        "shop",
        "service",
        "medical",
        "fitness",
        "education",
        "entertainment",
        "auto",
        "hotel",
    }

    @classmethod
    def validate_business_name(cls, name: str) -> Tuple[bool, List[str]]:
        """
        Валидация названия предприятия

        Args:
            name: Название предприятия

        Returns:
            Tuple[bool, List[str]]: (валидно, список предупреждений)
        """
        warnings = []

        if not name or len(name.strip()) < 2:
            return False, ["Название слишком короткое"]

        # Проверяем подозрительные паттерны
        for pattern in cls.SUSPICIOUS_NAME_PATTERNS:
            if re.match(pattern, name.strip()):
                warnings.append(f"Подозрительный паттерн в названии: {name}")

        # Проверяем на слишком много специальных символов
        special_chars_ratio = len(re.sub(r"[\w\s]", "", name)) / len(name)
        if special_chars_ratio > 0.3:
            warnings.append("Слишком много специальных символов в названии")

        # Проверяем на повторяющиеся символы
        if re.search(r"(.)\1{4,}", name):
            warnings.append("Обнаружены повторяющиеся символы")

        return len(warnings) == 0, warnings

    @classmethod
    def validate_rating_consistency(
        cls, rating: Optional[float], reviews_count: Optional[int]
    ) -> Tuple[bool, List[str]]:
        """
        Валидация соответствия рейтинга и количества отзывов

        Args:
            rating: Рейтинг предприятия
            reviews_count: Количество отзывов

        Returns:
            Tuple[bool, List[str]]: (валидно, список предупреждений)
        """
        warnings = []

        # Если есть рейтинг, должно быть и количество отзывов
        if rating is not None and (reviews_count is None or reviews_count == 0):
            warnings.append("Указан рейтинг, но нет отзывов")

        # Если очень высокий рейтинг при малом количестве отзывов
        if (
            rating is not None
            and reviews_count is not None
            and rating >= 4.8
            and reviews_count < 5
        ):
            warnings.append(
                "Подозрительно высокий рейтинг при малом количестве отзывов"
            )

        # Если много отзывов, но нет рейтинга
        if reviews_count is not None and reviews_count > 10 and rating is None:
            warnings.append("Много отзывов, но нет рейтинга")

        return len(warnings) == 0, warnings

    @classmethod
    def validate_contact_info(
        cls,
        phone: Optional[str],
        website: Optional[str],
        social_networks: Dict[str, Optional[str]],
    ) -> Tuple[bool, List[str]]:
        """
        Валидация контактной информации

        Args:
            phone: Номер телефона
            website: Сайт
            social_networks: Социальные сети

        Returns:
            Tuple[bool, List[str]]: (валидно, список предупреждений)
        """
        warnings = []
        contact_methods = 0

        # Подсчитываем способы связи
        if phone:
            contact_methods += 1
            if not DataValidationUtils.validate_phone_format(phone):
                warnings.append("Некорректный формат телефона")

        if website:
            contact_methods += 1
            if not DataValidationUtils.validate_url_domain(website):
                warnings.append("Некорректный формат сайта")

        # Социальные сети
        active_networks = sum(1 for network in social_networks.values() if network)
        contact_methods += active_networks

        if contact_methods == 0:
            warnings.append("Отсутствует контактная информация")
        elif contact_methods == 1:
            warnings.append("Только один способ связи")

        return len(warnings) == 0, warnings


class ServiceDataValidator:
    """Валидатор для данных услуг"""

    @classmethod
    def validate_service_price(
        cls, price: Optional[str], price_from: Optional[str], price_to: Optional[str]
    ) -> Tuple[bool, List[str]]:
        """
        Валидация цены услуги

        Args:
            price: Основная цена
            price_from: Цена от
            price_to: Цена до

        Returns:
            Tuple[bool, List[str]]: (валидно, список предупреждений)
        """
        warnings = []

        # Извлекаем числовые значения
        def extract_price(price_str: Optional[str]) -> Optional[float]:
            if not price_str:
                return None
            try:
                match = re.search(r"(\d+(?:[.,]\d+)?)", price_str)
                if match:
                    return float(match.group(1).replace(",", "."))
            except:
                pass
            return None

        price_val = extract_price(price)
        price_from_val = extract_price(price_from)
        price_to_val = extract_price(price_to)

        # Проверяем диапазон цен
        if price_from_val and price_to_val:
            if price_from_val >= price_to_val:
                warnings.append("Минимальная цена больше или равна максимальной")

            # Проверяем разумность диапазона
            ratio = price_to_val / price_from_val if price_from_val > 0 else 0
            if ratio > 10:
                warnings.append("Слишком большой диапазон цен")

        # Проверяем разумность цен
        all_prices = [
            p for p in [price_val, price_from_val, price_to_val] if p is not None
        ]
        for p in all_prices:
            if p < 1:
                warnings.append("Слишком низкая цена")
            elif p > 1000000:
                warnings.append("Слишком высокая цена")

        return len(warnings) == 0, warnings


class ReviewDataValidator:
    """Валидатор для данных отзывов"""

    @classmethod
    def validate_review_authenticity(
        cls, author: str, text: Optional[str], rating: Optional[int]
    ) -> Tuple[bool, List[str]]:
        """
        Базовая проверка подлинности отзыва

        Args:
            author: Автор отзыва
            text: Текст отзыва
            rating: Рейтинг

        Returns:
            Tuple[bool, List[str]]: (валидно, список предупреждений)
        """
        warnings = []

        # Проверяем автора
        if len(author) < 2:
            warnings.append("Слишком короткое имя автора")

        # Подозрительные паттерны в имени
        if re.match(r"^[a-zA-Z]+\s+[A-Z]\.$", author):
            # Нормальный паттерн "Имя Ф."
            pass
        elif re.match(r"^[a-zA-Z]{1,2}\s+[a-zA-Z]{1,2}\.$", author):
            warnings.append("Подозрительно короткое имя автора")

        # Проверяем текст отзыва
        if text:
            # Слишком короткий или длинный текст
            if len(text) < 10:
                warnings.append("Слишком короткий текст отзыва")
            elif len(text) > 3000:
                warnings.append("Слишком длинный текст отзыва")
