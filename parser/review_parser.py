"""
Парсер для извлечения данных отзывов
"""

import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from .base_parser import BaseParser, ParseResult, TextNormalizer


class ReviewParser(BaseParser):
    """Парсер для отзывов о предприятиях"""

    # Паттерны для парсинга рейтинга
    RATING_PATTERNS = [
        r"(\d)\s*(?:из\s*5|/5|\*|★)",  # X из 5, X/5, X*, X★
        r"(\d+)\s*звезд[ыа]?",  # X звезд
        r"(\d+)\s*балл[ао]в?",  # X баллов
        r"оценка:\s*(\d+)",  # Оценка: X
    ]

    # Паттерны для дат
    DATE_PATTERNS = [
        # Полные даты с годом
        (
            r"(\d{1,2})\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\s+(\d{4})",
            "ru_full",
        ),
        (
            r"(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{4})",
            "en_full",
        ),
        # Даты без года
        (
            r"(\d{1,2})\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)",
            "ru_short",
        ),
        (r"(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)", "en_short"),
        # Числовые форматы
        (r"(\d{1,2})\.(\d{1,2})\.(\d{2,4})", "numeric_dot"),
        (r"(\d{1,2})/(\d{1,2})/(\d{2,4})", "numeric_slash"),
        (r"(\d{4})-(\d{1,2})-(\d{1,2})", "iso"),
        # Относительные даты
        (r"(сегодня|вчера|позавчера)", "relative_ru"),
        (r"(today|yesterday)", "relative_en"),
        (r"(\d+)\s*(?:дн|дня|дней|day|days)\s*назад", "days_ago"),
        (r"(\d+)\s*(?:нед|недел[иь]|week|weeks)\s*назад", "weeks_ago"),
        (r"(\d+)\s*(?:мес|месяц[ае]в?|month|months)\s*назад", "months_ago"),
    ]

    # Индикаторы ответа владельца
    OWNER_RESPONSE_INDICATORS = [
        "ответ владельца",
        "ответ заведения",
        "от администрации",
        "owner response",
        "business response",
        "management response",
        "администратор",
        "менеджер",
        "руководство",
    ]

    # Паттерны для извлечения имен авторов
    AUTHOR_PATTERNS = [
        r"^([А-Я][а-я]+\s+[А-Я]\.?)$",  # Имя Ф.
        r"^([А-Я][а-я]+)$",  # Просто имя
        r"^([A-Z][a-z]+\s+[A-Z]\.?)$",  # English Name F.
        r"^([A-Z][a-z]+)$",  # English name
    ]

    def parse(self, raw_data: str) -> ParseResult:
        """
        Основной метод парсинга отзывов

        Args:
            raw_data: Сырые данные отзывов

        Returns:
            ParseResult: Результат парсинга
        """
        if not raw_data:
            return self.create_result([], success=False, confidence=0.0)

        try:
            # Разделяем на отдельные отзывы
            review_blocks = self._split_reviews(raw_data)

            # Парсим каждый отзыв
            parsed_reviews = []
            total_confidence = 0.0

            for block in review_blocks:
                review_data = self._parse_single_review(block)
                if review_data:
                    parsed_reviews.append(review_data["data"])
                    total_confidence += review_data["confidence"]

            # Вычисляем среднюю уверенность
            avg_confidence = (
                total_confidence / len(review_blocks) if review_blocks else 0.0
            )

            return self.create_result(
                data=parsed_reviews,
                success=len(parsed_reviews) > 0,
                confidence=avg_confidence,
            )

        except Exception as e:
            result = self.create_result([], success=False, confidence=0.0)
            result.add_error(f"Ошибка парсинга отзывов: {str(e)}")
            return result

    def _split_reviews(self, text: str) -> List[str]:
        """Разделение текста на отдельные отзывы"""

        # Различные способы разделения отзывов
        separators = [
            r"\n\s*[-•*]\s*",  # Маркированные списки
            r"\n\s*\d+\.\s*",  # Нумерованные списки
            r"\n{3,}",  # Множественные переносы
            r"(?=\n[А-Я][а-я]+\s+[А-Я]\.)",  # Новый отзыв с именем автора
            r"(?=\n\d+\s+(?:звезд|★))",  # Новый отзыв с рейтингом
        ]

        # Пробуем разделители по порядку
        for separator in separators:
            blocks = re.split(separator, text)
            if len(blocks) > 1:
                cleaned_blocks = []
                for block in blocks:
                    cleaned = self.clean_text(block)
                    if len(cleaned) > 20:  # Минимальная длина отзыва
                        cleaned_blocks.append(cleaned)

                if len(cleaned_blocks) > 1:
                    return cleaned_blocks

        # Если не удалось разделить, возвращаем исходный текст
        return [text] if text.strip() else []

    def _parse_single_review(self, review_text: str) -> Optional[Dict]:
        """Парсинг одного отзыва"""

        if not review_text or len(review_text.strip()) < 10:
            return None

        review_data = {
            "author": "",
            "rating": None,
            "date": None,
            "text": None,
            "response": None,
            "helpful_count": None,
        }

        confidence = 0.3  # Базовая уверенность

        # Извлекаем автора
        author_info = self._extract_author(review_text)
        if author_info:
            review_data["author"] = author_info["name"]
            confidence += 0.2

        # Извлекаем рейтинг
        rating_info = self._extract_rating(review_text)
        if rating_info:
            review_data["rating"] = rating_info
            confidence += 0.2

        # Извлекаем дату
        date_info = self.parse_date(review_text)
        if date_info:
            review_data["date"] = date_info
            confidence += 0.1

        # Извлекаем текст отзыва и ответ
        text_info = self._extract_review_text(review_text)
        if text_info:
            review_data["text"] = text_info.get("review_text")
            review_data["response"] = text_info.get("owner_response")
            confidence += 0.2

        # Извлекаем количество "полезно"
        helpful_info = self._extract_helpful_count(review_text)
        if helpful_info:
            review_data["helpful_count"] = helpful_info
            confidence += 0.1

        # Проверяем минимальные требования
        if not review_data["author"]:
            return None

        return {"data": review_data, "confidence": min(confidence, 1.0)}

    def _extract_author(self, text: str) -> Optional[Dict]:
        """Извлечение автора отзыва"""

        # Ищем в начале текста
        lines = text.split("\n")
        for line in lines[:3]:  # Проверяем первые 3 строки
            line = line.strip()

            # Проверяем паттерны имен
            for pattern in self.AUTHOR_PATTERNS:
                match = re.match(pattern, line)
                if match:
                    name = match.group(1).strip()
                    if 2 <= len(name) <= 50:  # Разумная длина имени
                        return {"name": name}

        # Если паттерны не сработали, ищем первое "разумное" слово
        words = text.split()
        for word in words[:5]:  # Проверяем первые 5 слов
            cleaned_word = re.sub(r"[^\w\s.-]", "", word, flags=re.UNICODE)
            if (
                len(cleaned_word) >= 2
                and cleaned_word[0].isupper()
                and not cleaned_word.isdigit()
            ):
                return {"name": cleaned_word}

        return None

    def _extract_rating(self, text: str) -> Optional[int]:
        """Извлечение рейтинга"""

        for pattern in self.RATING_PATTERNS:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    rating = int(match.group(1))
                    if 1 <= rating <= 5:
                        return rating
                except (ValueError, IndexError):
                    continue

        # Ищем звездочки
        stars_match = re.search(r"(★+)", text)
        if stars_match:
            star_count = len(stars_match.group(1))
            if 1 <= star_count <= 5:
                return star_count

        return None

    def parse_date(self, date_text: str) -> Optional[str]:
        """
        Парсинг даты отзыва

        Args:
            date_text: Текст с датой

        Returns:
            Optional[str]: Нормализованная дата
        """
        if not date_text:
            return None

        # Обрабатываем каждый паттерн
        for pattern, format_type in self.DATE_PATTERNS:
            match = re.search(pattern, date_text, re.IGNORECASE)
            if match:
                try:
                    if format_type in ["ru_full", "en_full"]:
                        day, month, year = match.groups()
                        month_num = TextNormalizer.normalize_month_name(month)
                        if month_num:
                            return f"{day} {month} {year}"

                    elif format_type in ["ru_short", "en_short"]:
                        day, month = match.groups()
                        month_num = TextNormalizer.normalize_month_name(month)
                        current_year = datetime.now().year
                        if month_num:
                            return f"{day} {month} {current_year}"

                    elif format_type == "numeric_dot":
                        day, month, year = match.groups()
                        if len(year) == 2:
                            year = f"20{year}" if int(year) <= 30 else f"19{year}"
                        return f"{day}.{month}.{year}"

                    elif format_type == "numeric_slash":
                        month, day, year = match.groups()  # Американский формат
                        if len(year) == 2:
                            year = f"20{year}" if int(year) <= 30 else f"19{year}"
                        return f"{day}.{month}.{year}"

                    elif format_type == "iso":
                        year, month, day = match.groups()
                        return f"{day}.{month}.{year}"

                    elif format_type == "relative_ru":
                        relative = match.group(1).lower()
                        if relative == "сегодня":
                            return datetime.now().strftime("%d.%m.%Y")
                        elif relative == "вчера":
                            return (datetime.now() - timedelta(days=1)).strftime(
                                "%d.%m.%Y"
                            )
                        elif relative == "позавчера":
                            return (datetime.now() - timedelta(days=2)).strftime(
                                "%d.%m.%Y"
                            )

                    elif format_type == "relative_en":
                        relative = match.group(1).lower()
                        if relative == "today":
                            return datetime.now().strftime("%d.%m.%Y")
                        elif relative == "yesterday":
                            return (datetime.now() - timedelta(days=1)).strftime(
                                "%d.%m.%Y"
                            )

                    elif format_type == "days_ago":
                        days = int(match.group(1))
                        if days <= 365:  # Максимум год назад
                            return (datetime.now() - timedelta(days=days)).strftime(
                                "%d.%m.%Y"
                            )

                    elif format_type == "weeks_ago":
                        weeks = int(match.group(1))
                        if weeks <= 52:  # Максимум год назад
                            return (datetime.now() - timedelta(weeks=weeks)).strftime(
                                "%d.%m.%Y"
                            )

                    elif format_type == "months_ago":
                        months = int(match.group(1))
                        if months <= 12:  # Максимум год назад
                            return (
                                datetime.now() - timedelta(days=months * 30)
                            ).strftime("%d.%m.%Y")

                except (ValueError, TypeError):
                    continue

        return None

    def _extract_review_text(self, text: str) -> Optional[Dict]:
        """Извлечение текста отзыва и ответа владельца"""

        # Убираем метаинформацию (автор, дата, рейтинг)
        cleaned_text = text

        # Убираем строки с автором
        lines = cleaned_text.split("\n")
        filtered_lines = []

        for line in lines:
            line_stripped = line.strip()

            # Пропускаем строки с именами авторов
            if any(
                re.match(pattern, line_stripped) for pattern in self.AUTHOR_PATTERNS
            ):
                continue

            # Пропускаем строки с рейтингами
            if any(
                re.search(pattern, line_stripped, re.IGNORECASE)
                for pattern in self.RATING_PATTERNS
            ):
                continue

            # Пропускаем строки с датами
            if any(
                re.search(pattern[0], line_stripped, re.IGNORECASE)
                for pattern in self.DATE_PATTERNS
            ):
                continue

            # Добавляем оставшиеся строки
            if len(line_stripped) > 5:
                filtered_lines.append(line_stripped)

        if not filtered_lines:
            return None

        full_text = " ".join(filtered_lines)

        # Ищем ответ владельца
        owner_response = None
        review_text = full_text

        for indicator in self.OWNER_RESPONSE_INDICATORS:
            if indicator in full_text.lower():
                parts = re.split(indicator, full_text, 1, re.IGNORECASE)
                if len(parts) == 2:
                    review_text = parts[0].strip()
                    owner_response = parts[1].strip()
                    break

        # Очищаем тексты
        if review_text:
            review_text = self.clean_text(review_text)

        if owner_response:
            owner_response = self.clean_text(owner_response)

        return {
            "review_text": review_text if len(review_text) > 10 else None,
            "owner_response": (
                owner_response if owner_response and len(owner_response) > 10 else None
            ),
        }

    def _extract_helpful_count(self, text: str) -> Optional[int]:
        """Извлечение количества отметок 'полезно'"""

        helpful_patterns = [
            r"(\d+)\s*(?:полезн|helpful|like)",
            r"(?:полезн|helpful|like).*?(\d+)",
            r"👍\s*(\d+)",
            r"(\d+)\s*👍",
        ]

        for pattern in helpful_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    count = int(match.group(1))
                    if 0 <= count <= 10000:  # Разумный диапазон
                        return count
                except (ValueError, IndexError):
                    continue

        return None

    def analyze_sentiment(self, review_text: str) -> Dict[str, any]:
        """Базовый анализ тональности отзыва"""

        if not review_text:
            return {"sentiment": "neutral", "confidence": 0.0}

        text_lower = review_text.lower()

        # Позитивные слова
        positive_words = [
            "отлично",
            "прекрасно",
            "замечательно",
            "великолепно",
            "супер",
            "класс",
            "круто",
            "восхитительно",
            "превосходно",
            "шикарно",
            "рекомендую",
            "советую",
            "довольн",
            "понравил",
            "хорошо",
        ]

        # Негативные слова
        negative_words = [
            "плохо",
            "ужасно",
            "кошмар",
            "отвратительно",
            "мерзко",
            "не рекомендую",
            "разочарован",
            "расстроен",
            "недоволен",
            "жаль",
            "сожалею",
            "проблем",
            "ошибк",
            "неудач",
        ]

        positive_count = sum(1 for word in positive_words if word in text_lower)
        negative_count = sum(1 for word in negative_words if word in text_lower)

        if positive_count > negative_count:
            sentiment = "positive"
            confidence = min(
                positive_count / (positive_count + negative_count + 1), 0.9
            )
        elif negative_count > positive_count:
            sentiment = "negative"
            confidence = min(
                negative_count / (positive_count + negative_count + 1), 0.9
            )
        else:
            sentiment = "neutral"
            confidence = 0.5

        return {
            "sentiment": sentiment,
            "confidence": confidence,
            "positive_indicators": positive_count,
            "negative_indicators": negative_count,
        }

    def extract_review_topics(self, review_text: str) -> List[str]:
        """Извлечение тем/аспектов из отзыва"""

        if not review_text:
            return []

        # Словарь тем и ключевых слов
        topics = {
            "service": [
                "сервис",
                "обслуживание",
                "персонал",
                "сотрудник",
                "администратор",
            ],
            "quality": ["качество", "результат", "работа", "выполнение"],
            "price": ["цена", "стоимость", "дорого", "дешево", "деньги"],
            "atmosphere": ["атмосфера", "интерьер", "обстановка", "уют"],
            "cleanliness": ["чистота", "чисто", "грязно", "убрано"],
            "time": ["время", "быстро", "долго", "опоздание", "ожидание"],
            "location": ["место", "расположение", "парковка", "добраться"],
        }

        text_lower = review_text.lower()
        found_topics = []

        for topic, keywords in topics.items():
            if any(keyword in text_lower for keyword in keywords):
                found_topics.append(topic)

        return found_topics[:3]  # Ограничиваем количество тем
