"""
Парсер для извлечения данных об услугах и ценах
"""

import re
from typing import Dict, List, Optional, Tuple

from .base_parser import BaseParser, ParseResult, TextNormalizer


class ServiceParser(BaseParser):
    """Парсер для услуг предприятий"""

    # Паттерны для поиска цен
    PRICE_PATTERNS = [
        # Базовые форматы цен
        r"(\d+(?:[.,]\d+)?)\s*(?:₽|руб|рублей?|р\.?|p\.?)",
        r"(?:₽|руб|рублей?|р\.?|p\.?)\s*(\d+(?:[.,]\d+)?)",
        # Диапазоны цен
        r"(?:от\s*)?(\d+(?:[.,]\d+)?)\s*[-–—]\s*(\d+(?:[.,]\d+)?)\s*(?:₽|руб|рублей?|р\.?|p\.?)",
        r"(?:₽|руб|рублей?|р\.?|p\.?)\s*(\d+(?:[.,]\d+)?)\s*[-–—]\s*(\d+(?:[.,]\d+)?)",
        # "От" и "до"
        r"от\s*(\d+(?:[.,]\d+)?)\s*(?:₽|руб|рублей?|р\.?|p\.?)",
        r"до\s*(\d+(?:[.,]\d+)?)\s*(?:₽|руб|рублей?|р\.?|p\.?)",
        # Просто числа (когда контекст ясен)
        r"(\d+(?:[.,]\d+)?)",
    ]

    # Паттерны для длительности услуг
    DURATION_PATTERNS = [
        r"(\d+(?:[.,]\d+)?)\s*(?:мин|минут|minutes?|м\.?)",
        r"(\d+(?:[.,]\d+)?)\s*(?:ч|час|часов|hours?|h\.?)",
        r"(\d+):(\d+)",  # Формат ЧЧ:ММ
        r"(\d+(?:[.,]\d+)?)\s*[-–—]\s*(\d+(?:[.,]\d+)?)\s*(?:мин|минут|minutes?)",
    ]

    # Ключевые слова для определения услуг
    SERVICE_KEYWORDS = [
        "услуга",
        "сервис",
        "процедура",
        "стрижка",
        "маникюр",
        "педикюр",
        "массаж",
        "окрашивание",
        "укладка",
        "лечение",
        "консультация",
        "диагностика",
        "обследование",
        "анализ",
        "процедура",
    ]

    def parse(self, raw_data: str) -> ParseResult:
        """
        Основной метод парсинга услуг

        Args:
            raw_data: Сырые данные об услугах

        Returns:
            ParseResult: Результат парсинга
        """
        if not raw_data:
            return self.create_result([], success=False, confidence=0.0)

        try:
            # Очищаем входные данные
            clean_data = self.clean_text(raw_data)

            # Разделяем на отдельные услуги
            service_items = self._split_services(clean_data)

            # Парсим каждую услугу
            parsed_services = []
            total_confidence = 0.0

            for item in service_items:
                service_data = self._parse_single_service(item)
                if service_data:
                    parsed_services.append(service_data["data"])
                    total_confidence += service_data["confidence"]

            # Вычисляем среднюю уверенность
            avg_confidence = (
                total_confidence / len(service_items) if service_items else 0.0
            )

            return self.create_result(
                data=parsed_services,
                success=len(parsed_services) > 0,
                confidence=avg_confidence,
            )

        except Exception as e:
            result = self.create_result([], success=False, confidence=0.0)
            result.add_error(f"Ошибка парсинга услуг: {str(e)}")
            return result

    def _split_services(self, text: str) -> List[str]:
        """Разделение текста на отдельные услуги"""

        # Пробуем различные способы разделения
        separators = [
            r"\n\s*[-•*]\s*",  # Списки с маркерами
            r"\n\s*\d+\.\s*",  # Нумерованные списки
            r"\n\s*\d+\)\s*",  # Списки с номерами в скобках
            r"\n{2,}",  # Двойные переносы строк
            r";\s*",  # Точка с запятой
            r"\|\s*",  # Вертикальная черта
        ]

        # Пробуем каждый разделитель
        for separator in separators:
            items = re.split(separator, text)
            if len(items) > 1:
                # Очищаем каждый элемент
                cleaned_items = []
                for item in items:
                    cleaned = self.clean_text(item)
                    if len(cleaned) > 5:  # Минимальная длина для услуги
                        cleaned_items.append(cleaned)

                if len(cleaned_items) > 1:
                    return cleaned_items

        # Если не удалось разделить, возвращаем исходный текст
        return [text] if text.strip() else []

    def _parse_single_service(self, service_text: str) -> Optional[Dict]:
        """Парсинг одной услуги"""

        if not service_text or len(service_text.strip()) < 3:
            return None

        service_data = {
            "name": "",
            "price": None,
            "price_from": None,
            "price_to": None,
            "description": None,
            "duration": None,
        }

        confidence = 0.5  # Базовая уверенность

        # Парсим цену
        price_info = self.parse_price(service_text)
        if price_info["success"]:
            service_data.update(price_info["data"])
            confidence += 0.3

        # Парсим длительность
        duration_info = self._parse_duration(service_text)
        if duration_info:
            service_data["duration"] = duration_info
            confidence += 0.1

        # Извлекаем название услуги
        name_info = self._extract_service_name(service_text, price_info.get("data", {}))
        if name_info:
            service_data["name"] = name_info["name"]
            service_data["description"] = name_info.get("description")
            confidence += 0.2

        # Проверяем качество извлеченных данных
        if not service_data["name"]:
            return None

        return {"data": service_data, "confidence": min(confidence, 1.0)}

    def parse_price(self, price_text: str) -> ParseResult:
        """
        Парсинг информации о цене

        Args:
            price_text: Текст с ценой

        Returns:
            ParseResult: Результат парсинга цены
        """
        if not price_text:
            return self.create_result({}, success=False)

        # Нормализуем текст
        normalized_text = TextNormalizer.normalize_price_text(price_text)

        price_data = {"price": None, "price_from": None, "price_to": None}

        confidence = 0.0

        # Ищем диапазон цен
        range_match = re.search(
            r"(\d+(?:[.,]\d+)?)\s*[-–—]\s*(\d+(?:[.,]\d+)?)", normalized_text
        )
        if range_match:
            try:
                price_from = float(range_match.group(1).replace(",", "."))
                price_to = float(range_match.group(2).replace(",", "."))

                if price_from < price_to:  # Проверяем логичность диапазона
                    price_data["price_from"] = str(int(price_from))
                    price_data["price_to"] = str(int(price_to))
                    price_data["price"] = (
                        f"{price_data['price_from']}-{price_data['price_to']}"
                    )
                    confidence = 0.9

            except ValueError:
                pass

        # Если диапазон не найден, ищем одиночную цену
        if not price_data["price"]:
            # Ищем "от X"
            from_match = re.search(r"от\s*(\d+(?:[.,]\d+)?)", normalized_text)
            if from_match:
                try:
                    price_from = float(from_match.group(1).replace(",", "."))
                    price_data["price_from"] = str(int(price_from))
                    price_data["price"] = f"от {price_data['price_from']}"
                    confidence = 0.8
                except ValueError:
                    pass

            # Ищем "до X"
            elif re.search(r"до\s*(\d+(?:[.,]\d+)?)", normalized_text):
                to_match = re.search(r"до\s*(\d+(?:[.,]\d+)?)", normalized_text)
                try:
                    price_to = float(to_match.group(1).replace(",", "."))
                    price_data["price_to"] = str(int(price_to))
                    price_data["price"] = f"до {price_data['price_to']}"
                    confidence = 0.8
                except ValueError:
                    pass

            # Ищем простую цену
            else:
                simple_price_match = re.search(r"(\d+(?:[.,]\d+)?)", normalized_text)
                if simple_price_match:
                    try:
                        price = float(simple_price_match.group(1).replace(",", "."))
                        price_data["price"] = str(int(price))
                        confidence = 0.7
                    except ValueError:
                        pass

        # Валидация цен
        if confidence > 0:
            confidence = self._validate_price_reasonableness(price_data, confidence)

        return self.create_result(
            data=price_data, success=confidence > 0, confidence=confidence
        )

    def _validate_price_reasonableness(
        self, price_data: Dict, current_confidence: float
    ) -> float:
        """Валидация разумности цен"""

        def is_reasonable_price(price_str: str) -> bool:
            try:
                price = float(price_str)
                return 50 <= price <= 500000  # Разумный диапазон цен
            except (ValueError, TypeError):
                return False

        # Проверяем все цены
        prices_to_check = [
            price_data.get("price"),
            price_data.get("price_from"),
            price_data.get("price_to"),
        ]

        reasonable_count = 0
        total_count = 0

        for price in prices_to_check:
            if (
                price
                and price.replace("от ", "").replace("до ", "").split("-")[0].isdigit()
            ):
                total_count += 1
                # Извлекаем числовое значение
                numeric_price = re.search(r"(\d+)", price)
                if numeric_price and is_reasonable_price(numeric_price.group(1)):
                    reasonable_count += 1

        if total_count == 0:
            return 0.0

        reasonableness_ratio = reasonable_count / total_count
        return current_confidence * reasonableness_ratio

    def _parse_duration(self, text: str) -> Optional[str]:
        """Парсинг длительности услуги"""

        duration_patterns = [
            (r"(\d+)\s*(?:мин|минут|min|minutes?)", "мин"),
            (r"(\d+)\s*(?:ч|час|часов|hour|hours?)", "ч"),
            (r"(\d+):(\d+)", None),  # Формат ЧЧ:ММ
            (
                r"(\d+(?:[.,]\d+)?)\s*[-–—]\s*(\d+(?:[.,]\d+)?)\s*(?:мин|минут)",
                "мин_диапазон",
            ),
        ]

        for pattern, duration_type in duration_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                if duration_type == "мин":
                    return f"{match.group(1)} мин"
                elif duration_type == "ч":
                    hours = int(match.group(1))
                    return f"{hours} ч" if hours == 1 else f"{hours} ч"
                elif duration_type is None:  # Формат ЧЧ:ММ
                    return f"{match.group(1)}:{match.group(2)}"
                elif duration_type == "мин_диапазон":
                    return f"{match.group(1)}-{match.group(2)} мин"

        return None

    def _extract_service_name(self, text: str, price_info: Dict) -> Optional[Dict]:
        """Извлечение названия услуги"""

        # Убираем информацию о цене из текста
        text_without_price = text
        if price_info:
            # Удаляем найденные цены
            price_patterns = [
                r"\d+(?:[.,]\d+)?\s*[-–—]\s*\d+(?:[.,]\d+)?\s*(?:₽|руб|рублей?|р\.?)",
                r"(?:от\s*)?\d+(?:[.,]\d+)?\s*(?:₽|руб|рублей?|р\.?)",
                r"(?:до\s*)?\d+(?:[.,]\d+)?\s*(?:₽|руб|рублей?|р\.?)",
            ]

            for pattern in price_patterns:
                text_without_price = re.sub(
                    pattern, "", text_without_price, flags=re.IGNORECASE
                )

        # Убираем информацию о времени
        time_patterns = [
            r"\d+\s*(?:мин|минут|min|minutes?)",
            r"\d+\s*(?:ч|час|часов|hour|hours?)",
            r"\d+:\d+",
        ]

        for pattern in time_patterns:
            text_without_price = re.sub(
                pattern, "", text_without_price, flags=re.IGNORECASE
            )

        # Очищаем текст
        cleaned_name = self.clean_text(text_without_price)

        # Разделяем на название и описание
        sentences = re.split(r"[.!?]", cleaned_name)

        if sentences:
            name = sentences[0].strip()
            description = (
                " ".join(sentences[1:]).strip() if len(sentences) > 1 else None
            )

            # Базовая валидация названия
            if len(name) < 3 or len(name) > 200:
                return None

            return {
                "name": name,
                "description": (
                    description if description and len(description) > 10 else None
                ),
            }

        return None

    def parse_service_category(self, service_name: str) -> Optional[str]:
        """Определение категории услуги"""

        if not service_name:
            return None

        name_lower = service_name.lower()

        categories = {
            "beauty": [
                "стрижка",
                "окрашивание",
                "укладка",
                "прическа",
                "маникюр",
                "педикюр",
                "косметология",
            ],
            "massage": ["массаж", "spa", "релакс"],
            "medical": [
                "лечение",
                "диагностика",
                "консультация",
                "обследование",
                "анализ",
            ],
            "fitness": ["тренировка", "фитнес", "йога", "пилатес"],
            "education": ["курс", "обучение", "семинар", "мастер-класс"],
        }

        for category, keywords in categories.items():
            if any(keyword in name_lower for keyword in keywords):
                return category

        return "other"

    def extract_service_benefits(self, description: str) -> List[str]:
        """Извлечение преимуществ услуги из описания"""

        if not description:
            return []

        benefit_patterns = [
            r"(?:включает?|в том числе):\s*([^.]+)",
            r"(?:преимущества?|плюсы?):\s*([^.]+)",
            r"(?:результат|эффект):\s*([^.]+)",
        ]

        benefits = []

        for pattern in benefit_patterns:
            matches = re.finditer(pattern, description, re.IGNORECASE)
            for match in matches:
                benefit_text = match.group(1).strip()
                # Разделяем по запятым или точкам с запятой
                benefit_items = re.split(r"[,;]", benefit_text)
                for item in benefit_items:
                    cleaned_item = self.clean_text(item)
                    if cleaned_item and len(cleaned_item) > 3:
                        benefits.append(cleaned_item)

        return benefits[:5]  # Ограничиваем количество преимуществ
