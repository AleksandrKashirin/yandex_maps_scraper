"""
Парсер для извлечения контактной информации
"""

import re
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

from .base_parser import BaseParser, ParseResult


class ContactParser(BaseParser):
    """Парсер для контактной информации"""

    # Паттерны для номеров телефонов
    PHONE_PATTERNS = [
        # Российские номера
        r"\+7\s*\(?(\d{3})\)?\s*(\d{3})[-\s]*(\d{2})[-\s]*(\d{2})",
        r"8\s*\(?(\d{3})\)?\s*(\d{3})[-\s]*(\d{2})[-\s]*(\d{2})",
        r"7\s*\(?(\d{3})\)?\s*(\d{3})[-\s]*(\d{2})[-\s]*(\d{2})",
        # Международные номера
        r"\+(\d{1,3})\s*\(?(\d{1,4})\)?\s*(\d{1,4})[-\s]*(\d{1,4})[-\s]*(\d{0,4})",
        # Общие паттерны
        r"(\d{3,4})[-\s]*(\d{2,3})[-\s]*(\d{2,3})[-\s]*(\d{2,3})",
    ]

    # Паттерны для сайтов
    WEBSITE_PATTERNS = [
        r"https?://(?:[-\w.])+(?:\.[a-zA-Z]{2,})(?:/(?:[\w._~!$&\'()*+,;=:@]|%\d+)*)*(?:\?(?:[\w._~!$&\'()*+,;=:@/?]|%\d+)*)?(?:#(?:[\w._~!$&\'()*+,;=:@/?]|%\d+)*)?",
        r"www\.(?:[-\w.])+\.[a-zA-Z]{2,}(?:/(?:[\w._~!$&\'()*+,;=:@]|%\d+)*)*",
        r"(?:[-\w.])+\.[a-zA-Z]{2,}(?:/(?:[\w._~!$&\'()*+,;=:@]|%\d+)*)*",
    ]

    # Паттерны для социальных сетей
    SOCIAL_PATTERNS = {
        "telegram": [
            r"(?:https?://)?(?:t\.me|telegram\.me|telegram\.org)/([a-zA-Z0-9_]+)",
            r"@([a-zA-Z0-9_]+)\s*(?:telegram|tg)",
            r"telegram\s*[:@]\s*([a-zA-Z0-9_]+)",
        ],
        "whatsapp": [
            r"(?:https?://)?(?:wa\.me|api\.whatsapp\.com/send\?phone=)(\d+)",
            r"whatsapp\s*[:@]\s*\+?(\d+)",
            r"wa\s*[:@]\s*\+?(\d+)",
        ],
        "vk": [
            r"(?:https?://)?(?:vk\.com|m\.vk\.com)/([a-zA-Z0-9_.]+)",
            r"vk\s*[:@]\s*([a-zA-Z0-9_.]+)",
            r"вконтакте\s*[:@]\s*([a-zA-Z0-9_.]+)",
        ]
    }

    # Индикаторы email
    EMAIL_PATTERN = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"

    def parse(self, raw_data: Dict) -> ParseResult:
        """
        Основной метод парсинга контактов

        Args:
            raw_data: Словарь с сырыми контактными данными

        Returns:
            ParseResult: Результат парсинга
        """
        if not raw_data:
            return self.create_result({}, success=False, confidence=0.0)

        try:
            contact_data = {
                "phone": None,
                "website": None,
                "email": None,
                "social_networks": {},
            }

            total_confidence = 0.0
            found_contacts = 0

            # Парсим телефон
            if "phone" in raw_data and raw_data["phone"]:
                phone_result = self.parse_phone(raw_data["phone"])
                if phone_result:
                    contact_data["phone"] = phone_result
                    total_confidence += 0.9
                    found_contacts += 1

            # Парсим сайт
            if "website" in raw_data and raw_data["website"]:
                website_result = self.parse_website(raw_data["website"])
                if website_result:
                    contact_data["website"] = website_result
                    total_confidence += 0.8
                    found_contacts += 1

            # Парсим email
            if "email" in raw_data and raw_data["email"]:
                email_result = self.parse_email(raw_data["email"])
                if email_result:
                    contact_data["email"] = email_result
                    total_confidence += 0.7
                    found_contacts += 1

            # Парсим социальные сети
            social_result = self._parse_social_networks(raw_data)
            if social_result:
                contact_data["social_networks"] = social_result
                total_confidence += len(social_result) * 0.6
                found_contacts += len(social_result)

            # Автоматическое извлечение из текста
            if "text" in raw_data and raw_data["text"]:
                auto_extracted = self._auto_extract_contacts(raw_data["text"])
                if auto_extracted:
                    # Объединяем с уже найденными данными
                    for key, value in auto_extracted.items():
                        if value and not contact_data.get(key):
                            contact_data[key] = value
                            total_confidence += 0.5
                            found_contacts += 1

            # Вычисляем среднюю уверенность
            avg_confidence = (
                min(total_confidence / max(found_contacts, 1), 1.0)
                if found_contacts > 0
                else 0.0
            )

            return self.create_result(
                data=contact_data, success=found_contacts > 0, confidence=avg_confidence
            )

        except Exception as e:
            result = self.create_result({}, success=False, confidence=0.0)
            result.add_error(f"Ошибка парсинга контактов: {str(e)}")
            return result

    def parse_phone(self, phone_text: str) -> Optional[str]:
        """
        Парсинг номера телефона

        Args:
            phone_text: Текст с номером телефона

        Returns:
            Optional[str]: Нормализованный номер телефона
        """
        if not phone_text:
            return None

        # Убираем лишние символы
        clean_text = re.sub(r"[^\d\+\(\)\-\s]", "", phone_text)

        # Пробуем различные паттерны
        for pattern in self.PHONE_PATTERNS:
            match = re.search(pattern, clean_text)
            if match:
                groups = match.groups()

                # Обрабатываем российские номера
                if len(groups) >= 4:
                    if pattern.startswith(r"\+7") or clean_text.startswith("+7"):
                        code, part1, part2, part3 = groups[:4]
                        return f"+7 ({code}) {part1}-{part2}-{part3}"
                    elif pattern.startswith(r"8") or clean_text.startswith("8"):
                        code, part1, part2, part3 = groups[:4]
                        return f"+7 ({code}) {part1}-{part2}-{part3}"
                    elif pattern.startswith(r"7"):
                        code, part1, part2, part3 = groups[:4]
                        return f"+7 ({code}) {part1}-{part2}-{part3}"

                # Обрабатываем международные номера
                if len(groups) >= 3:
                    return "+" + "".join(groups)

        # Если паттерны не сработали, попробуем простое извлечение цифр
        digits = re.sub(r"[^\d]", "", phone_text)
        if len(digits) >= 10:
            if digits.startswith("8") and len(digits) == 11:
                # Российский номер, начинающийся с 8
                return f"+7 ({digits[1:4]}) {digits[4:7]}-{digits[7:9]}-{digits[9:11]}"
            elif digits.startswith("7") and len(digits) == 11:
                # Российский номер, начинающийся с 7
                return f"+7 ({digits[1:4]}) {digits[4:7]}-{digits[7:9]}-{digits[9:11]}"
            elif len(digits) == 10:
                # Российский номер без кода страны
                return f"+7 ({digits[:3]}) {digits[3:6]}-{digits[6:8]}-{digits[8:10]}"

        return None

    def parse_website(self, website_text: str) -> Optional[str]:
        """
        Парсинг сайта

        Args:
            website_text: Текст с сайтом

        Returns:
            Optional[str]: Нормализованный URL сайта
        """
        if not website_text:
            return None

        # Убираем пробелы
        clean_text = website_text.strip()

        # Пробуем паттерны URL
        for pattern in self.WEBSITE_PATTERNS:
            match = re.search(pattern, clean_text, re.IGNORECASE)
            if match:
                url = match.group(0)

                # Добавляем схему если отсутствует
                if not url.startswith(("http://", "https://")):
                    if url.startswith("www."):
                        url = "https://" + url
                    else:
                        url = "https://" + url

                # Валидируем URL
                if self._validate_url(url):
                    return url

        return None

    def parse_email(self, email_text: str) -> Optional[str]:
        """
        Парсинг email адреса

        Args:
            email_text: Текст с email

        Returns:
            Optional[str]: Нормализованный email
        """
        if not email_text:
            return None

        match = re.search(self.EMAIL_PATTERN, email_text)
        if match:
            email = match.group(0).lower()

            # Базовая валидация
            if self._validate_email(email):
                return email

        return None

    def _parse_social_networks(self, raw_data: Dict) -> Dict[str, str]:
        """Парсинг социальных сетей"""

        social_networks = {}

        # Обрабатываем каждую социальную сеть
        for network, patterns in self.SOCIAL_PATTERNS.items():

            # Проверяем прямые ссылки в данных
            if network in raw_data and raw_data[network]:
                url = self._normalize_social_url(network, raw_data[network])
                if url:
                    social_networks[network] = url
                    continue

            # Ищем в тексте
            if "text" in raw_data and raw_data["text"]:
                for pattern in patterns:
                    match = re.search(pattern, raw_data["text"], re.IGNORECASE)
                    if match:
                        url = self._build_social_url(network, match)
                        if url:
                            social_networks[network] = url
                            break

        return social_networks

    def _normalize_social_url(self, network: str, url: str) -> Optional[str]:
        """Нормализация URL социальной сети"""

        if not url:
            return None

        # Убираем пробелы
        clean_url = url.strip()

        # Если это уже полный URL, валидируем и возвращаем
        if clean_url.startswith(("http://", "https://")):
            if self._validate_social_url(network, clean_url):
                return clean_url

        # Пробуем построить URL из частичной информации
        for pattern in self.SOCIAL_PATTERNS[network]:
            match = re.search(pattern, clean_url, re.IGNORECASE)
            if match:
                return self._build_social_url(network, match)

        return None

    def _build_social_url(self, network: str, match) -> Optional[str]:
        """Построение полного URL социальной сети"""

        if not match:
            return None

        try:
            if network == "telegram":
                username = match.group(1)
                return f"https://t.me/{username}"

            elif network == "whatsapp":
                phone = match.group(1)
                # Убираем все кроме цифр
                clean_phone = re.sub(r"[^\d]", "", phone)
                if len(clean_phone) >= 10:
                    return f"https://wa.me/{clean_phone}"

            elif network == "vk":
                username = match.group(1)
                return f"https://vk.com/{username}"

        except (IndexError, AttributeError):
            pass

        return None

    def _auto_extract_contacts(self, text: str) -> Dict[str, any]:
        """Автоматическое извлечение контактов из текста"""

        extracted = {}

        if not text:
            return extracted

        # Ищем телефоны
        phone = self.parse_phone(text)
        if phone:
            extracted["phone"] = phone

        # Ищем email
        email = self.parse_email(text)
        if email:
            extracted["email"] = email

        # Ищем сайты
        website = self.parse_website(text)
        if website:
            extracted["website"] = website

        # Ищем социальные сети
        social_networks = {}
        for network, patterns in self.SOCIAL_PATTERNS.items():
            for pattern in patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    url = self._build_social_url(network, match)
                    if url:
                        social_networks[network] = url
                        break

        if social_networks:
            extracted["social_networks"] = social_networks

        return extracted

    def _validate_url(self, url: str) -> bool:
        """Валидация URL"""

        try:
            parsed = urlparse(url)

            # Проверяем схему
            if parsed.scheme not in ["http", "https"]:
                return False

            # Проверяем наличие домена
            if not parsed.netloc:
                return False

            # Проверяем, что это не Яндекс.Карты
            if "yandex" in parsed.netloc.lower() and "maps" in url.lower():
                return False

            return True

        except Exception:
            return False

    def _validate_email(self, email: str) -> bool:
        """Валидация email"""

        if not email or "@" not in email:
            return False

        # Базовые проверки
        parts = email.split("@")
        if len(parts) != 2:
            return False

        local, domain = parts

        # Проверяем локальную часть
        if len(local) < 1 or len(local) > 64:
            return False

        # Проверяем домен
        if len(domain) < 1 or len(domain) > 255:
            return False

        if "." not in domain:
            return False

        return True

    def _validate_social_url(self, network: str, url: str) -> bool:
        """Валидация URL социальной сети"""

        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()

            domain_patterns = {
                "telegram": ["t.me", "telegram.me", "telegram.org"],
                "whatsapp": ["wa.me", "api.whatsapp.com"],
                "vk": ["vk.com", "m.vk.com"]
            }

            if network in domain_patterns:
                return any(pattern in domain for pattern in domain_patterns[network])

            return False

        except Exception:
            return False

    def extract_contact_context(self, text: str) -> Dict[str, List[str]]:
        """Извлечение контекста для контактной информации"""

        context = {
            "phone_context": [],
            "website_context": [],
            "email_context": [],
            "social_context": [],
        }

        if not text:
            return context

        # Контекстные слова для телефонов
        phone_indicators = [
            "телефон",
            "тел",
            "phone",
            "позвонить",
            "звонить",
            "связаться",
            "контакт",
            "номер",
        ]

        # Контекстные слова для сайтов
        website_indicators = [
            "сайт",
            "website",
            "интернет",
            "онлайн",
            "домен",
            "страница",
            "ссылка",
        ]

        # Ищем контекст для каждого типа контактов
        sentences = re.split(r"[.!?]", text)

        for sentence in sentences:
            sentence_lower = sentence.lower()

            # Контекст для телефонов
            if any(indicator in sentence_lower for indicator in phone_indicators):
                if self.parse_phone(sentence):
                    context["phone_context"].append(sentence.strip())

            # Контекст для сайтов
            if any(indicator in sentence_lower for indicator in website_indicators):
                if self.parse_website(sentence):
                    context["website_context"].append(sentence.strip())

        return context
