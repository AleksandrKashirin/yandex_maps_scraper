"""
Модель данных для социальных сетей предприятия
"""

import re
from typing import Dict, List, Optional
from urllib.parse import urlparse

from pydantic import BaseModel, Field, field_validator


class SocialNetworks(BaseModel):
    """Модель данных социальных сетей предприятия"""

    telegram: Optional[str] = Field(None, description="Ссылка на Telegram")
    whatsapp: Optional[str] = Field(None, description="Ссылка на WhatsApp")
    vk: Optional[str] = Field(None, description="Ссылка на ВКонтакте")

    @field_validator("telegram")
    @classmethod
    def validate_telegram(cls, v: Optional[str]) -> Optional[str]:
        """Валидация ссылки на Telegram"""
        if not v:
            return None

        v = v.strip()
        if not v:
            return None

        # Валидные форматы Telegram
        valid_patterns = [
            r"^https?://t\.me/[\w_]+",
            r"^https?://telegram\.me/[\w_]+",
            r"^https?://telegram\.org/[\w_]+",
            r"^@[\w_]+$",  # Простое имя пользователя
        ]

        # Если это простое имя пользователя, конвертируем в полную ссылку
        if v.startswith("@"):
            return f"https://t.me/{v[1:]}"

        # Проверяем валидность URL
        if not any(re.match(pattern, v, re.IGNORECASE) for pattern in valid_patterns):
            # Если не соответствует паттернам, пробуем дополнить URL
            if not v.startswith("http"):
                if v.startswith("t.me/") or v.startswith("telegram.me/"):
                    return f"https://{v}"
                else:
                    return f"https://t.me/{v}"

        return v

    @field_validator("whatsapp")
    @classmethod
    def validate_whatsapp(cls, v: Optional[str]) -> Optional[str]:
        """Валидация ссылки на WhatsApp"""
        if not v:
            return None

        v = v.strip()
        if not v:
            return None

        # Валидные форматы WhatsApp
        valid_patterns = [
            r"^https?://wa\.me/\d+",
            r"^https?://api\.whatsapp\.com/send\?phone=\d+",
            r"^https?://whatsapp\.com/[\w/]+",
            r"^\+?\d{10,15}$",  # Простой номер телефона
        ]

        # Если это номер телефона, конвертируем в wa.me ссылку
        if re.match(r"^\+?\d{10,15}$", v):
            phone = re.sub(r"[^\d]", "", v)
            return f"https://wa.me/{phone}"

        # Проверяем валидность URL
        if not any(re.match(pattern, v, re.IGNORECASE) for pattern in valid_patterns):
            # Если не соответствует паттернам, возвращаем как есть
            pass

        return v

    @field_validator("vk")
    @classmethod
    def validate_vk(cls, v: Optional[str]) -> Optional[str]:
        """Валидация ссылки на ВКонтакте"""
        if not v:
            return None

        v = v.strip()
        if not v:
            return None

        # Валидные форматы VK
        valid_patterns = [
            r"^https?://vk\.com/[\w.]+",
            r"^https?://m\.vk\.com/[\w.]+",
        ]

        # Если это простое имя пользователя, дополняем
        if not v.startswith("http") and not "/" in v:
            return f"https://vk.com/{v}"

        # Проверяем валидность URL
        if not any(re.match(pattern, v, re.IGNORECASE) for pattern in valid_patterns):
            if not v.startswith("http"):
                return f"https://vk.com/{v}"

        return v

    def get_active_networks(self) -> Dict[str, str]:
        """Получить словарь активных социальных сетей"""
        networks = {}
        for field_name, field_value in self.model_dump().items():
            if field_value:
                networks[field_name] = field_value
        return networks

    def get_network_usernames(self) -> Dict[str, Optional[str]]:
        """Извлечь имена пользователей из ссылок"""
        usernames = {}

        for network, url in self.get_active_networks().items():
            if not url:
                usernames[network] = None
                continue

            try:
                parsed = urlparse(url)
                path = parsed.path.strip("/")

                if network == "telegram":
                    # t.me/username
                    usernames[network] = path if path else None
                elif network == "whatsapp":
                    # wa.me/phone или номер телефона
                    usernames[network] = path if path else None
                else:
                    usernames[network] = path

            except Exception:
                usernames[network] = None

        return usernames

    def has_any_network(self) -> bool:
        """Проверить, есть ли хотя бы одна активная социальная сеть"""
        return any(self.model_dump().values())

    def get_networks_count(self) -> int:
        """Получить количество активных социальных сетей"""
        return len(self.get_active_networks())

    class Config:
        """Конфигурация модели"""

        json_schema_extra = {
            "example": {
                "telegram": "https://t.me/username",
                "whatsapp": "https://wa.me/79991234567",
                "vk": "https://vk.com/username",
            }
        }
