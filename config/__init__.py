"""
Пакет конфигурации для системы скрапинга Яндекс.Карт
"""

from .chrome_config import ChromeConfig, user_agent_rotator
from .settings import settings

__all__ = ["settings", "ChromeConfig", "user_agent_rotator"]
