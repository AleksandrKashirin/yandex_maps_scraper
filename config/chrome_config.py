"""
Конфигурация Chrome WebDriver для безопасного и эффективного скрапинга
"""

import random
from typing import Optional

from fake_useragent import UserAgent
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from .settings import settings


class ChromeConfig:
    """Класс для настройки Chrome WebDriver"""

    @staticmethod
    def get_chrome_options(user_agent: Optional[str] = None) -> Options:
        """
        Получить настройки Chrome для скрапинга

        Args:
            user_agent: Пользовательский User-Agent, если None - генерируется случайно

        Returns:
            Options: Настроенные опции Chrome
        """
        options = Options()

        # Базовые настройки безопасности и производительности
        chrome_args = [
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--disable-web-security",
            "--disable-features=VizDisplayCompositor",
            "--disable-extensions",
            "--disable-plugins",
            "--disable-images",  # Отключаем загрузку изображений для скорости
            "--disable-javascript",  # Может понадобиться включить для динамического контента
            "--disable-dev-tools",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-default-apps",
            "--disable-popup-blocking",
            "--disable-translate",
            "--disable-background-timer-throttling",
            "--disable-renderer-backgrounding",
            "--disable-backgrounding-occluded-windows",
            "--disable-ipc-flooding-protection",
            f"--window-size={settings.WINDOW_SIZE}",
        ]

        # Headless режим
        if settings.HEADLESS:
            chrome_args.append("--headless=new")

        # Добавляем все аргументы
        for arg in chrome_args:
            options.add_argument(arg)

        # User-Agent
        if user_agent is None and settings.ROTATE_USER_AGENT:
            ua = UserAgent()
            user_agent = ua.random

        if user_agent:
            options.add_argument(f"--user-agent={user_agent}")

        # Настройки приватности
        prefs = {
            "profile.default_content_setting_values": {
                "notifications": 2,
                "media_stream": 2,
                "geolocation": 2,
            },
            "profile.managed_default_content_settings": {
                "images": 2,  # Блокируем изображения
            },
        }
        options.add_experimental_option("prefs", prefs)

        # Отключаем логи
        options.add_experimental_option("excludeSwitches", ["enable-logging"])
        options.add_experimental_option("useAutomationExtension", False)

        return options

    @staticmethod
    def get_chrome_service() -> Service:
        """
        Получить сервис Chrome с автоматическим управлением драйвером

        Returns:
            Service: Настроенный сервис Chrome
        """
        service = Service(ChromeDriverManager().install())
        return service

    @staticmethod
    def create_driver(user_agent: Optional[str] = None) -> webdriver.Chrome:
        """
        Создать настроенный экземпляр Chrome WebDriver

        Args:
            user_agent: Пользовательский User-Agent

        Returns:
            webdriver.Chrome: Настроенный драйвер
        """
        options = ChromeConfig.get_chrome_options(user_agent)
        service = ChromeConfig.get_chrome_service()

        driver = webdriver.Chrome(service=service, options=options)

        # Настройки таймаутов
        driver.set_page_load_timeout(settings.PAGE_LOAD_TIMEOUT)
        driver.implicitly_wait(settings.ELEMENT_WAIT_TIMEOUT)

        # Скрываем признаки автоматизации
        driver.execute_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

        return driver


class UserAgentRotator:
    """Класс для ротации User-Agent'ов"""

    def __init__(self):
        self.ua = UserAgent()
        self.used_agents = set()

    def get_random_user_agent(self) -> str:
        """Получить случайный User-Agent"""
        # Если использовали больше 50 агентов, очищаем список
        if len(self.used_agents) > 50:
            self.used_agents.clear()

        # Генерируем новый агент до тех пор, пока не получим неиспользованный
        attempts = 0
        while attempts < 10:
            agent = self.ua.random
            if agent not in self.used_agents:
                self.used_agents.add(agent)
                return agent
            attempts += 1

        # Если не удалось найти новый, возвращаем случайный
        return self.ua.random

    def get_chrome_user_agent(self) -> str:
        """Получить User-Agent для Chrome"""
        return self.ua.chrome


# Глобальный экземпляр ротатора
user_agent_rotator = UserAgentRotator()
