"""
Улучшенная конфигурация Chrome WebDriver для максимальной стабильности и производительности
"""

import os
import random
import tempfile
import time
from typing import Dict, List, Optional

from fake_useragent import UserAgent
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from webdriver_manager.chrome import ChromeDriverManager

from .settings import settings


class ChromeConfig:
    """Улучшенный класс для настройки Chrome WebDriver"""

    # Список реалистичных User-Agent'ов для ротации
    REALISTIC_USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    ]

    @staticmethod
    def get_chrome_options(
        user_agent: Optional[str] = None, stealth_mode: bool = True
    ) -> Options:
        """
        Получить улучшенные настройки Chrome для скрапинга

        Args:
            user_agent: Пользовательский User-Agent
            stealth_mode: Включить режим скрытности

        Returns:
            Options: Настроенные опции Chrome
        """
        options = Options()

        # Базовые настройки безопасности и производительности
        base_args = [
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--disable-web-security",
            "--disable-features=VizDisplayCompositor",
            "--disable-extensions",
            "--disable-plugins",
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

        # Режим скрытности - дополнительные опции для избежания детекции
        if stealth_mode:
            stealth_args = [
                "--disable-blink-features=AutomationControlled",
                "--disable-client-side-phishing-detection",
                "--disable-sync",
                "--disable-background-networking",
                "--disable-background-timer-throttling",
                "--disable-renderer-backgrounding",
                "--disable-backgrounding-occluded-windows",
                "--disable-features=TranslateUI",
                "--hide-scrollbars",
                "--mute-audio",
                "--no-zygote",
                "--disable-logging",
                "--disable-login-animations",
                "--disable-notifications",
                "--disable-permissions-api",
                "--disable-web-security",
                "--allow-running-insecure-content",
                "--ignore-certificate-errors",
                "--ignore-ssl-errors",
                "--ignore-certificate-errors-spki-list",
            ]
            base_args.extend(stealth_args)

        # Оптимизации производительности
        performance_args = [
            "--aggressive-cache-discard",
            "--memory-pressure-off",
            "--max_old_space_size=4096",
        ]
        base_args.extend(performance_args)

        # Headless режим
        if settings.HEADLESS:
            base_args.append("--headless=new")

        # Добавляем все аргументы
        for arg in base_args:
            options.add_argument(arg)

        # User-Agent
        if user_agent is None and settings.ROTATE_USER_AGENT:
            user_agent = random.choice(ChromeConfig.REALISTIC_USER_AGENTS)

        if user_agent:
            options.add_argument(f"--user-agent={user_agent}")

        # Расширенные настройки приватности и безопасности
        prefs = {
            "profile.default_content_setting_values": {
                "notifications": 2,
                "media_stream": 2,
                "geolocation": 2,
                "plugins": 2,
                "popups": 2,
                "automatic_downloads": 2,
                "mixed_script": 2,
                "media_stream_mic": 2,
                "media_stream_camera": 2,
                "protocol_handlers": 2,
                "ppapi_broker": 2,
                "metro_switch_to_desktop": 2,
                "protected_media_identifier": 2,
                "app_banner": 2,
                "site_engagement": 2,
                "durable_storage": 2,
            },
            "profile.managed_default_content_settings": {
                "images": 2,  # Блокируем изображения для скорости
            },
            "profile.default_content_settings": {"popups": 0},
            "managed.default_content_settings": {"popups": 0},
            "profile.password_manager_enabled": False,
            "profile.default_content_setting_values.notifications": 2,
            "credentials_enable_service": False,
            "password_manager_enabled": False,
        }
        options.add_experimental_option("prefs", prefs)

        # Отключаем логирование и автоматизацию
        options.add_experimental_option(
            "excludeSwitches",
            ["enable-logging", "enable-automation", "disable-extensions"],
        )
        options.add_experimental_option("useAutomationExtension", False)

        # Дополнительные capabilities для стабильности
        caps = DesiredCapabilities.CHROME
        caps["goog:loggingPrefs"] = {"driver": "OFF", "server": "OFF", "browser": "OFF"}
        options.add_experimental_option("detach", True)

        return options

    @staticmethod
    def get_chrome_service(custom_path: Optional[str] = None) -> Service:
        """
        Получить сервис Chrome с автоматическим управлением драйвером

        Args:
            custom_path: Путь к кастомному ChromeDriver

        Returns:
            Service: Настроенный сервис Chrome
        """
        if custom_path and os.path.exists(custom_path):
            service = Service(custom_path)
        else:
            # Автоматическая загрузка и установка ChromeDriver
            driver_path = ChromeDriverManager().install()
            if driver_path.endswith('THIRD_PARTY_NOTICES.chromedriver'):
                driver_path = driver_path.replace('THIRD_PARTY_NOTICES.chromedriver', 'chromedriver.exe')
            service = Service(driver_path)

        # Настройки сервиса для стабильности
        service.creation_flags = 0x08000000  # CREATE_NO_WINDOW на Windows

        return service

    @staticmethod
    def create_driver(
        user_agent: Optional[str] = None, config: Optional[Dict] = None
    ) -> webdriver.Chrome:
        """
        Создать полностью настроенный экземпляр Chrome WebDriver

        Args:
            user_agent: Пользовательский User-Agent
            config: Дополнительная конфигурация

        Returns:
            webdriver.Chrome: Настроенный драйвер
        """
        config = config or {}

        # Получаем опции и сервис
        options = ChromeConfig.get_chrome_options(
            user_agent=user_agent, stealth_mode=config.get("stealth_mode", True)
        )
        service = ChromeConfig.get_chrome_service(config.get("chromedriver_path"))

        # Создаем временную директорию для профиля пользователя
        temp_profile = tempfile.mkdtemp()
        options.add_argument(f"--user-data-dir={temp_profile}")

        # Создаем драйвер
        driver = webdriver.Chrome(service=service, options=options)

        # Настройки таймаутов
        driver.set_page_load_timeout(settings.PAGE_LOAD_TIMEOUT)
        driver.implicitly_wait(settings.ELEMENT_WAIT_TIMEOUT)

        # Применяем дополнительные скрипты для маскировки автоматизации
        ChromeConfig._apply_stealth_scripts(driver)

        # Настройка размера окна с небольшой случайностью
        if not settings.HEADLESS:
            width, height = map(int, settings.WINDOW_SIZE.split(","))
            # Добавляем небольшую случайность к размерам
            width += random.randint(-50, 50)
            height += random.randint(-30, 30)
            driver.set_window_size(width, height)

        return driver

    @staticmethod
    def _apply_stealth_scripts(driver: webdriver.Chrome):
        """Применение скриптов для маскировки автоматизации"""

        # Скрываем признаки webdriver
        driver.execute_script(
            """
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined,
            });
        """
        )

        # Маскируем другие признаки автоматизации
        driver.execute_script(
            """
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en'],
            });
        """
        )

        driver.execute_script(
            """
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5],
            });
        """
        )

        # Эмулируем поведение реального пользователя
        driver.execute_script(
            """
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                    Promise.resolve({ state: Notification.permission }) :
                    originalQuery(parameters)
            );
        """
        )

        # Добавляем случайные свойства для большей реалистичности
        driver.execute_script(
            f"""
            Object.defineProperty(navigator, 'hardwareConcurrency', {{
                get: () => {random.randint(2, 8)},
            }});
        """
        )

        driver.execute_script(
            f"""
            Object.defineProperty(screen, 'colorDepth', {{
                get: () => {random.choice([24, 32])},
            }});
        """
        )


class AdvancedUserAgentRotator:
    """Продвинутый ротатор User-Agent'ов с аналитикой"""

    def __init__(self):
        self.ua = UserAgent()
        self.used_agents = {}  # Словарь с историей использования
        self.success_rates = {}  # Статистика успешности агентов
        self.last_rotation = 0

    def get_best_user_agent(self) -> str:
        """Получить лучший User-Agent на основе статистики"""

        current_time = time.time()

        # Ротация не чаще чем раз в минуту
        if current_time - self.last_rotation < 60:
            if hasattr(self, "_last_agent"):
                return self._last_agent

        # Если есть статистика, выбираем агент с лучшим success rate
        if self.success_rates:
            best_agent = max(
                self.success_rates.keys(),
                key=lambda x: self.success_rates[x]["success_rate"],
            )

            # Но не используем один и тот же агент слишком часто
            if self.used_agents.get(best_agent, 0) < 10:
                self._last_agent = best_agent
                self.last_rotation = current_time
                return best_agent

        # Выбираем случайный реалистичный агент
        agent = random.choice(ChromeConfig.REALISTIC_USER_AGENTS)
        self._last_agent = agent
        self.last_rotation = current_time
        return agent

    def record_success(self, user_agent: str, success: bool):
        """Записать результат использования User-Agent"""

        if user_agent not in self.success_rates:
            self.success_rates[user_agent] = {
                "total": 0,
                "successes": 0,
                "success_rate": 0.0,
            }

        stats = self.success_rates[user_agent]
        stats["total"] += 1
        if success:
            stats["successes"] += 1

        stats["success_rate"] = stats["successes"] / stats["total"]

        # Обновляем счетчик использования
        self.used_agents[user_agent] = self.used_agents.get(user_agent, 0) + 1

    def get_statistics(self) -> Dict:
        """Получить статистику User-Agent'ов"""
        return {
            "total_agents_used": len(self.used_agents),
            "total_requests": sum(self.used_agents.values()),
            "success_rates": dict(
                sorted(
                    self.success_rates.items(),
                    key=lambda x: x[1]["success_rate"],
                    reverse=True,
                )
            ),
            "usage_counts": dict(
                sorted(self.used_agents.items(), key=lambda x: x[1], reverse=True)
            ),
        }


# Глобальные экземпляры
user_agent_rotator = AdvancedUserAgentRotator()


class BrowserPool:
    """Пул браузеров для более эффективного использования ресурсов"""

    def __init__(self, max_size: int = 3):
        self.max_size = max_size
        self.pool: List[webdriver.Chrome] = []
        self.in_use: set = set()

    def get_driver(self, config: Optional[Dict] = None) -> webdriver.Chrome:
        """Получить драйвер из пула или создать новый"""

        # Ищем свободный драйвер в пуле
        for driver in self.pool:
            if id(driver) not in self.in_use:
                try:
                    # Проверяем, что драйвер еще живой
                    driver.current_url
                    self.in_use.add(id(driver))
                    return driver
                except:
                    # Удаляем мертвый драйвер
                    self.pool.remove(driver)
                    continue

        # Создаем новый драйвер
        driver = ChromeConfig.create_driver(config=config)
        self.pool.append(driver)
        self.in_use.add(id(driver))

        # Ограничиваем размер пула
        if len(self.pool) > self.max_size:
            old_driver = self.pool.pop(0)
            try:
                old_driver.quit()
            except:
                pass

        return driver

    def release_driver(self, driver: webdriver.Chrome):
        """Вернуть драйвер в пул"""
        driver_id = id(driver)
        if driver_id in self.in_use:
            self.in_use.remove(driver_id)

    def cleanup(self):
        """Закрыть все драйверы в пуле"""
        for driver in self.pool:
            try:
                driver.quit()
            except:
                pass
        self.pool.clear()
        self.in_use.clear()


# Глобальный пул браузеров (для будущего использования)
browser_pool = BrowserPool(max_size=2)
