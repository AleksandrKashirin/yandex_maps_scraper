"""
Модуль навигации для взаимодействия с элементами Яндекс.Карт
"""

import random
import time
from typing import Any, List, Optional

from selenium import webdriver
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    NoSuchElementException,
    StaleElementReferenceException,
    TimeoutException,
)
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from config.settings import settings
from core.logger import get_logger

from .selectors import SelectorConfig, selectors


class NavigationError(Exception):
    """Исключение для ошибок навигации"""

    pass


class YandexMapsNavigator:
    """Класс для навигации по страницам Яндекс.Карт"""

    def __init__(self, driver: webdriver.Chrome):
        self.driver = driver
        self.logger = get_logger(__name__)
        self.wait = WebDriverWait(driver, settings.ELEMENT_WAIT_TIMEOUT)

    def wait_for_page_load(self, timeout: int = None) -> bool:
        """
        Ожидание полной загрузки страницы

        Args:
            timeout: Таймаут ожидания в секундах

        Returns:
            bool: True если страница загружена
        """
        if timeout is None:
            timeout = settings.PAGE_LOAD_TIMEOUT

        try:
            # Ждем, пока страница полностью загрузится
            self.wait.until(
                lambda driver: driver.execute_script("return document.readyState")
                == "complete"
            )

            # Дополнительная пауза для загрузки динамического контента
            time.sleep(random.uniform(1, 3))

            self.logger.debug("Страница полностью загружена")
            return True

        except TimeoutException:
            self.logger.warning(f"Таймаут ожидания загрузки страницы ({timeout}s)")
            return False

    def find_element_with_fallback(
        self, config: SelectorConfig, parent_element: Any = None
    ) -> Optional[Any]:
        """
        Поиск элемента с использованием fallback селекторов

        Args:
            config: Конфигурация селектора
            parent_element: Родительский элемент для поиска

        Returns:
            WebElement или None если элемент не найден
        """

        self.logger.info(f"Поиск элемента с CSS: {config.css}, XPath: {config.xpath}")

        search_context = parent_element if parent_element else self.driver

        # Список всех селекторов для попытки
        selectors_to_try = []

        if config.css:
            selectors_to_try.append(("css", config.css))
        if config.xpath:
            selectors_to_try.append(("xpath", config.xpath))
        if config.fallback_selectors:
            for selector in config.fallback_selectors:
                # Определяем тип селектора
                if selector.startswith("//") or selector.startswith("./"):
                    selectors_to_try.append(("xpath", selector))
                else:
                    selectors_to_try.append(("css", selector))

        # Пробуем каждый селектор
        for selector_type, selector in selectors_to_try:
            try:
                if config.multiple:
                    if selector_type == "css":
                        elements = search_context.find_elements(
                            By.CSS_SELECTOR, selector
                        )
                    else:
                        elements = search_context.find_elements(By.XPATH, selector)

                    if elements:
                        self.logger.debug(
                            f"Найдено {len(elements)} элементов с селектором: {selector}"
                        )
                        return elements
                else:
                    if selector_type == "css":
                        element = search_context.find_element(By.CSS_SELECTOR, selector)
                    else:
                        element = search_context.find_element(By.XPATH, selector)

                    self.logger.debug(f"Элемент найден с селектором: {selector}")
                    return element

            except (NoSuchElementException, StaleElementReferenceException):
                continue
            except Exception as e:
                self.logger.warning(f"Ошибка при поиске с селектором {selector}: {e}")
                continue

        self.logger.warning("Элемент не найден ни с одним из селекторов")
        return None

    def wait_and_find_element(
        self, config: SelectorConfig, timeout: int = None
    ) -> Optional[Any]:
        """
        Ожидание и поиск элемента

        Args:
            config: Конфигурация селектора
            timeout: Таймаут ожидания

        Returns:
            WebElement или None
        """
        if timeout is None:
            timeout = settings.ELEMENT_WAIT_TIMEOUT

        try:
            # Пробуем основной селектор
            if config.css:
                element = WebDriverWait(self.driver, timeout).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, config.css))
                )
                return element
            elif config.xpath:
                element = WebDriverWait(self.driver, timeout).until(
                    EC.presence_of_element_located((By.XPATH, config.xpath))
                )
                return element
        except TimeoutException:
            # Если основной селектор не сработал, пробуем fallback
            self.logger.debug("Основной селектор не сработал, пробуем fallback")
            return self.find_element_with_fallback(config)

        return None

    def safe_click(self, element: Any, max_attempts: int = 3) -> bool:
        """
        Безопасный клик по элементу с повторными попытками

        Args:
            element: Элемент для клика
            max_attempts: Максимальное количество попыток

        Returns:
            bool: True если клик успешен
        """
        for attempt in range(max_attempts):
            try:
                # Прокручиваем к элементу
                self.driver.execute_script(
                    "arguments[0].scrollIntoView(true);", element
                )
                time.sleep(0.5)

                # Ждем, пока элемент станет кликабельным
                WebDriverWait(self.driver, 5).until(EC.element_to_be_clickable(element))

                # Пробуем обычный клик
                element.click()

                self.logger.debug(f"Клик выполнен успешно с попытки {attempt + 1}")
                return True

            except ElementClickInterceptedException:
                # Если элемент перекрыт, пробуем JavaScript клик
                try:
                    self.driver.execute_script("arguments[0].click();", element)
                    self.logger.debug(
                        f"JavaScript клик выполнен с попытки {attempt + 1}"
                    )
                    return True
                except Exception as e:
                    self.logger.warning(f"JavaScript клик не удался: {e}")

            except StaleElementReferenceException:
                self.logger.warning(f"Элемент устарел на попытке {attempt + 1}")
                return False

            except Exception as e:
                self.logger.warning(f"Ошибка клика на попытке {attempt + 1}: {e}")

            # Пауза между попытками
            time.sleep(1)

        self.logger.error(f"Не удалось кликнуть после {max_attempts} попыток")
        return False

    def navigate_to_services_tab(self) -> bool:
        """
        Переход на вкладку услуг

        Returns:
            bool: True если переход успешен
        """
        self.logger.info("Переходим на вкладку услуг")

        services_tab = self.find_element_with_fallback(
            selectors.NAVIGATION["services_tab"]
        )

        if not services_tab:
            self.logger.warning("Вкладка услуг не найдена")
            return False

        if self.safe_click(services_tab):
            # Ждем загрузки контента услуг
            time.sleep(random.uniform(2, 4))
            self.logger.info("Переход на вкладку услуг выполнен")
            return True

        return False

    def navigate_to_reviews_tab(self) -> bool:
        """
        Переход на вкладку отзывов

        Returns:
            bool: True если переход успешен
        """
        self.logger.info("Переходим на вкладку отзывов")

        reviews_tab = self.find_element_with_fallback(
            selectors.NAVIGATION["reviews_tab"]
        )

        if not reviews_tab:
            self.logger.warning("Вкладка отзывов не найдена")
            return False

        if self.safe_click(reviews_tab):
            # Ждем загрузки отзывов
            time.sleep(random.uniform(2, 4))
            self.logger.info("Переход на вкладку отзывов выполнен")
            return True

        return False

    def load_more_content(self, content_type: str = "services") -> int:
        """
        Загрузка дополнительного контента (услуги или отзывы)

        Args:
            content_type: Тип контента ("services" или "reviews")

        Returns:
            int: Количество загруженных элементов
        """
        loaded_count = 0
        max_loads = 5
        
        selector_key = f"show_more_{content_type}"
        
        if selector_key not in selectors.LOADING:
            self.logger.debug(f"Селектор для загрузки {content_type} не найден, пропускаем")
            return 0

        for i in range(max_loads):
            show_more_btn = self.find_element_with_fallback(
                selectors.LOADING[selector_key]
            )

            if not show_more_btn:
                self.logger.debug(f"Кнопка 'Показать ещё' для {content_type} не найдена, завершаем загрузку")
                break

            # Проверяем, что кнопка видна и активна
            if not show_more_btn.is_displayed() or not show_more_btn.is_enabled():
                self.logger.debug(f"Кнопка 'Показать ещё' для {content_type} неактивна")
                break

            if self.safe_click(show_more_btn):
                loaded_count += 1
                self.logger.debug(
                    f"Загружен дополнительный контент {content_type} #{i + 1}"
                )

                # Ждем загрузки нового контента
                time.sleep(random.uniform(2, 5))

                # Проверяем наличие индикатора загрузки
                self._wait_for_loading_complete()
            else:
                break

        if loaded_count > 0:
            self.logger.info(
                f"Загружено {loaded_count} дополнительных блоков {content_type}"
            )

        return loaded_count

    def _wait_for_loading_complete(self, timeout: int = 10):
        """
        Ожидание завершения загрузки контента

        Args:
            timeout: Таймаут ожидания
        """
        try:
            # Ждем исчезновения индикатора загрузки
            WebDriverWait(self.driver, timeout).until_not(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".loading, .spinner"))
            )
        except TimeoutException:
            # Если индикатор не найден или не исчез, просто ждем
            time.sleep(1)

    def random_delay(self, min_delay: float = None, max_delay: float = None):
        """
        Случайная задержка между действиями

        Args:
            min_delay: Минимальная задержка
            max_delay: Максимальная задержка
        """
        if min_delay is None:
            min_delay = settings.MIN_DELAY
        if max_delay is None:
            max_delay = settings.MAX_DELAY

        delay = random.uniform(min_delay, max_delay)
        self.logger.debug(f"Пауза {delay:.2f} секунд")
        time.sleep(delay)

    def simulate_human_behavior(self):
        """Имитация поведения человека (прокрутка, движения мыши)"""
        try:
            # Случайная прокрутка страницы
            scroll_amount = random.randint(100, 500)
            self.driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
            time.sleep(random.uniform(0.5, 1.5))

            # Случайные движения мыши
            actions = ActionChains(self.driver)
            actions.move_by_offset(random.randint(-50, 50), random.randint(-50, 50))
            actions.perform()

        except Exception as e:
            self.logger.debug(f"Ошибка при имитации поведения: {e}")

    def check_for_captcha(self) -> bool:
        """
        Проверка наличия капчи на странице

        Returns:
            bool: True если обнаружена капча
        """
        captcha_selectors = [
            ".captcha",
            "[data-testid='captcha']",
            ".g-recaptcha",
            "iframe[src*='recaptcha']",
            ".yandex-captcha",
        ]

        for selector in captcha_selectors:
            try:
                element = self.driver.find_element(By.CSS_SELECTOR, selector)
                if element.is_displayed():
                    self.logger.warning("Обнаружена капча на странице")
                    return True
            except NoSuchElementException:
                continue

        return False
