"""
Основной класс для скрапинга данных с Яндекс.Карт
"""

import json
import re
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urlparse

from pydantic import BaseModel, Field, ValidationError
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from tenacity import retry, stop_after_attempt, wait_exponential

from config.chrome_config import ChromeConfig, user_agent_rotator
from config.settings import settings
from core.logger import get_logger, log_scraping_stats, scraping_metrics

from .navigation import NavigationError, YandexMapsNavigator
from .selectors import selectors


class ServiceData(BaseModel):
    """Модель данных услуги"""

    name: str
    price: Optional[str] = None
    price_from: Optional[str] = None
    price_to: Optional[str] = None
    description: Optional[str] = None
    duration: Optional[str] = None


class SocialNetworks(BaseModel):
    """Модель социальных сетей"""

    telegram: Optional[str] = None
    whatsapp: Optional[str] = None
    vk: Optional[str] = None


class WorkingHours(BaseModel):
    """Модель рабочих часов"""

    current_status: Optional[str] = None
    schedule: Optional[Dict[str, str]] = None
    notes: Optional[str] = None


class ReviewData(BaseModel):
    """Модель отзыва"""

    author: str
    rating: Optional[int] = None
    date: Optional[str] = None
    text: Optional[str] = None
    response: Optional[str] = None


class BusinessData(BaseModel):
    """Модель данных предприятия"""

    name: str
    category: Optional[str] = None
    services: List[ServiceData] = Field(default_factory=list)
    website: Optional[str] = None
    address: Optional[str] = None
    social_networks: SocialNetworks = Field(default_factory=SocialNetworks)
    phone: Optional[str] = None
    working_hours: WorkingHours = Field(default_factory=WorkingHours)
    rating: Optional[float] = None
    reviews_count: Optional[int] = None
    reviews: List[ReviewData] = Field(default_factory=list)
    scraping_date: datetime = Field(default_factory=datetime.now)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class YandexMapsScraper:
    """Основной класс для скрапинга Яндекс.Карт"""

    def __init__(self, config: Optional[Dict] = None):
        """
        Инициализация скрапера

        Args:
            config: Дополнительная конфигурация
        """
        self.config = config or {}
        self.logger = get_logger(__name__)
        self.driver: Optional[webdriver.Chrome] = None
        self.navigator: Optional[YandexMapsNavigator] = None
        self.start_time: Optional[float] = None

    def __enter__(self):
        """Контекстный менеджер - вход"""
        self.initialize_driver()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Контекстный менеджер - выход"""
        self.cleanup()

    def initialize_driver(self):
        """Инициализация WebDriver"""
        try:
            self.logger.info("Инициализация Chrome WebDriver")

            # Получаем новый User-Agent
            user_agent = None
            if settings.ROTATE_USER_AGENT:
                user_agent = user_agent_rotator.get_best_user_agent()
                self.logger.debug(f"Используется User-Agent: {user_agent[:50]}...")

            # Создаем драйвер
            self.driver = ChromeConfig.create_driver(user_agent)
            self.navigator = YandexMapsNavigator(self.driver)

            self.logger.info("WebDriver успешно инициализирован")

        except Exception as e:
            self.logger.error(f"Ошибка инициализации WebDriver: {e}")
            raise

    def cleanup(self):
        """Очистка ресурсов"""
        if self.driver:
            try:
                self.driver.quit()
                self.logger.info("WebDriver закрыт")
            except Exception as e:
                self.logger.warning(f"Ошибка при закрытии WebDriver: {e}")

    def validate_url(self, url: str) -> bool:
        """
        Валидация URL Яндекс.Карт

        Args:
            url: URL для валидации

        Returns:
            bool: True если URL валиден
        """
        try:
            parsed = urlparse(url)

            # Проверяем схему
            if parsed.scheme not in ["http", "https"]:
                return False

            # Проверяем домен
            domain_valid = any(
                domain in parsed.netloc for domain in settings.YANDEX_MAPS_DOMAINS
            )

            # Проверяем путь
            path_valid = "/maps/" in parsed.path

            return domain_valid and path_valid

        except Exception:
            return False

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=4, max=10)
    )
    def load_page(self, url: str) -> bool:
        """
        Загрузка страницы с повторными попытками

        Args:
            url: URL страницы

        Returns:
            bool: True если страница загружена успешно
        """
        if not self.validate_url(url):
            raise ValueError(f"Невалидный URL: {url}")

        self.logger.info(f"Загрузка страницы: {url}")
        self.start_time = time.time()

        try:
            self.driver.get(url)

            # Ждем загрузки страницы
            if not self.navigator.wait_for_page_load():
                raise TimeoutException("Страница не загрузилась в отведенное время")

            # Проверяем на капчу
            if self.navigator.check_for_captcha():
                raise NavigationError("Обнаружена капча")

            # Имитируем поведение человека
            self.navigator.simulate_human_behavior()

            self.logger.info("Страница успешно загружена")

            return True

        except Exception as e:
            self.logger.error(f"Ошибка загрузки страницы: {e}")
            raise

    def extract_basic_info(self) -> Dict[str, Any]:
        """
        Извлечение базовой информации о предприятии

        Returns:
            Dict: Словарь с базовой информацией
        """
        self.logger.info("Извлечение базовой информации")

        data = {}

        # Название
        name_element = self.navigator.find_element_with_fallback(
            selectors.BASIC_INFO["name"]
        )
        if name_element:
            data["name"] = name_element.text.strip()
            self.logger.debug(f"Название: {data['name']}")

        # Категория
        category_element = self.navigator.find_element_with_fallback(
            selectors.BASIC_INFO["category"]
        )
        if category_element:
            data["category"] = category_element.text.strip()
            self.logger.debug(f"Категория: {data['category']}")

        # Рейтинг
        rating_element = self.navigator.find_element_with_fallback(
            selectors.BASIC_INFO["rating"]
        )
        if rating_element:
            rating_text = rating_element.text.strip()
            # Извлекаем числовое значение рейтинга
            rating_match = re.search(r"(\d+[\.,]\d+)", rating_text)
            if rating_match:
                data["rating"] = float(rating_match.group(1).replace(",", "."))
                self.logger.debug(f"Рейтинг: {data['rating']}")

        # Количество отзывов
        reviews_count_element = self.navigator.find_element_with_fallback(
            selectors.BASIC_INFO["reviews_count"]
        )
        if reviews_count_element:
            reviews_text = reviews_count_element.text.strip()
            # Извлекаем числовое значение
            reviews_match = re.search(r"(\d+)", reviews_text)
            if reviews_match:
                data["reviews_count"] = int(reviews_match.group(1))
                self.logger.debug(f"Количество отзывов: {data['reviews_count']}")

        # Адрес
        address_element = self.navigator.find_element_with_fallback(
            selectors.BASIC_INFO["address"]
        )
        if address_element:
            data["address"] = address_element.text.strip()
            self.logger.debug(f"Адрес: {data['address']}")

        # Телефон
        phone_elements = self.navigator.find_element_with_fallback(
            selectors.BASIC_INFO["phone"]
        )
        if phone_elements:
            if isinstance(phone_elements, list):
                # Берем первый телефон
                data["phone"] = phone_elements[0].text.strip()
            else:
                data["phone"] = phone_elements.text.strip()
            self.logger.debug(f"Телефон: {data['phone']}")

        # Сайт
        website_element = self.navigator.find_element_with_fallback(
            selectors.BASIC_INFO["website"]
        )
        if website_element:
            href = website_element.get_attribute("href")
            if href:
                data["website"] = href
                self.logger.debug(f"Сайт: {data['website']}")

        # Часы работы
        working_hours_element = self.navigator.find_element_with_fallback(
            selectors.BASIC_INFO["working_hours"]
        )
        if working_hours_element:
            data["working_hours"] = {
                "current_status": working_hours_element.text.strip()
            }
            self.logger.debug(
                f"Статус работы: {data['working_hours']['current_status']}"
            )

        return data

    def extract_social_networks(self) -> Dict[str, str]:
        """
        Извлечение ссылок на социальные сети

        Returns:
            Dict: Словарь с социальными сетями
        """
        self.logger.debug("Извлечение социальных сетей")

        social_data = {}

        for network, config in selectors.SOCIAL_NETWORKS.items():
            element = self.navigator.find_element_with_fallback(config)
            if element:
                href = element.get_attribute("href")
                if href:
                    social_data[network] = href
                    self.logger.debug(f"{network}: {href}")

        return social_data

    def navigate_to_services_tab(self) -> bool:
        """
        Переход на вкладку услуг

        Returns:
            bool: True если переход успешен
        """
        return self.navigator.navigate_to_services_tab()

    def extract_services(self) -> List[Dict[str, Any]]:
        """
        Извлечение услуг и цен

        Returns:
            List: Список услуг
        """
        self.logger.info("Извлечение услуг")
        services = []

        # Загружаем дополнительные услуги если есть
        self.navigator.load_more_content("services")

        # Находим все элементы услуг
        service_elements = self.navigator.find_element_with_fallback(
            selectors.SERVICES["service_items"]
        )

        if not service_elements:
            self.logger.warning("Услуги не найдены")
            return services

        if not isinstance(service_elements, list):
            service_elements = [service_elements]

        total_elements = len(service_elements)
        self.logger.info(f"Найдено {total_elements} элементов услуг")

        # Progress bar setup
        progress_step = max(1, total_elements // 10)  # Обновляем каждые 10%

        for i, element in enumerate(service_elements, 1):
            try:
                # Показываем прогресс
                if i % progress_step == 0 or i == total_elements:
                    percentage = (i / total_elements) * 100
                    filled = int(percentage // 5)  # 20 символов = 100%
                    bar = "█" * filled + "░" * (20 - filled)
                    print(
                        f"\r🔍 Обработка услуг: [{bar}] {percentage:.0f}% ({i}/{total_elements}) | Найдено: {len(services)}",
                        end="",
                        flush=True,
                    )

                service_data = {}

                # БЫСТРАЯ ПРОВЕРКА: есть ли название услуги вообще
                name_element = self.navigator.find_element_with_fallback(
                    selectors.SERVICES["service_name"], element
                )
                if not name_element:
                    continue  # Пропускаем элементы без названия

                service_data["name"] = name_element.text.strip()
                if not service_data["name"]:
                    continue  # Пропускаем пустые названия

                # Цена
                price_element = self.navigator.find_element_with_fallback(
                    selectors.SERVICES["service_price"], element
                )
                if price_element:
                    price_text = price_element.text.strip()
                    service_data.update(self._parse_price(price_text))

                # Описание (опционально)
                try:
                    desc_element = self.navigator.find_element_with_fallback(
                        selectors.SERVICES["service_description"], element
                    )
                    if desc_element:
                        desc_text = desc_element.text.strip()
                        if desc_text:
                            service_data["description"] = desc_text
                except:
                    pass

                services.append(service_data)
                self.logger.debug(
                    f"Услуга #{len(services)}: {service_data.get('name')} - {service_data.get('price', 'Цена не указана')}"
                )

            except Exception as e:
                self.logger.debug(f"Пропускаем элемент #{i}: {e}")
                continue

        # Завершаем progress bar
        print()  # Новая строка после progress bar
        self.logger.info(f"Извлечено {len(services)} услуг")
        return services

    def navigate_to_reviews_tab(self) -> bool:
        """
        Переход на вкладку отзывов

        Returns:
            bool: True если переход успешен
        """
        return self.navigator.navigate_to_reviews_tab()

    def extract_reviews(self, max_reviews: int = 50) -> List[Dict[str, Any]]:
        """
        Извлечение отзывов

        Args:
            max_reviews: Максимальное количество отзывов для извлечения

        Returns:
            List: Список отзывов
        """
        self.logger.info(f"Извлечение отзывов (макс. {max_reviews})")

        reviews = []

        # Загружаем дополнительные отзывы
        self.navigator.load_more_content("reviews")

        # Находим все отзывы
        review_elements = self.navigator.find_element_with_fallback(
            selectors.REVIEWS["review_items"]
        )

        if not review_elements:
            self.logger.warning("Отзывы не найдены")
            return reviews

        if not isinstance(review_elements, list):
            review_elements = [review_elements]

        # Ограничиваем количество отзывов
        review_elements = review_elements[:max_reviews]

        for element in review_elements:
            try:
                review_data = {}

                # Автор
                author_element = self.navigator.find_element_with_fallback(
                    selectors.REVIEWS["review_author"], element
                )
                if author_element:
                    review_data["author"] = author_element.text.strip()

                # Рейтинг
                rating_element = self.navigator.find_element_with_fallback(
                    selectors.REVIEWS["review_rating"], element
                )
                if rating_element:
                    aria_label = rating_element.get_attribute("aria-label")
                    if aria_label:
                        # Извлекаем число из "Оценка X Из 5"
                        rating_match = re.search(
                            r"(?:Оценка|Rating) (\d+) (?:Из|Out of)",
                            aria_label,
                            re.IGNORECASE,
                        )
                        if rating_match:
                            review_data["rating"] = int(rating_match.group(1))

                # Дата
                date_element = self.navigator.find_element_with_fallback(
                    selectors.REVIEWS["review_date"], element
                )
                if date_element:
                    review_data["date"] = date_element.text.strip()

                # Текст отзыва
                text_element = self.navigator.find_element_with_fallback(
                    selectors.REVIEWS["review_text"], element
                )
                if text_element:
                    review_data["text"] = text_element.text.strip()

                # Ответ владельца - новая логика
                response_text = self._extract_owner_response(element)
                if response_text:
                    review_data["response"] = response_text

                if review_data.get("author"):
                    reviews.append(review_data)

            except Exception as e:
                self.logger.warning(f"Ошибка обработки отзыва: {e}")
                continue

        self.logger.info(f"Извлечено {len(reviews)} отзывов")
        return reviews

    def _extract_owner_response(self, review_element) -> Optional[str]:
        """
        Извлечение ответа владельца на отзыв

        Args:
            review_element: Элемент отзыва

        Returns:
            Optional[str]: Текст ответа владельца или None
        """
        try:
            # Ищем кнопку "Посмотреть ответ организации"
            response_button = self.navigator.find_element_with_fallback(
                selectors.REVIEWS["review_response_button"], review_element
            )

            if not response_button:
                return None

            # Проверяем, что это действительно кнопка для показа ответа
            button_text = response_button.text.strip().lower()
            if (
                "посмотреть ответ" not in button_text
                and "показать ответ" not in button_text
            ):
                return None

            # Кликаем по кнопке
            if self.navigator.safe_click(response_button):
                # Небольшая пауза для загрузки ответа
                time.sleep(1)

                # Ищем контент ответа в том же review_element или рядом с ним
                # Сначала ищем в самом элементе отзыва
                response_content = self.navigator.find_element_with_fallback(
                    selectors.REVIEWS["review_response_content"], review_element
                )

                # Если не найден в элементе отзыва, ищем в родительском контейнере
                if not response_content:
                    parent_element = review_element.find_element_by_xpath("./..")
                    response_content = self.navigator.find_element_with_fallback(
                        selectors.REVIEWS["review_response_content"], parent_element
                    )

                if response_content:
                    response_text = response_content.text.strip()
                    self.logger.debug(
                        f"Найден ответ организации: {response_text[:50]}..."
                    )
                    return response_text

            return None

        except Exception as e:
            self.logger.debug(f"Ошибка извлечения ответа организации: {e}")
            return None

    def _parse_price(self, price_text: str) -> Dict[str, str]:
        """
        Парсинг цены из текста

        Args:
            price_text: Текст с ценой

        Returns:
            Dict: Распарсенная цена
        """
        result = {}

        if not price_text:
            return result

        # Убираем лишние символы
        clean_text = re.sub(r"[^\d\s\-–—от до₽]", "", price_text)

        # Ищем диапазон цен (например, "2000-3000" или "от 2000 до 3000")
        range_match = re.search(r"(\d+)\s*[-–—]\s*(\d+)", clean_text)
        if range_match:
            result["price_from"] = range_match.group(1)
            result["price_to"] = range_match.group(2)
            result["price"] = f"{result['price_from']}-{result['price_to']}"
            return result

        # Ищем "от X" или "до X"
        from_match = re.search(r"от\s*(\d+)", clean_text)
        to_match = re.search(r"до\s*(\d+)", clean_text)

        if from_match:
            result["price_from"] = from_match.group(1)
            result["price"] = f"от {result['price_from']}"
        elif to_match:
            result["price_to"] = to_match.group(1)
            result["price"] = f"до {result['price_to']}"
        else:
            # Ищем простое число
            simple_match = re.search(r"(\d+)", clean_text)
            if simple_match:
                result["price"] = simple_match.group(1)

        return result

    def scrape_business(
        self, url: str, max_reviews: int = 50
    ) -> Optional[BusinessData]:
        """
        Полное извлечение данных о предприятии

        Args:
            url: URL страницы предприятия

        Returns:
            BusinessData: Извлеченные данные или None
        """
        if not self.driver:
            raise RuntimeError("WebDriver не инициализирован")

        self.logger.info(f"Начинаем скрапинг: {url}")

        try:
            # Загружаем страницу
            if not self.load_page(url):
                return None

            # Пауза после загрузки
            self.navigator.random_delay()

            # Извлекаем базовую информацию
            basic_data = self.extract_basic_info()

            if not basic_data.get("name"):
                self.logger.error("Не удалось извлечь название предприятия")
                return None

            # Извлекаем социальные сети
            social_data = self.extract_social_networks()

            # Данные услуг
            services_data = []
            if self.navigate_to_services_tab():
                self.navigator.random_delay()
                services_data = self.extract_services()

            # Данные отзывов
            reviews_data = []
            if self.navigate_to_reviews_tab():
                self.navigator.random_delay()
                reviews_data = self.extract_reviews(max_reviews=max_reviews)

            # Создаем объект данных
            business_data = BusinessData(
                name=basic_data.get("name", ""),
                category=basic_data.get("category"),
                services=[ServiceData(**service) for service in services_data],
                website=basic_data.get("website"),
                address=basic_data.get("address"),
                social_networks=SocialNetworks(**social_data),
                phone=basic_data.get("phone"),
                working_hours=WorkingHours(**basic_data.get("working_hours", {})),
                rating=basic_data.get("rating"),
                reviews_count=basic_data.get("reviews_count"),
                reviews=[ReviewData(**review) for review in reviews_data],
                metadata={
                    "source_url": url,
                    "scraper_version": "1.0",
                    "processing_time_sec": (
                        round(time.time() - self.start_time, 2)
                        if self.start_time
                        else 0
                    ),
                },
            )

            # Логируем успешную статистику
            processing_time = time.time() - self.start_time if self.start_time else 0
            data_extracted = (
                len(services_data)
                + len(reviews_data)
                + len([k for k, v in basic_data.items() if v])
            )

            log_scraping_stats(url, True, processing_time, data_extracted)
            scraping_metrics.record_request(True, processing_time)

            self.logger.info(f"Скрапинг завершен успешно за {processing_time:.2f}s")
            return business_data

        except Exception as e:
            processing_time = time.time() - self.start_time if self.start_time else 0
            error_msg = str(e)

            log_scraping_stats(url, False, processing_time, 0, error_msg)
            scraping_metrics.record_request(False, processing_time, error_msg)

            self.logger.error(f"Ошибка скрапинга: {e}")
            return None
