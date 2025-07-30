"""
Основной класс для скрапинга данных с Яндекс.Карт
"""

import json
import re
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urlparse

from bs4 import BeautifulSoup
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
    ratings_count: Optional[int] = None  # Добавляем - количество оценок (102)
    reviews_count: Optional[int] = None  # Оставляем - количество отзывов (89)
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

    def navigate_to_services_tab(self) -> bool:
        """
        Переход на вкладку услуг

        Returns:
            bool: True если переход успешен
        """
        return self.navigator.navigate_to_services_tab()

    def navigate_to_reviews_tab(self) -> bool:
        """
        Переход на вкладку отзывов

        Returns:
            bool: True если переход успешен
        """
        return self.navigator.navigate_to_reviews_tab()

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
            
            # Получаем фактическое количество отзывов
            actual_reviews_count = getattr(self, '_actual_reviews_count', None)

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
                ratings_count=basic_data.get("ratings_count"),  # 102
                reviews_count=actual_reviews_count,  # 89
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

    def extract_services(self) -> List[Dict[str, Any]]:
        """
        Быстрое извлечение услуг через BeautifulSoup после навигации Selenium

        Returns:
            List: Список услуг
        """
        self.logger.info("Быстрое извлечение услуг (гибридный подход)")

        services = []

        # 1. Используем Selenium для навигации
        if not self.navigate_to_services_tab():
            self.logger.warning("Не удалось перейти на вкладку услуг")
            return services

        # Небольшая пауза для полной загрузки контента
        time.sleep(3)

        # 2. Получаем HTML страницы
        self.logger.info("Получение HTML страницы...")
        page_html = self.driver.page_source

        soup = BeautifulSoup(page_html, "html.parser")

        # 4. Находим все элементы услуг
        service_elements = soup.find_all(
            "div", class_="business-full-items-grouped-view__item"
        )

        if not service_elements:
            self.logger.warning("Услуги не найдены в HTML")
            return services

        total_elements = len(service_elements)
        self.logger.info(f"Найдено {total_elements} элементов услуг в HTML")

        # 5. Progress bar для обработки
        progress_step = max(1, total_elements // 20)  # Обновляем каждые 5%

        for i, element in enumerate(service_elements, 1):
            try:
                # Progress bar
                if i % progress_step == 0 or i == total_elements:
                    percentage = (i / total_elements) * 100
                    filled = int(percentage // 5)
                    bar = "█" * filled + "░" * (20 - filled)
                    print(
                        f"\r⚡ Быстрая обработка: [{bar}] {percentage:.0f}% ({i}/{total_elements}) | Найдено: {len(services)}",
                        end="",
                        flush=True,
                    )

                service_data = {}

                # Ищем название (grid тип)
                name_elem = element.find("div", class_="related-item-photo-view__title")
                # Fallback для list типа
                if not name_elem:
                    name_elem = element.find(
                        "div", class_="related-item-list-view__title"
                    )

                if not name_elem:
                    continue

                service_name = name_elem.get_text(strip=True)
                if not service_name:
                    continue

                service_data["name"] = service_name

                # Ищем цену (grid тип)
                price_elem = element.find("span", class_="related-product-view__price")
                # Fallback для list типа
                if not price_elem:
                    price_elem = element.find(
                        "div", class_="related-item-list-view__price"
                    )

                if price_elem:
                    price_text = price_elem.get_text(strip=True)
                    service_data.update(self._parse_price(price_text))

                # Ищем описание (опционально)
                desc_elem = element.find(
                    "div", class_="related-item-photo-view__description"
                )
                # Fallback для list типа
                if not desc_elem:
                    desc_elem = element.find(
                        "div", class_="related-item-list-view__subtitle"
                    )

                if desc_elem:
                    desc_text = desc_elem.get_text(strip=True)
                    if desc_text:
                        service_data["description"] = desc_text

                services.append(service_data)

            except Exception as e:
                self.logger.debug(f"Ошибка обработки услуги #{i}: {e}")
                continue

        # Завершаем progress bar
        print()
        self.logger.info(
            f"⚡ Быстро извлечено {len(services)} услуг за {time.time() - time.time():.1f}s"
        )
        return services

    def extract_basic_info(self) -> Dict[str, Any]:
        """
        Быстрое извлечение базовой информации через BeautifulSoup

        Returns:
            Dict: Словарь с базовой информацией
        """
        self.logger.info("Быстрое извлечение базовой информации")

        # Получаем HTML страницы
        page_html = self.driver.page_source

        # Парсим через BeautifulSoup
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(page_html, "html.parser")

        data = {}

        # Название
        name_elem = soup.find("a", class_="card-title-view__title-link")
        if name_elem:
            data["name"] = name_elem.get_text(strip=True)
            self.logger.debug(f"Название: {data['name']}")

        # Категория
        category_elem = soup.find("a", class_="business-categories-view__category")
        if category_elem:
            data["category"] = category_elem.get_text(strip=True)
            self.logger.debug(f"Категория: {data['category']}")

        # Рейтинг
        rating_elem = soup.find(
            "span", class_="business-rating-badge-view__rating-text"
        )
        if rating_elem:
            rating_text = rating_elem.get_text(strip=True)
            rating_match = re.search(r"(\d+[\.,]\d+)", rating_text)
            if rating_match:
                data["rating"] = float(rating_match.group(1).replace(",", "."))
                self.logger.debug(f"Рейтинг: {data['rating']}")

        # Количество оценок (было reviews_count)
        reviews_elem = soup.find("div", class_="business-header-rating-view__text")
        if reviews_elem:
            reviews_text = reviews_elem.get_text(strip=True)
            reviews_match = re.search(r"(\d+)", reviews_text)
            if reviews_match:
                data["ratings_count"] = int(reviews_match.group(1))  # Изменено
                self.logger.debug(f"Количество оценок: {data['ratings_count']}")

        # Адрес
        address_elem = soup.find("div", class_="business-contacts-view__address-link")
        if address_elem:
            data["address"] = address_elem.get_text(strip=True)
            self.logger.debug(f"Адрес: {data['address']}")

        # Телефон
        phone_elem = soup.find("div", class_="card-phones-view__phone-number")
        if phone_elem:
            data["phone"] = phone_elem.get_text(strip=True)
            self.logger.debug(f"Телефон: {data['phone']}")

        # Сайт
        website_elem = soup.find("a", class_="business-urls-view__link")
        if website_elem:
            href = website_elem.get("href")
            if href:
                data["website"] = href
                self.logger.debug(f"Сайт: {data['website']}")

        # Часы работы
        working_elem = soup.find("div", class_="business-working-status-view")
        if working_elem:
            data["working_hours"] = {
                "current_status": working_elem.get_text(strip=True)
            }
            self.logger.debug(
                f"Статус работы: {data['working_hours']['current_status']}"
            )

        return data

    def extract_reviews(self, max_reviews: int = -1) -> List[Dict[str, Any]]:
        """
        Быстрое извлечение отзывов через BeautifulSoup + Selenium для ответов
        """
        self.logger.info(f"Быстрое извлечение отзывов{'(без ограничений)' if max_reviews == -1 else f'(макс. {max_reviews})'}")
        
        reviews = []
        
        # 1. Переходим на вкладку отзывов через Selenium
        if not self.navigate_to_reviews_tab():
            self.logger.warning("Не удалось перейти на вкладку отзывов")
            return reviews
        
        # Небольшая пауза для загрузки
        time.sleep(3)
        
        # 2. Получаем HTML страницы
        self.logger.info("Получение HTML страницы с отзывами...")
        page_html = self.driver.page_source
        
        # 3. Парсим через BeautifulSoup
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(page_html, 'html.parser')
        
        # 4. Извлекаем количество отзывов из заголовка
        reviews_header = soup.find('h2', class_='card-section-header__title')
        if reviews_header:
            header_text = reviews_header.get_text(strip=True)
            reviews_count_match = re.search(r'(\d+)\s*отзыв', header_text)
            if reviews_count_match:
                actual_reviews_count = int(reviews_count_match.group(1))
                self.logger.info(f"Количество отзывов на странице: {actual_reviews_count}")
                self._actual_reviews_count = actual_reviews_count
        
        # 5. Находим все отзывы - БОЛЕЕ ТОЧНЫЙ СЕЛЕКТОР
        review_elements = soup.find_all('div', class_='business-review-view__info')
        
        if not review_elements:
            self.logger.warning("Отзывы не найдены в HTML")
            return reviews
        
        # Ограничиваем количество отзывов только если задано ограничение
        if max_reviews > 0:
            review_elements = review_elements[:max_reviews]
        
        total_reviews = len(review_elements)
        
        self.logger.info(f"Найдено {total_reviews} отзывов в HTML")
        
        # 5. Progress bar для отзывов
        progress_step = max(1, total_reviews // 20)
        
        for i, element in enumerate(review_elements, 1):
            try:
                # Progress bar
                if i % progress_step == 0 or i == total_reviews:
                    percentage = (i / total_reviews) * 100
                    filled = int(percentage // 5)
                    bar = "█" * filled + "░" * (20 - filled)
                    print(f"\r💬 Быстрая обработка отзывов: [{bar}] {percentage:.0f}% ({i}/{total_reviews}) | Найдено: {len(reviews)}", end="", flush=True)
                
                review_data = {}
                
                # Автор
                author_elem = element.find('span', attrs={'itemprop': 'name'})
                if author_elem:
                    review_data["author"] = author_elem.get_text(strip=True)
                
                # Рейтинг из aria-label
                rating_elem = element.find('div', class_='business-rating-badge-view__stars')
                if rating_elem:
                    aria_label = rating_elem.get('aria-label', '')
                    rating_match = re.search(r"(?:Оценка|Rating) (\d+) (?:Из|Out of)", aria_label, re.IGNORECASE)
                    if rating_match:
                        review_data["rating"] = int(rating_match.group(1))
                
                # Дата - ИСПРАВЛЕННАЯ ВЕРСИЯ
                date_elem = element.find('span', class_='business-review-view__date')
                if date_elem:
                    # Ищем span с текстом даты
                    date_span = date_elem.find('span')
                    if date_span:
                        review_data["date"] = date_span.get_text(strip=True)
                    # Fallback - ищем meta с datePublished
                    elif date_elem.find('meta', attrs={'itemprop': 'datePublished'}):
                        content = date_elem.find('meta')['content']
                        try:
                            from datetime import datetime
                            dt = datetime.fromisoformat(content.replace('Z', '+00:00'))
                            review_data["date"] = dt.strftime('%d %B')
                        except:
                            review_data["date"] = content
                
                # Текст отзыва
                text_elem = element.find('span', class_='spoiler-view__text-container') or \
                        element.find('div', class_='business-review-view__body')
                if text_elem:
                    review_data["text"] = text_elem.get_text(strip=True)
                
                # Ответ владельца - ГИБРИДНЫЙ ПОДХОД
                # Сначала пробуем найти в HTML, если не найден - используем Selenium
                response_elem = element.find('div', class_='business-review-comment-content__bubble')
                if response_elem:
                    review_data["response"] = response_elem.get_text(strip=True)
                else:
                    # Если ответ не виден, пробуем найти кнопку и кликнуть через Selenium
                    response_button = element.find('div', class_='business-review-view__comment-expand')
                    if response_button:
                        # Используем Selenium для клика (медленнее, но надежнее)
                        try:
                            # Находим соответствующий элемент через Selenium
                            selenium_elements = self.navigator.find_element_with_fallback(
                                selectors.REVIEWS["review_items"]
                            )
                            if selenium_elements and isinstance(selenium_elements, list) and i <= len(selenium_elements):
                                selenium_element = selenium_elements[i-1]
                                response_text = self._extract_owner_response(selenium_element)
                                if response_text:
                                    review_data["response"] = response_text
                        except:
                            pass  # Если не получилось через Selenium, оставляем пустым
                
                # В конце цикла for перед добавлением отзыва:
                if review_data.get("author") and len(review_data.get("author", "")) > 1:
                    # Дополнительная проверка - отзыв должен иметь хотя бы автора и текст или рейтинг
                    if review_data.get("text") or review_data.get("rating"):
                        reviews.append(review_data)
                    
            except Exception as e:
                self.logger.debug(f"Ошибка обработки отзыва #{i}: {e}")
                continue
        
        # Завершаем progress bar
        print()
        self.logger.info(f"⚡ Быстро извлечено {len(reviews)} отзывов")
        return reviews

        # Завершаем progress bar
        print()
        self.logger.info(f"⚡ Быстро извлечено {len(reviews)} отзывов")
        return reviews

    def extract_social_networks(self) -> Dict[str, str]:
        """
        Быстрое извлечение ссылок на социальные сети через BeautifulSoup

        Returns:
            Dict: Словарь с социальными сетями
        """
        self.logger.debug("Быстрое извлечение социальных сетей")

        # Получаем HTML если еще не получили
        page_html = self.driver.page_source

        from bs4 import BeautifulSoup

        soup = BeautifulSoup(page_html, "html.parser")

        social_data = {}

        # Ищем все ссылки на странице
        all_links = soup.find_all("a", href=True)

        for link in all_links:
            href = link.get("href", "")

            # Telegram
            if ("t.me" in href or "telegram" in href) and "telegram" not in social_data:
                social_data["telegram"] = href
                self.logger.debug(f"telegram: {href}")

            # WhatsApp
            elif (
                "wa.me" in href or "whatsapp" in href
            ) and "whatsapp" not in social_data:
                social_data["whatsapp"] = href
                self.logger.debug(f"whatsapp: {href}")

            # VK
            elif "vk.com" in href and "vk" not in social_data:
                social_data["vk"] = href
                self.logger.debug(f"vk: {href}")

        return social_data
