"""
CSS и XPath селекторы для извлечения данных с Яндекс.Карт
"""

from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class SelectorConfig:
    """Конфигурация селектора"""

    css: str = ""
    xpath: str = ""
    multiple: bool = False  # Если True, ищем множественные элементы
    attribute: str = ""  # Атрибут для извлечения (href, text и т.д.)
    fallback_selectors: List[str] = field(
        default_factory=list
    )  # Альтернативные селекторы


class YandexMapsSelectors:
    """Селекторы для извлечения данных с Яндекс.Карт"""

    # Базовая информация о предприятии
    BASIC_INFO = {
        "name": SelectorConfig(
            css=".card-title-view__title-link",  # было: h1.card-title-view__title
            xpath="//a[contains(@class, 'card-title-view__title-link')]",
            fallback_selectors=[
                ".orgpage-header-view__header",
                ".business-card-title-view__title",
                "[data-test-id='business-name']",
            ],
        ),
        "category": SelectorConfig(
            css=".business-categories-view__category",
            xpath="//div[contains(@class, 'business-categories-view__category')]",
            fallback_selectors=[
                ".business-card-title-view__categories",
                ".orgpage-header-view__category",
            ],
        ),
        "rating": SelectorConfig(
            css=".business-rating-badge-view__rating-text",
            xpath="//span[contains(@class, 'business-rating-badge-view__rating-text')]",
            fallback_selectors=[
                ".business-summary-rating-badge-view__rating",
                "[data-test-id='rating-badge']",
            ],
        ),
        "reviews_count": SelectorConfig(
            css=".business-header-rating-view__text",
            xpath="//span[contains(@class, 'business-header-rating-view__text')]",
            fallback_selectors=[
                ".business-summary-rating-badge-view__text",
                ".rating-badge-view__text",
            ],
        ),
        "address": SelectorConfig(
            css=".business-contacts-view__address-link",
            xpath="//div[contains(@class, 'business-contacts-view__address')]",
            fallback_selectors=[
                ".business-card-title-view__address",
                ".orgpage-header-view__address",
            ],
        ),
        "phone": SelectorConfig(
            css=".card-phones-view__phone-number",
            xpath="//span[contains(@class, 'card-phones-view__phone-number')]",
            multiple=True,
            fallback_selectors=[
                ".business-contacts-view__phone",
                "[data-test-id='phone-number']",
            ],
        ),
        "website": SelectorConfig(
            css="a[href*='http']:not([href*='yandex']):not([href*='maps'])",
            xpath="//a[contains(@href, 'http') and not(contains(@href, 'yandex')) and not(contains(@href, 'maps'))]",
            attribute="href",
            fallback_selectors=[
                ".business-urls-view__url",
                ".business-contacts-view__website",
            ],
        ),
        "working_hours": SelectorConfig(
            css=".business-working-status-view",
            xpath="//div[contains(@class, 'business-working-status-view')]",
            fallback_selectors=[
                ".business-summary-working-status-view",
                ".working-status-view",
            ],
        ),
    }

    # Навигационные элементы (вкладки)
    NAVIGATION = {
        "services_tab": SelectorConfig(
            xpath="//div[contains(@aria-label, 'Products and services') or contains(text(), 'Услуги') or contains(text(), 'Products')]",
            fallback_selectors=[
                "//button[contains(text(), 'Услуги')]",
                "//a[contains(@href, 'services')]",
                "//div[contains(@class, 'tabs') and contains(text(), 'Услуги')]",
            ],
        ),
        "reviews_tab": SelectorConfig(
            xpath="//a[contains(@href, 'reviews') or contains(text(), 'Отзывы') or contains(text(), 'Reviews')]",
            fallback_selectors=[
                "//button[contains(text(), 'Отзывы')]",
                "//div[contains(@class, 'tabs') and contains(text(), 'Отзывы')]",
            ],
        ),
        "overview_tab": SelectorConfig(
            xpath="//div[contains(@aria-label, 'Overview') or contains(text(), 'Обзор')]",
            fallback_selectors=[
                "//button[contains(text(), 'Обзор')]",
                "//a[contains(@href, 'overview')]",
            ],
        ),
    }

    # Услуги и цены
    SERVICES = {
        "service_items": SelectorConfig(
            css=".business-full-items-grouped-view__item",
            xpath="//div[contains(@class, 'business-menu-view__item')]",
            multiple=True,
            fallback_selectors=[
                ".menu-item-view",
                ".service-item",
                "[data-test-id='service-item']",
            ],
        ),
        "service_name": SelectorConfig(
            css=".related-item-list-view__title",
            xpath=".//div[contains(@class, 'business-menu-view__item-name')]",
            fallback_selectors=[".menu-item-view__name", ".service-name"],
        ),
        "service_price": SelectorConfig(
            css=".related-item-list-view__price",
            xpath=".//div[contains(@class, 'business-menu-view__item-price')]",
            fallback_selectors=[".menu-item-view__price", ".service-price", ".price"],
        ),
        "service_description": SelectorConfig(
            css=".business-menu-view__item-description",
            xpath=".//div[contains(@class, 'business-menu-view__item-description')]",
            fallback_selectors=[".menu-item-view__description", ".service-description"],
        ),
    }

    # Отзывы
    REVIEWS = {
        "review_items": SelectorConfig(
            css=".business-reviews-card-view__review",
            xpath="//div[contains(@class, 'business-reviews-card-view__review')]",
            multiple=True,
            fallback_selectors=[
                ".review-item",
                ".reviews-view__item",
                "[data-test-id='review-item']",
            ],
        ),
        "review_author": SelectorConfig(
            css=".business-review-view__author",
            xpath=".//span[contains(@class, 'business-review-view__author')]",
            fallback_selectors=[".review-author", ".author-name"],
        ),
        "review_rating": SelectorConfig(
            css=".business-review-view__rating",
            xpath=".//span[contains(@class, 'business-review-view__rating')]",
            fallback_selectors=[".review-rating", ".rating-stars"],
        ),
        "review_date": SelectorConfig(
            css=".business-review-view__date",
            xpath=".//span[contains(@class, 'business-review-view__date')]",
            fallback_selectors=[".review-date", ".date"],
        ),
        "review_text": SelectorConfig(
            css=".business-review-view__body",
            xpath=".//div[contains(@class, 'business-review-view__body')]",
            fallback_selectors=[".review-text", ".review-body"],
        ),
        "review_response": SelectorConfig(
            css=".business-review-view__response",
            xpath=".//div[contains(@class, 'business-review-view__response')]",
            fallback_selectors=[".review-response", ".owner-response"],
        ),
    }

    # Социальные сети
    SOCIAL_NETWORKS = {
        "telegram": SelectorConfig(
            xpath="//a[contains(@href, 't.me') or contains(@href, 'telegram')]",
            attribute="href",
        ),
        "whatsapp": SelectorConfig(
            xpath="//a[contains(@href, 'wa.me') or contains(@href, 'whatsapp')]",
            attribute="href",
        ),
        "vk": SelectorConfig(xpath="//a[contains(@href, 'vk.com')]", attribute="href"),
        "instagram": SelectorConfig(
            xpath="//a[contains(@href, 'instagram.com')]", attribute="href"
        ),
        "facebook": SelectorConfig(
            xpath="//a[contains(@href, 'facebook.com')]", attribute="href"
        ),
    }

    # Загрузка дополнительного контента
    LOADING = {
        "show_more_services": SelectorConfig(
            xpath="//button[contains(text(), 'Показать ещё') or contains(text(), 'Show more')]",
            fallback_selectors=[
                "//button[contains(@class, 'show-more')]",
                "//a[contains(text(), 'Все услуги')]",
            ],
        ),
        "show_more_reviews": SelectorConfig(
            xpath="//button[contains(text(), 'Ещё отзывы') or contains(text(), 'More reviews')]",
            fallback_selectors=[
                "//button[contains(@class, 'reviews-show-more')]",
                "//a[contains(text(), 'Все отзывы')]",
            ],
        ),
        "loading_spinner": SelectorConfig(
            css=".loading, .spinner",
            xpath="//div[contains(@class, 'loading') or contains(@class, 'spinner')]",
        ),
    }


class SelectorValidator:
    """Валидатор селекторов"""

    @staticmethod
    def validate_selector(selector: str, selector_type: str = "css") -> bool:
        """
        Валидация селектора

        Args:
            selector: Селектор для валидации
            selector_type: Тип селектора (css или xpath)

        Returns:
            bool: True если селектор валиден
        """
        if not selector or not isinstance(selector, str):
            return False

        if selector_type == "xpath":
            # Базовая валидация XPath
            return selector.startswith("//") or selector.startswith("./")

        # Базовая валидация CSS
        return len(selector.strip()) > 0 and not selector.startswith(" ")

    @staticmethod
    def get_fallback_selectors(config: SelectorConfig) -> List[str]:
        """
        Получить список всех доступных селекторов (основной + fallback)

        Args:
            config: Конфигурация селектора

        Returns:
            List[str]: Список селекторов
        """
        selectors = []

        if config.css:
            selectors.append(config.css)
        if config.xpath:
            selectors.append(config.xpath)
        if config.fallback_selectors:
            selectors.extend(config.fallback_selectors)

        return [s for s in selectors if SelectorValidator.validate_selector(s)]


# Экземпляр селекторов для использования в других модулях
selectors = YandexMapsSelectors()
