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
            css=".card-title-view__title-link",
            xpath="//a[contains(@class, 'card-title-view__title-link')]",
        ),
        "category": SelectorConfig(
            css=".business-categories-view__category _outline _clickable",
            xpath="//a[contains(@class, 'business-categories-view__category _outline _clickable')]",
        ),
        "rating": SelectorConfig(
            css=".business-rating-badge-view__rating-text",
            xpath="//span[contains(@class, 'business-rating-badge-view__rating-text')]",
        ),
        "reviews_count": SelectorConfig(
            css=".business-header-rating-view__text _clickable",
            xpath="//div[contains(@class, 'business-header-rating-view__text _clickable')]",
        ),
        "address": SelectorConfig(
            css=".business-contacts-view__address-link",
            xpath="//div[contains(@class, 'business-contacts-view__address-link')]",
        ),
        "phone": SelectorConfig(
            css=".card-phones-view__phone-number",
            xpath="//div[contains(@class, 'card-phones-view__phone-number')]",
        ),
        "website": SelectorConfig(
            css=".business-urls-view__link",
            xpath="//a[contains(@class, 'business-urls-view__link')]",
            attribute="href",
        ),
        "working_hours": SelectorConfig(
            css=".business-working-status-view",
            xpath="//div[contains(@class, 'business-working-status-view')]",
        ),
    }

    # Навигационные элементы (вкладки)
    NAVIGATION = {
        "services_tab": SelectorConfig(
            css=".tabs-select-view__title._name_prices",
            xpath="//div[contains(@class, 'tabs-select-view__title') and contains(@class, '_name_prices')]",
        ),
        "reviews_tab": SelectorConfig(
            css=".tabs-select-view__title._name_reviews", 
            xpath="//div[contains(@class, 'tabs-select-view__title') and contains(@class, '_name_reviews')]",
        ),
        "overview_tab": SelectorConfig(
            css=".tabs-select-view__title._name_overview",
            xpath="//div[contains(@class, 'tabs-select-view__title') and contains(@class, '_name_overview')]",
        ),
    }

    # Услуги и цены
    SERVICES = {
        "service_items": SelectorConfig(
            css=".business-full-items-grouped-view__content",
            xpath="//div[contains(@class, 'business-full-items-grouped-view__content')]",
            multiple=True,
        ),

        "service_name": SelectorConfig(
            css=".related-item-photo-view__title",
            xpath=".//div[contains(@class, 'related-item-photo-view__title')]",
        ),

        "service_price": SelectorConfig(
            css=".related-product-view__price",
            xpath=".//span[contains(@class, 'related-product-view__price')]",
        ),

        "service_description": SelectorConfig(
            css=".related-item-photo-view__description",
            xpath=".//div[contains(@class, 'related-item-photo-view__description')]",
        ),
    }

    # Отзывы
    REVIEWS = {
        "review_items": SelectorConfig(
            css=".business-review-view__info",
            xpath="//div[contains(@class, 'business-review-view__info')]",
            multiple=True,
        ),
        "review_author": SelectorConfig(
            css="span[itemprop='name']",
            xpath=".//span[@itemprop='name']",
        ),
        "review_rating": SelectorConfig(
            css=".business-review-view__rating .business-rating-badge-view__stars",
            xpath=".//div[contains(@class, 'business-review-view__rating')]//div[contains(@class, 'business-rating-badge-view__stars')]",
            attribute="aria-label",
        ),
        "review_date": SelectorConfig(
            css=".business-review-view__date span",
            xpath=".//div[contains(@class, 'business-review-view__date')]//span",
        ),
        "review_text": SelectorConfig(
            css=".business-review-view__body .spoiler-view__text-container",
            xpath=".//div[contains(@class, 'business-review-view__body')]//span[contains(@class, 'spoiler-view__text-container')]",
            fallback_selectors=[".business-review-view__body"]
        ),
        "review_response_button": SelectorConfig(
            css=".business-review-view__comment-expand",
            xpath=".//div[contains(@class, 'business-review-view__comment-expand')]",
        ),
        "review_response_content": SelectorConfig(
            css=".business-review-comment-content__bubble",
            xpath=".//div[contains(@class, 'business-review-comment-content__bubble')]",
        ),
    }

    # Социальные сети
    SOCIAL_NETWORKS = {
        "telegram": SelectorConfig(
            xpath="//a[contains(@href, 't.me') or contains(@href, 'telegram') or contains(@aria-label, 'telegram')]",
            attribute="href",
        ),
        "whatsapp": SelectorConfig(
            xpath="//a[contains(@href, 'wa.me') or contains(@href, 'whatsapp') or contains(@aria-label, 'whatsapp')]",
            attribute="href",
        ),
        "vk": SelectorConfig(
            xpath="//a[contains(@href, 'vk.com') or contains(@aria-label, 'vkontakte')]",
            attribute="href",
        ),
    }

    # Загрузка дополнительного контента
    LOADING = {
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
