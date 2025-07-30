#!/usr/bin/env python3
"""
Главный класс системы извлечения данных с Яндекс.Карт
Объединяет все компоненты в единый высокоуровневый интерфейс
"""

import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union
import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed

from config.settings import settings
from core.logger import get_logger, scraping_metrics
from scrapper import YandexMapsScraper, BusinessData
from exporters import UnifiedExporter
from models import Enterprise


class ExtractionResult:
    """Результат извлечения данных"""
    
    def __init__(self):
        self.successful_extractions: List[BusinessData] = []
        self.failed_extractions: List[Dict] = []
        self.total_urls: int = 0
        self.processing_time: float = 0.0
        self.export_paths: Dict[str, str] = {}
        self.errors: List[str] = []
        
    @property
    def success_rate(self) -> float:
        """Процент успешных извлечений"""
        return (len(self.successful_extractions) / self.total_urls * 100) if self.total_urls > 0 else 0.0
    
    def add_successful(self, data: BusinessData, url: str):
        """Добавить успешное извлечение"""
        self.successful_extractions.append(data)
        
    def add_failed(self, url: str, error: str):
        """Добавить неудачное извлечение"""
        self.failed_extractions.append({
            'url': url,
            'error': error,
            'timestamp': datetime.now().isoformat()
        })
        
    def get_summary(self) -> Dict:
        """Получить сводку результатов"""
        return {
            'total_urls': self.total_urls,
            'successful': len(self.successful_extractions),
            'failed': len(self.failed_extractions),
            'success_rate': round(self.success_rate, 2),
            'processing_time': round(self.processing_time, 2),
            'avg_time_per_url': round(self.processing_time / self.total_urls, 2) if self.total_urls > 0 else 0,
            'export_formats': list(self.export_paths.keys()),
            'timestamp': datetime.now().isoformat()
        }


class EnterpriseDataExtractor:
    """
    Главный класс системы извлечения данных о предприятиях с Яндекс.Карт
    
    Объединяет все компоненты системы и предоставляет высокоуровневый API
    для извлечения, обработки и экспорта данных о предприятиях.
    """
    
    def __init__(self, config: Optional[Dict] = None):
        """
        Инициализация экстрактора
        
        Args:
            config: Дополнительная конфигурация
        """
        self.config = config or {}
        self.logger = get_logger(__name__)
        self.exporter = UnifiedExporter()
        
        # Статистика сессии
        self.session_stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'total_processing_time': 0.0,
            'session_start': datetime.now()
        }
        
        self.logger.info("EnterpriseDataExtractor инициализирован")
    
    def extract_single(
        self, 
        url: str,
        export_formats: List[str] = None,
        output_dir: str = None,
        include_services: bool = True,
        include_reviews: bool = True,
        max_reviews: int = 50
    ) -> Dict:
        """
        Извлечение данных одного предприятия
        
        Args:
            url: URL страницы предприятия на Яндекс.Картах
            export_formats: Форматы экспорта ['json', 'csv', 'database']
            output_dir: Директория для сохранения файлов
            include_services: Извлекать услуги
            include_reviews: Извлекать отзывы
            max_reviews: Максимальное количество отзывов
            
        Returns:
            Dict: Результат извлечения с путями к файлам
        """
        start_time = time.time()
        self.session_stats['total_requests'] += 1
        
        if export_formats is None:
            export_formats = ['json']
            
        if output_dir:
            self.exporter = UnifiedExporter(output_dir)
        
        self.logger.info(f"Начинаем извлечение данных: {url}")
        
        try:
            # Валидация URL
            if not self._validate_url(url):
                raise ValueError(f"Некорректный URL: {url}")
            
            # Извлечение данных
            with YandexMapsScraper(self.config) as scraper:
                business_data = scraper.scrape_business(url, max_reviews)
                
                if not business_data:
                    raise Exception("Не удалось извлечь данные предприятия")
            
            # Экспорт в указанные форматы
            export_results = {}
            for format_type in export_formats:
                try:
                    file_path = self.exporter.export_by_format(business_data, format_type)
                    export_results[format_type] = file_path
                    self.logger.info(f"Данные экспортированы в {format_type}: {file_path}")
                except Exception as e:
                    export_results[f"{format_type}_error"] = str(e)
                    self.logger.error(f"Ошибка экспорта в {format_type}: {e}")
            
            # Обновляем статистику
            self.session_stats['successful_requests'] += 1
            processing_time = time.time() - start_time
            self.session_stats['total_processing_time'] += processing_time
            
            result = {
                'success': True,
                'data': business_data.model_dump() if business_data else None,
                'export_paths': export_results,
                'processing_time': round(processing_time, 2),
                'url': url,
                'timestamp': datetime.now().isoformat(),
                'data_quality': self._assess_data_quality(business_data) if business_data else {}
            }
            
            self.logger.info(f"Извлечение завершено успешно за {processing_time:.2f}s")
            return result
            
        except Exception as e:
            self.session_stats['failed_requests'] += 1
            processing_time = time.time() - start_time
            
            error_result = {
                'success': False,
                'error': str(e),
                'url': url,
                'processing_time': round(processing_time, 2),
                'timestamp': datetime.now().isoformat()
            }
            
            self.logger.error(f"Ошибка извлечения данных из {url}: {e}")
            return error_result
    
    def extract_batch(
        self,
        urls: List[str],
        export_formats: List[str] = None,
        output_dir: str = None,
        delay_between: float = None,
        max_workers: int = 1,
        continue_on_error: bool = True
    ) -> ExtractionResult:
        """
        Массовое извлечение данных из списка URL
        
        Args:
            urls: Список URL для обработки
            export_formats: Форматы экспорта
            output_dir: Директория для сохранения
            delay_between: Задержка между запросами (секунды)
            max_workers: Количество параллельных потоков (для будущего использования)
            continue_on_error: Продолжать при ошибках
            
        Returns:
            ExtractionResult: Результаты обработки
        """
        start_time = time.time()
        result = ExtractionResult()
        result.total_urls = len(urls)
        
        if export_formats is None:
            export_formats = ['json']
            
        if delay_between is None:
            delay_between = (settings.MIN_DELAY + settings.MAX_DELAY) / 2
        
        self.logger.info(f"Начинаем массовое извлечение: {len(urls)} URL")
        
        for i, url in enumerate(urls, 1):
            self.logger.info(f"Обрабатываем URL {i}/{len(urls)}: {url}")
            
            try:
                # Извлекаем данные одного предприятия
                extraction_result = self.extract_single(
                    url=url,
                    export_formats=export_formats,
                    output_dir=output_dir
                )
                
                if extraction_result['success']:
                    # Создаем объект BusinessData из результата
                    business_data = BusinessData(**extraction_result['data'])
                    result.add_successful(business_data, url)
                    
                    # Сохраняем пути экспорта
                    for format_type, path in extraction_result.get('export_paths', {}).items():
                        if format_type not in result.export_paths:
                            result.export_paths[format_type] = []
                        if isinstance(result.export_paths[format_type], list):
                            result.export_paths[format_type].append(path)
                        else:
                            result.export_paths[format_type] = [result.export_paths[format_type], path]
                else:
                    result.add_failed(url, extraction_result.get('error', 'Unknown error'))
                    
            except Exception as e:
                result.add_failed(url, str(e))
                if not continue_on_error:
                    self.logger.error(f"Прерывание обработки из-за ошибки: {e}")
                    break
            
            # Задержка между запросами (кроме последнего)
            if i < len(urls):
                self.logger.debug(f"Пауза {delay_between}s перед следующим запросом")
                time.sleep(delay_between)
        
        result.processing_time = time.time() - start_time
        
        # Создаем сводный экспорт для успешных результатов
        if result.successful_extractions and 'json' in export_formats:
            try:
                summary_path = self.exporter.json_exporter.export_multiple(
                    result.successful_extractions,
                    format_type='object_collection'
                )
                result.export_paths['batch_summary'] = summary_path
            except Exception as e:
                self.logger.error(f"Ошибка создания сводного файла: {e}")
        
        # Логируем итоговую статистику
        summary = result.get_summary()
        self.logger.info(f"Массовое извлечение завершено: {summary}")
        scraping_metrics.log_summary()
        
        return result
    
    def extract_from_file(
        self,
        file_path: Union[str, Path],
        **kwargs
    ) -> ExtractionResult:
        """
        Извлечение данных из файла со списком URL
        
        Args:
            file_path: Путь к файлу с URL (по одному на строку)
            **kwargs: Параметры для extract_batch
            
        Returns:
            ExtractionResult: Результаты обработки
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"Файл не найден: {file_path}")
        
        # Читаем URL из файла
        with open(file_path, 'r', encoding='utf-8') as f:
            urls = [line.strip() for line in f if line.strip() and not line.startswith('#')]
        
        if not urls:
            raise ValueError(f"В файле {file_path} не найдено валидных URL")
        
        self.logger.info(f"Загружено {len(urls)} URL из файла {file_path}")
        
        return self.extract_batch(urls, **kwargs)
    
    def validate_urls(self, urls: List[str]) -> Dict[str, List[str]]:
        """
        Валидация списка URL
        
        Args:
            urls: Список URL для проверки
            
        Returns:
            Dict: Словарь с валидными и невалидными URL
        """
        valid_urls = []
        invalid_urls = []
        
        for url in urls:
            if self._validate_url(url):
                valid_urls.append(url)
            else:
                invalid_urls.append(url)
        
        return {
            'valid': valid_urls,
            'invalid': invalid_urls,
            'valid_count': len(valid_urls),
            'invalid_count': len(invalid_urls),
            'total_count': len(urls)
        }
    
    def get_session_statistics(self) -> Dict:
        """Получить статистику текущей сессии"""
        current_time = datetime.now()
        session_duration = (current_time - self.session_stats['session_start']).total_seconds()
        
        stats = {
            **self.session_stats,
            'session_duration_seconds': round(session_duration, 2),
            'success_rate': round(
                (self.session_stats['successful_requests'] / max(self.session_stats['total_requests'], 1)) * 100, 2
            ),
            'avg_processing_time': round(
                self.session_stats['total_processing_time'] / max(self.session_stats['successful_requests'], 1), 2
            ),
            'requests_per_minute': round(
                self.session_stats['total_requests'] / max(session_duration / 60, 1), 2
            )
        }
        
        return stats
    
    def _validate_url(self, url: str) -> bool:
        """Валидация URL Яндекс.Карт"""
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            
            # Проверяем схему
            if parsed.scheme not in ['http', 'https']:
                return False
            
            # Проверяем домен
            domain_valid = any(
                domain in parsed.netloc 
                for domain in settings.YANDEX_MAPS_DOMAINS
            )
            
            # Проверяем путь
            path_valid = '/maps/' in parsed.path
            
            return domain_valid and path_valid
            
        except Exception:
            return False
    
    def _assess_data_quality(self, business_data: BusinessData) -> Dict:
        """Оценка качества извлеченных данных"""
        if not business_data:
            return {'score': 0.0, 'completeness': 0.0}
        
        # Подсчитываем заполненные поля
        total_fields = 10  # Основные поля согласно ТЗ
        filled_fields = 0
        
        if business_data.name:
            filled_fields += 1
        if business_data.category:
            filled_fields += 1
        if business_data.address:
            filled_fields += 1
        if business_data.phone:
            filled_fields += 1
        if business_data.website:
            filled_fields += 1
        if business_data.rating is not None:
            filled_fields += 1
        if business_data.reviews_count is not None:
            filled_fields += 1
        if business_data.services:
            filled_fields += 1
        if business_data.social_networks and (
            business_data.social_networks.telegram or 
            business_data.social_networks.whatsapp or 
            business_data.social_networks.vk
        ):
            filled_fields += 1
        if business_data.working_hours and (
            business_data.working_hours.current_status or 
            business_data.working_hours.schedule
        ):
            filled_fields += 1
        
        completeness = filled_fields / total_fields
        
        # Дополнительные метрики качества
        quality_metrics = {
            'completeness': round(completeness, 2),
            'has_contact_info': bool(business_data.phone or business_data.website),
            'has_services': len(business_data.services) > 0,
            'has_reviews': len(business_data.reviews) > 0,
            'has_rating': business_data.rating is not None,
            'has_social_networks': bool(
                business_data.social_networks and any([
                    business_data.social_networks.telegram,
                    business_data.social_networks.whatsapp,
                    business_data.social_networks.vk
                ])
            ),
            'services_count': len(business_data.services),
            'reviews_count': len(business_data.reviews),
            'data_richness_score': round(
                (len(business_data.services) * 0.2 + 
                 len(business_data.reviews) * 0.1 + 
                 (1 if business_data.rating else 0) * 0.3 +
                 completeness * 0.4), 2
            )
        }
        
        return quality_metrics


# Convenience функции для быстрого использования
def extract_single_enterprise(url: str, **kwargs) -> Dict:
    """Быстрое извлечение данных одного предприятия"""
    extractor = EnterpriseDataExtractor()
    return extractor.extract_single(url, **kwargs)


def extract_multiple_enterprises(urls: List[str], **kwargs) -> ExtractionResult:
    """Быстрое извлечение данных нескольких предприятий"""
    extractor = EnterpriseDataExtractor()
    return extractor.extract_batch(urls, **kwargs)


def extract_from_file(file_path: str, **kwargs) -> ExtractionResult:
    """Быстрое извлечение данных из файла"""
    extractor = EnterpriseDataExtractor()
    return extractor.extract_from_file(file_path, **kwargs)


if __name__ == "__main__":
    # Пример использования
    extractor = EnterpriseDataExtractor()
    
    # Тестовый URL (замените на реальный)
    test_url = "https://yandex.com.ge/maps/-/CHXU6Fmb"
    
    print("🚀 Тестирование EnterpriseDataExtractor")
    print("=" * 50)
    
    try:
        result = extractor.extract_single(
            url=test_url,
            export_formats=['json', 'csv'],
            include_services=True,
            include_reviews=True
        )
        
        if result['success']:
            print("✅ Извлечение выполнено успешно!")
            print(f"📊 Время обработки: {result['processing_time']}s")
            print(f"📁 Экспортированные файлы:")
            for format_type, path in result['export_paths'].items():
                print(f"   • {format_type}: {path}")
            
            # Выводим краткую информацию о данных
            data = result.get('data', {})
            print(f"\n📋 Информация о предприятии:")
            print(f"   • Название: {data.get('name', 'Не указано')}")
            print(f"   • Категория: {data.get('category', 'Не указано')}")
            print(f"   • Рейтинг: {data.get('rating', 'Не указано')}")
            print(f"   • Услуг извлечено: {len(data.get('services', []))}")
            print(f"   • Отзывов извлечено: {len(data.get('reviews', []))}")
            
        else:
            print("❌ Ошибка извлечения:", result['error'])
            
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
    
    # Выводим статистику сессии
    print(f"\n📈 Статистика сессии:")
    stats = extractor.get_session_statistics()
    for key, value in stats.items():
        if key != 'session_start':
            print(f"   • {key}: {value}")