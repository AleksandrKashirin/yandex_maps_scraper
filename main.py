#!/usr/bin/env python3
"""
Главный скрипт для запуска системы извлечения данных с Яндекс.Карт
Обновленная версия с использованием EnterpriseDataExtractor
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import List

# Добавляем корневую директорию в Python path
sys.path.insert(0, str(Path(__file__).parent))

from config.settings import settings
from core.logger import get_logger
from enterprise_data_extractor import EnterpriseDataExtractor


def setup_directories():
    """Создание необходимых директорий"""
    directories = [Path(settings.OUTPUT_PATH), Path("logs"), Path("data")]

    for directory in directories:
        directory.mkdir(exist_ok=True)


def print_extraction_summary(result):
    """Красивый вывод результатов извлечения"""
    print("\n" + "=" * 60)
    print("📊 РЕЗУЛЬТАТЫ ИЗВЛЕЧЕНИЯ")
    print("=" * 60)

    if hasattr(result, "get_summary"):  # Batch result
        summary = result.get_summary()

        print(f"🎯 Общая статистика:")
        print(f"   • Всего URL: {summary['total_urls']}")
        print(f"   • Успешно: {summary['successful']} ({summary['success_rate']:.1f}%)")
        print(f"   • Неудачно: {summary['failed']}")
        print(f"   • Время обработки: {summary['processing_time']:.2f}s")
        print(f"   • Среднее время на URL: {summary['avg_time_per_url']:.2f}s")

        if result.export_paths:
            print(f"\n📁 Экспортированные файлы:")
            for format_type, paths in result.export_paths.items():
                if isinstance(paths, list):
                    print(f"   • {format_type}: {len(paths)} файлов")
                    for path in paths[:3]:  # Показываем первые 3
                        print(f"     - {path}")
                    if len(paths) > 3:
                        print(f"     ... и еще {len(paths) - 3} файлов")
                else:
                    print(f"   • {format_type}: {paths}")

        # Показываем ошибки
        if result.failed_extractions:
            print(f"\n❌ Ошибки:")
            for failure in result.failed_extractions[:5]:  # Показываем первые 5
                print(f"   • {failure['url']}: {failure['error']}")
            if len(result.failed_extractions) > 5:
                print(f"   ... и еще {len(result.failed_extractions) - 5} ошибок")

    else:  # Single result
        if result["success"]:
            print(f"✅ Извлечение выполнено успешно!")
            print(f"⏱️  Время обработки: {result['processing_time']:.2f}s")

            # Информация о предприятии
            data = result.get("data", {})
            if data:
                print(f"\n🏢 Информация о предприятии:")
                print(f"   • Название: {data.get('name', 'Не указано')}")
                print(f"   • Категория: {data.get('category', 'Не указано')}")
                print(f"   • Адрес: {data.get('address', 'Не указано')}")
                print(f"   • Рейтинг: {data.get('rating', 'Не указано')}")
                print(
                    f"   • Количество отзывов: {data.get('reviews_count', 'Не указано')}"
                )
                print(f"   • Услуг извлечено: {len(data.get('services', []))}")
                print(f"   • Отзывов извлечено: {len(data.get('reviews', []))}")

            # Пути к файлам
            if result.get("export_paths"):
                print(f"\n📁 Файлы результатов:")
                for format_type, path in result["export_paths"].items():
                    if not format_type.endswith("_error"):
                        print(f"   • {format_type.upper()}: {path}")

            # Качество данных
            quality = result.get("data_quality", {})
            if quality:
                print(f"\n📈 Качество данных:")
                print(f"   • Полнота: {quality.get('completeness', 0):.0%}")
                print(
                    f"   • Контактная информация: {'✅' if quality.get('has_contact_info') else '❌'}"
                )
                print(f"   • Услуги: {'✅' if quality.get('has_services') else '❌'}")
                print(f"   • Отзывы: {'✅' if quality.get('has_reviews') else '❌'}")
                print(f"   • Рейтинг: {'✅' if quality.get('has_rating') else '❌'}")
                print(
                    f"   • Соц. сети: {'✅' if quality.get('has_social_networks') else '❌'}"
                )
        else:
            print(f"❌ Ошибка извлечения: {result['error']}")

    print("=" * 60)


def main():
    """Главная функция с улучшенным интерфейсом"""
    parser = argparse.ArgumentParser(
        description="🗺️  Система извлечения данных с Яндекс.Карт",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
🚀 Примеры использования:

  # Извлечение данных одного предприятия
  python main.py --url "https://yandex.com.ge/maps/-/CHXU6Fmb"
  
  # Извлечение из файла со списком URL  
  python main.py --file urls.txt --format json csv
  
  # Настройка задержки и параллелизма
  python main.py --file urls.txt --delay 10 --workers 1
  
  # Экспорт во все форматы
  python main.py --url "https://yandex.com.ge/maps/-/CHXU6Fmb" --format json csv database
  
  # Валидация URL без извлечения
  python main.py --validate-file urls.txt

📋 Поддерживаемые форматы экспорта: json, csv, database
        """,
    )

    # Основные параметры
    parser.add_argument("--url", type=str, help="URL страницы предприятия")
    parser.add_argument(
        "--file", type=str, help="Файл со списком URL (по одному на строку)"
    )

    # Форматы экспорта
    parser.add_argument(
        "--format",
        nargs="+",
        choices=["json", "csv", "database"],
        default=["json"],
        help="Форматы экспорта (можно указать несколько)",
    )

    # Настройки обработки
    parser.add_argument(
        "--output", type=str, help="Директория для сохранения результатов"
    )
    parser.add_argument(
        "--delay", type=float, help="Задержка между запросами в секундах"
    )
    parser.add_argument(
        "--workers", type=int, default=1, help="Количество параллельных потоков"
    )

    # Настройки контента
    parser.add_argument(
        "--no-services", action="store_true", help="Не извлекать услуги"
    )
    parser.add_argument("--no-reviews", action="store_true", help="Не извлекать отзывы")
    parser.add_argument("--max-reviews", type=int, default=10, help="Максимум отзывов")

    # Утилиты
    parser.add_argument(
        "--validate-file", type=str, help="Только валидировать URL в файле"
    )
    parser.add_argument(
        "--debug", action="store_true", help="Включить отладочный режим"
    )
    parser.add_argument(
        "--stats", action="store_true", help="Показать только статистику без обработки"
    )

    args = parser.parse_args()

    # Настройка отладки
    if args.debug:
        os.environ["DEBUG"] = "True"
        os.environ["LOG_LEVEL"] = "DEBUG"

    # Создаем необходимые директории
    setup_directories()

    logger = get_logger(__name__)

    print("🗺️  Система извлечения данных с Яндекс.Карт")
    print("=" * 60)

    # Обновляем путь вывода если задан
    if args.output:
        settings.OUTPUT_PATH = args.output

    # Создаем экстрактор
    try:
        extractor_config = {}
        extractor = EnterpriseDataExtractor(extractor_config)

        # Показать статистику
        if args.stats:
            stats = extractor.get_session_statistics()
            print("📊 Статистика текущей сессии:")
            for key, value in stats.items():
                if key != "session_start":
                    print(f"   • {key}: {value}")
            return

        # Валидация файла
        if args.validate_file:
            if not Path(args.validate_file).exists():
                print(f"❌ Файл не найден: {args.validate_file}")
                sys.exit(1)

            with open(args.validate_file, "r", encoding="utf-8") as f:
                urls = [
                    line.strip()
                    for line in f
                    if line.strip() and not line.startswith("#")
                ]

            print(f"🔍 Валидация {len(urls)} URL из файла {args.validate_file}")
            validation_result = extractor.validate_urls(urls)

            print(f"\n✅ Валидных URL: {validation_result['valid_count']}")
            print(f"❌ Невалидных URL: {validation_result['invalid_count']}")

            if validation_result["invalid"]:
                print(f"\n❌ Невалидные URL:")
                for url in validation_result["invalid"][:10]:  # Показываем первые 10
                    print(f"   • {url}")
                if len(validation_result["invalid"]) > 10:
                    print(f"   ... и еще {len(validation_result['invalid']) - 10}")

            return

        # Извлечение одного URL
        if args.url:
            print(f"🎯 Извлечение данных из: {args.url}")

            result = extractor.extract_single(
                url=args.url,
                export_formats=args.format,
                output_dir=args.output,
                include_services=not args.no_services,
                include_reviews=not args.no_reviews,
                max_reviews=args.max_reviews,
            )

            print_extraction_summary(result)

            if not result["success"]:
                sys.exit(1)

        # Извлечение из файла
        elif args.file:
            if not Path(args.file).exists():
                print(f"❌ Файл не найден: {args.file}")
                sys.exit(1)

            # Читаем URL из файла
            with open(args.file, "r", encoding="utf-8") as f:
                urls = [
                    line.strip()
                    for line in f
                    if line.strip() and not line.startswith("#")
                ]

            if not urls:
                print(f"❌ В файле {args.file} не найдено валидных URL")
                sys.exit(1)

            print(f"📋 Найдено {len(urls)} URL для обработки")

            # Валидируем URLs
            validation = extractor.validate_urls(urls)
            if validation["invalid_count"] > 0:
                print(f"⚠️  Обнаружено {validation['invalid_count']} невалидных URL")

            # Обрабатываем только валидные URLs
            if validation["valid"]:
                result = extractor.extract_batch(
                    urls=validation["valid"],
                    export_formats=args.format,
                    output_dir=args.output,
                    delay_between=args.delay,
                    max_workers=args.workers,
                )

                print_extraction_summary(result)

                # Выводим финальную статистику сессии
                print(f"\n📈 Итоговая статистика сессии:")
                stats = extractor.get_session_statistics()
                for key, value in stats.items():
                    if key not in ["session_start", "total_processing_time"]:
                        print(f"   • {key}: {value}")
            else:
                print("❌ Не найдено валидных URL для обработки")
                sys.exit(1)
        else:
            print("❌ Необходимо указать --url или --file")
            parser.print_help()
            sys.exit(1)

    except KeyboardInterrupt:
        print("\n⚠️  Прерывание пользователем")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        print(f"❌ Критическая ошибка: {e}")
        if args.debug:
            import traceback

            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
