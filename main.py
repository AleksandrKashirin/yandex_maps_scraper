#!/usr/bin/env python3
"""
Основной скрипт для запуска системы скрапинга Яндекс.Карт
"""
import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import List, Optional

# Добавляем корневую директорию в Python path
sys.path.insert(0, str(Path(__file__).parent))

from config.settings import settings
from core.logger import get_logger, scraping_metrics
from scrapper import BusinessData, YandexMapsScraper


def setup_directories():
    """Создание необходимых директорий"""
    directories = [Path(settings.OUTPUT_PATH), Path("logs"), Path("data")]

    for directory in directories:
        directory.mkdir(exist_ok=True)


def save_results(
    data: BusinessData, output_format: str = "json", output_path: str = None
) -> str:
    """
    Сохранение результатов скрапинга

    Args:
        data: Данные для сохранения
        output_format: Формат вывода (json, csv)
        output_path: Путь для сохранения

    Returns:
        str: Путь к сохраненному файлу
    """
    if output_path is None:
        output_path = settings.OUTPUT_PATH

    output_dir = Path(output_path)
    output_dir.mkdir(exist_ok=True)

    # Генерируем имя файла на основе названия предприятия и времени
    safe_name = "".join(
        c for c in data.name if c.isalnum() or c in (" ", "-", "_")
    ).rstrip()
    safe_name = safe_name.replace(" ", "_")[:50]  # Ограничиваем длину

    timestamp = data.scraping_date.strftime("%Y%m%d_%H%M%S")
    filename = f"{safe_name}_{timestamp}"

    if output_format.lower() == "json":
        filepath = output_dir / f"{filename}.json"

        # Конвертируем в JSON-совместимый формат
        json_data = data.model_dump(mode="json")

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2)

    elif output_format.lower() == "csv":
        import pandas as pd

        filepath = output_dir / f"{filename}.csv"

        # Создаем DataFrame с основными данными
        basic_data = {
            "name": data.name,
            "category": data.category,
            "rating": data.rating,
            "reviews_count": data.reviews_count,
            "address": data.address,
            "phone": data.phone,
            "website": data.website,
            "services_count": len(data.services),
            "reviews_extracted": len(data.reviews),
            "scraping_date": data.scraping_date.isoformat(),
        }

        df = pd.DataFrame([basic_data])
        df.to_csv(filepath, index=False, encoding="utf-8")

    return str(filepath)


def scrape_single_url(url: str, output_format: str = "json") -> Optional[str]:
    """
    Скрапинг одного URL

    Args:
        url: URL для скрапинга
        output_format: Формат вывода

    Returns:
        str: Путь к файлу результата или None
    """
    logger = get_logger(__name__)

    try:
        with YandexMapsScraper() as scraper:
            logger.info(f"Начинаем скрапинг: {url}")

            result = scraper.scrape_business(url)

            if result:
                filepath = save_results(result, output_format)
                logger.info(f"Результаты сохранены в: {filepath}")
                return filepath
            else:
                logger.error("Не удалось извлечь данные")
                return None

    except Exception as e:
        logger.error(f"Ошибка скрапинга: {e}")
        return None


def scrape_multiple_urls(
    urls: List[str], output_format: str = "json", delay_between: float = None
) -> List[str]:
    """
    Скрапинг нескольких URL

    Args:
        urls: Список URL для скрапинга
        output_format: Формат вывода
        delay_between: Задержка между запросами

    Returns:
        List[str]: Список путей к файлам результатов
    """
    logger = get_logger(__name__)
    results = []

    if delay_between is None:
        delay_between = (settings.MIN_DELAY + settings.MAX_DELAY) / 2

    logger.info(f"Начинаем скрапинг {len(urls)} URL")

    for i, url in enumerate(urls, 1):
        logger.info(f"Обрабатываем URL {i}/{len(urls)}: {url}")

        try:
            result_path = scrape_single_url(url, output_format)
            if result_path:
                results.append(result_path)

            # Задержка между запросами (кроме последнего)
            if i < len(urls):
                logger.info(f"Пауза {delay_between}s перед следующим запросом")
                time.sleep(delay_between)

        except KeyboardInterrupt:
            logger.warning("Прерывание пользователем")
            break
        except Exception as e:
            logger.error(f"Ошибка обработки {url}: {e}")
            continue

    logger.info(f"Обработка завершена. Успешно: {len(results)}/{len(urls)}")
    scraping_metrics.log_summary()

    return results


def main():
    """Главная функция"""
    parser = argparse.ArgumentParser(
        description="Система извлечения данных с Яндекс.Карт",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:

  # Скрапинг одного предприятия
  python main.py --url "https://yandex.com.ge/maps/-/CHXU6Fmb"
  
  # Скрапинг из файла со списком URL
  python main.py --file urls.txt --format csv
  
  # Настройка задержки между запросами
  python main.py --file urls.txt --delay 10
        """,
    )

    parser.add_argument("--url", type=str, help="URL страницы предприятия")
    parser.add_argument(
        "--file", type=str, help="Файл со списком URL (по одному на строку)"
    )
    parser.add_argument(
        "--format",
        choices=["json", "csv"],
        default="json",
        help="Формат вывода (по умолчанию: json)",
    )
    parser.add_argument(
        "--output", type=str, help="Директория для сохранения результатов"
    )
    parser.add_argument(
        "--delay", type=float, help="Задержка между запросами в секундах"
    )
    parser.add_argument(
        "--debug", action="store_true", help="Включить отладочный режим"
    )

    args = parser.parse_args()

    # Настройка отладки
    if args.debug:
        os.environ["DEBUG"] = "True"
        os.environ["LOG_LEVEL"] = "DEBUG"

    # Создаем необходимые директории
    setup_directories()

    logger = get_logger(__name__)
    logger.info("Запуск системы скрапинга Яндекс.Карт")

    # Обновляем путь вывода если задан
    if args.output:
        settings.OUTPUT_PATH = args.output

    try:
        if args.url:
            # Скрапинг одного URL
            result = scrape_single_url(args.url, args.format)
            if result:
                print(f"✅ Результат сохранен: {result}")
            else:
                print("❌ Не удалось извлечь данные")
                sys.exit(1)

        elif args.file:
            # Скрапинг из файла
            urls_file = Path(args.file)
            if not urls_file.exists():
                print(f"❌ Файл не найден: {args.file}")
                sys.exit(1)

            # Читаем URL из файла
            with open(urls_file, "r", encoding="utf-8") as f:
                urls = [line.strip() for line in f if line.strip()]

            if not urls:
                print(f"❌ В файле {args.file} не найдено валидных URL")
                sys.exit(1)

            print(f"📋 Найдено {len(urls)} URL для обработки")

            results = scrape_multiple_urls(urls, args.format, args.delay)

            print(f"\n✅ Обработка завершена!")
            print(f"📊 Успешно обработано: {len(results)}/{len(urls)}")

            if results:
                print("\n📁 Файлы результатов:")
                for result_path in results:
                    print(f"  • {result_path}")
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
        sys.exit(1)


if __name__ == "__main__":
    main()
