"""
Модуль экспорта и сохранения данных для системы скрапинга Яндекс.Карт

Поддерживает экспорт в различные форматы: JSON, CSV, База данных
"""

import csv
import json
import sqlite3
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from pydantic import BaseModel

from .json_exporter import JSONExporter

# Версия экспортеров
EXPORTERS_VERSION = "1.0"

__all__ = [
    "BaseExporter",
    "JSONExporter",
    "CSVExporter",
    "DatabaseExporter",
    "UnifiedExporter",
    "ExportMetadata",
    "EXPORTERS_VERSION",
]


class ExportMetadata(BaseModel):
    """Метаданные экспорта"""

    export_timestamp: datetime
    exporter_version: str = EXPORTERS_VERSION
    source_url: Optional[str] = None
    records_count: int = 0
    export_format: str
    file_size_bytes: Optional[int] = None
    checksum: Optional[str] = None
    export_options: Dict[str, Any] = {}

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


class BaseExporter(ABC):
    """Базовый класс для всех экспортеров"""

    def __init__(self, output_dir: Union[str, Path] = "data"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True, parents=True)
        self.metadata = None

    @abstractmethod
    def export(self, data: Any, filename: str = None, **kwargs) -> str:
        """Экспорт данных (должен быть реализован в дочерних классах)"""
        pass

    def generate_filename(self, enterprise_name: str, format_ext: str) -> str:
        """Генерация имени файла"""
        # Очищаем название предприятия для имени файла
        safe_name = "".join(
            c for c in enterprise_name if c.isalnum() or c in (" ", "-", "_")
        )
        safe_name = safe_name.replace(" ", "_")[:50]

        # Добавляем timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        return f"{safe_name}_{timestamp}.{format_ext}"

    def create_metadata(
        self, data: Any, export_format: str, file_path: str = None, **kwargs
    ) -> ExportMetadata:
        """Создание метаданных экспорта"""

        # Подсчитываем количество записей
        records_count = 0
        if isinstance(data, list):
            records_count = len(data)
        elif isinstance(data, dict):
            records_count = 1
        elif hasattr(data, "__len__"):
            try:
                records_count = len(data)
            except:
                records_count = 1

        # Размер файла
        file_size = None
        if file_path and Path(file_path).exists():
            file_size = Path(file_path).stat().st_size

        # Извлекаем source_url из данных
        source_url = None
        if isinstance(data, dict) and "metadata" in data:
            source_url = data["metadata"].get("source_url")
        elif hasattr(data, "metadata") and hasattr(data.metadata, "get"):
            source_url = data.metadata.get("source_url")

        metadata = ExportMetadata(
            export_timestamp=datetime.now(),
            exporter_version=EXPORTERS_VERSION,
            source_url=source_url,
            records_count=records_count,
            export_format=export_format,
            file_size_bytes=file_size,
            export_options=kwargs,
        )

        self.metadata = metadata
        return metadata


class CSVExporter(BaseExporter):
    """Экспортер в CSV формат через Pandas"""

    def export(
        self,
        data: Any,
        filename: str = None,
        flatten_nested: bool = True,
        include_metadata: bool = True,
        encoding: str = "utf-8",
        **kwargs,
    ) -> str:
        """
        Экспорт данных в CSV формат

        Args:
            data: Данные для экспорта
            filename: Имя файла (генерируется автоматически если не указан)
            flatten_nested: Разворачивать вложенные структуры
            include_metadata: Включать метаданные
            encoding: Кодировка файла
            **kwargs: Дополнительные параметры pandas.to_csv()

        Returns:
            str: Путь к сохраненному файлу
        """

        # Подготовка данных для CSV
        if hasattr(data, "model_dump"):
            # Pydantic модель
            csv_data = data.model_dump()
        elif isinstance(data, dict):
            csv_data = data.copy()
        else:
            raise ValueError("Неподдерживаемый тип данных для CSV экспорта")

        # Генерируем имя файла если не указано
        if not filename:
            enterprise_name = csv_data.get("name", "enterprise")
            filename = self.generate_filename(enterprise_name, "csv")

        file_path = self.output_dir / filename

        # Подготавливаем данные для DataFrame
        if flatten_nested:
            flattened_data = self._flatten_data(csv_data)
        else:
            flattened_data = csv_data

        # Создаем DataFrame
        if isinstance(flattened_data, list):
            df = pd.DataFrame(flattened_data)
        else:
            df = pd.DataFrame([flattened_data])

        # Настройки экспорта по умолчанию
        export_kwargs = {
            "index": False,
            "encoding": encoding,
            "quoting": csv.QUOTE_NONNUMERIC,
            **kwargs,
        }

        # Сохраняем в CSV
        df.to_csv(file_path, **export_kwargs)

        # Создаем метаданные
        metadata = self.create_metadata(
            data,
            "csv",
            str(file_path),
            flatten_nested=flatten_nested,
            encoding=encoding,
            **kwargs,
        )

        # Сохраняем метаданные рядом с основным файлом
        if include_metadata:
            metadata_path = file_path.with_suffix(".csv.meta.json")
            with open(metadata_path, "w", encoding="utf-8") as f:
                json.dump(metadata.model_dump(), f, ensure_ascii=False, indent=2)

        return str(file_path)

    def _flatten_data(self, data: Dict, prefix: str = "") -> Dict:
        """Разворачивание вложенных структур для CSV"""

        flattened = {}

        for key, value in data.items():
            new_key = f"{prefix}{key}" if prefix else key

            if isinstance(value, dict):
                # Рекурсивно разворачиваем словари
                flattened.update(self._flatten_data(value, f"{new_key}_"))

            elif isinstance(value, list):
                # Для списков создаем сводную информацию
                if value:
                    flattened[f"{new_key}_count"] = len(value)

                    # Если это список словарей, пытаемся извлечь ключевые поля
                    if isinstance(value[0], dict):
                        for i, item in enumerate(
                            value[:3]
                        ):  # Ограничиваем первыми 3 элементами
                            for item_key, item_value in item.items():
                                if not isinstance(item_value, (dict, list)):
                                    flattened[f"{new_key}_{i+1}_{item_key}"] = (
                                        item_value
                                    )

                    # Иначе просто объединяем в строку
                    else:
                        flattened[f"{new_key}_joined"] = "; ".join(
                            str(v) for v in value[:5]
                        )
                else:
                    flattened[f"{new_key}_count"] = 0

            else:
                # Простые значения добавляем как есть
                flattened[new_key] = value

        return flattened

    def export_multiple(
        self, data_list: List[Any], filename: str = None, **kwargs
    ) -> str:
        """Экспорт нескольких записей в один CSV файл"""

        if not data_list:
            raise ValueError("Список данных не может быть пустым")

        # Генерируем имя файла
        if not filename:
            filename = (
                f"multiple_enterprises_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            )

        file_path = self.output_dir / filename

        # Подготавливаем все записи
        flattened_records = []
        for record in data_list:
            if hasattr(record, "model_dump"):
                record_data = record.model_dump()
            elif isinstance(record, dict):
                record_data = record.copy()
            else:
                continue

            flattened_record = self._flatten_data(record_data)
            flattened_records.append(flattened_record)

        # Создаем DataFrame и сохраняем
        df = pd.DataFrame(flattened_records)

        export_kwargs = {
            "index": False,
            "encoding": "utf-8",
            "quoting": csv.QUOTE_NONNUMERIC,
            **kwargs,
        }

        df.to_csv(file_path, **export_kwargs)

        # Создаем метаданные
        self.create_metadata(data_list, "csv", str(file_path), **kwargs)

        return str(file_path)


class DatabaseExporter(BaseExporter):
    """Экспортер в базу данных"""

    def __init__(self, output_dir: Union[str, Path] = "data", db_path: str = None):
        super().__init__(output_dir)
        self.db_path = db_path or str(self.output_dir / "enterprises.db")
        self._init_database()

    def _init_database(self):
        """Инициализация схемы базы данных"""

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Таблица предприятий
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS enterprises (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    category TEXT,
                    address TEXT,
                    phone TEXT,
                    website TEXT,
                    rating REAL,
                    reviews_count INTEGER,
                    scraping_date TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(name, address)
                )
            """
            )

            # Таблица услуг
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS services (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    enterprise_id INTEGER,
                    name TEXT NOT NULL,
                    price TEXT,
                    price_from TEXT,
                    price_to TEXT,
                    description TEXT,
                    duration TEXT,
                    FOREIGN KEY (enterprise_id) REFERENCES enterprises (id)
                )
            """
            )

            # Таблица отзывов
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS reviews (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    enterprise_id INTEGER,
                    author TEXT NOT NULL,
                    rating INTEGER,
                    date TEXT,
                    text TEXT,
                    response TEXT,
                    helpful_count INTEGER,
                    FOREIGN KEY (enterprise_id) REFERENCES enterprises (id)
                )
            """
            )

            # Таблица социальных сетей
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS social_networks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    enterprise_id INTEGER,
                    network_type TEXT NOT NULL,
                    url TEXT NOT NULL,
                    FOREIGN KEY (enterprise_id) REFERENCES enterprises (id),
                    UNIQUE(enterprise_id, network_type)
                )
            """
            )

            # Таблица метаданных экспорта
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS export_metadata (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    enterprise_id INTEGER,
                    export_timestamp TIMESTAMP,
                    source_url TEXT,
                    exporter_version TEXT,
                    processing_time_sec REAL,
                    FOREIGN KEY (enterprise_id) REFERENCES enterprises (id)
                )
            """
            )

            conn.commit()

    def export(self, data: Any, **kwargs) -> str:
        """
        Экспорт данных в базу данных

        Args:
            data: Данные предприятия для экспорта
            **kwargs: Дополнительные параметры

        Returns:
            str: Информация о сохранении
        """

        if hasattr(data, "model_dump"):
            enterprise_data = data.model_dump()
        elif isinstance(data, dict):
            enterprise_data = data.copy()
        else:
            raise ValueError("Неподдерживаемый тип данных для экспорта в БД")

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Вставляем основную информацию о предприятии
            enterprise_id = self._insert_enterprise(cursor, enterprise_data)

            # Вставляем услуги
            if "services" in enterprise_data and enterprise_data["services"]:
                self._insert_services(
                    cursor, enterprise_id, enterprise_data["services"]
                )

            # Вставляем отзывы
            if "reviews" in enterprise_data and enterprise_data["reviews"]:
                self._insert_reviews(cursor, enterprise_id, enterprise_data["reviews"])

            # Вставляем социальные сети
            if (
                "social_networks" in enterprise_data
                and enterprise_data["social_networks"]
            ):
                self._insert_social_networks(
                    cursor, enterprise_id, enterprise_data["social_networks"]
                )

            # Вставляем метаданные
            self._insert_metadata(
                cursor, enterprise_id, enterprise_data.get("metadata", {})
            )

            conn.commit()

        # Создаем метаданные экспорта
        self.create_metadata(data, "database", self.db_path, **kwargs)

        return f"Данные сохранены в БД: {self.db_path}, ID записи: {enterprise_id}"

    def _insert_enterprise(self, cursor, data: Dict) -> int:
        """Вставка основной информации о предприятии"""

        cursor.execute(
            """
            INSERT OR REPLACE INTO enterprises 
            (name, category, address, phone, website, rating, reviews_count, scraping_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                data.get("name"),
                data.get("category"),
                data.get("address"),
                data.get("phone"),
                data.get("website"),
                data.get("rating"),
                data.get("reviews_count"),
                data.get("scraping_date"),
            ),
        )

        return cursor.lastrowid

    def _insert_services(self, cursor, enterprise_id: int, services: List[Dict]):
        """Вставка услуг"""

        # Сначала удаляем старые услуги
        cursor.execute("DELETE FROM services WHERE enterprise_id = ?", (enterprise_id,))

        # Вставляем новые
        for service in services:
            cursor.execute(
                """
                INSERT INTO services 
                (enterprise_id, name, price, price_from, price_to, description, duration)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    enterprise_id,
                    service.get("name"),
                    service.get("price"),
                    service.get("price_from"),
                    service.get("price_to"),
                    service.get("description"),
                    service.get("duration"),
                ),
            )

    def _insert_reviews(self, cursor, enterprise_id: int, reviews: List[Dict]):
        """Вставка отзывов"""

        # Удаляем старые отзывы
        cursor.execute("DELETE FROM reviews WHERE enterprise_id = ?", (enterprise_id,))

        # Вставляем новые
        for review in reviews:
            cursor.execute(
                """
                INSERT INTO reviews 
                (enterprise_id, author, rating, date, text, response, helpful_count)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    enterprise_id,
                    review.get("author"),
                    review.get("rating"),
                    review.get("date"),
                    review.get("text"),
                    review.get("response"),
                    review.get("helpful_count"),
                ),
            )

    def _insert_social_networks(
        self, cursor, enterprise_id: int, social_networks: Dict
    ):
        """Вставка социальных сетей"""

        # Удаляем старые записи
        cursor.execute(
            "DELETE FROM social_networks WHERE enterprise_id = ?", (enterprise_id,)
        )

        # Вставляем новые
        for network_type, url in social_networks.items():
            if url:  # Только если URL не пустой
                cursor.execute(
                    """
                    INSERT INTO social_networks (enterprise_id, network_type, url)
                    VALUES (?, ?, ?)
                """,
                    (enterprise_id, network_type, url),
                )

    def _insert_metadata(self, cursor, enterprise_id: int, metadata: Dict):
        """Вставка метаданных"""

        cursor.execute(
            """
            INSERT INTO export_metadata 
            (enterprise_id, export_timestamp, source_url, exporter_version, processing_time_sec)
            VALUES (?, ?, ?, ?, ?)
        """,
            (
                enterprise_id,
                datetime.now().isoformat(),
                metadata.get("source_url"),
                metadata.get("scraper_version", EXPORTERS_VERSION),
                metadata.get("processing_time_sec"),
            ),
        )

    def query_enterprises(self, **filters) -> List[Dict]:
        """Запрос предприятий из базы данных"""

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row  # Для получения результатов как словарей
            cursor = conn.cursor()

            # Базовый запрос
            query = "SELECT * FROM enterprises WHERE 1=1"
            params = []

            # Добавляем фильтры
            if "category" in filters:
                query += " AND category LIKE ?"
                params.append(f"%{filters['category']}%")

            if "rating_min" in filters:
                query += " AND rating >= ?"
                params.append(filters["rating_min"])

            if "city" in filters:
                query += " AND address LIKE ?"
                params.append(f"%{filters['city']}%")

            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]


class UnifiedExporter:
    """Унифицированный экспортер с поддержкой всех форматов"""

    def __init__(self, output_dir: Union[str, Path] = "data"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True, parents=True)

        self.json_exporter = JSONExporter(output_dir)
        self.csv_exporter = CSVExporter(output_dir)
        self.db_exporter = DatabaseExporter(output_dir)

    def export_all_formats(
        self, data: Any, base_filename: str = None
    ) -> Dict[str, str]:
        """Экспорт во все поддерживаемые форматы"""

        results = {}

        try:
            # JSON экспорт
            json_path = self.json_exporter.export(data, base_filename)
            results["json"] = json_path
        except Exception as e:
            results["json_error"] = str(e)

        try:
            # CSV экспорт
            csv_filename = f"{base_filename}.csv" if base_filename else None
            csv_path = self.csv_exporter.export(data, csv_filename)
            results["csv"] = csv_path
        except Exception as e:
            results["csv_error"] = str(e)

        try:
            # Database экспорт
            db_result = self.db_exporter.export(data)
            results["database"] = db_result
        except Exception as e:
            results["database_error"] = str(e)

        return results

    def export_by_format(self, data: Any, format_type: str, **kwargs) -> str:
        """Экспорт в указанный формат"""

        if format_type.lower() == "json":
            return self.json_exporter.export(data, **kwargs)
        elif format_type.lower() == "csv":
            return self.csv_exporter.export(data, **kwargs)
        elif format_type.lower() in ["db", "database", "sqlite"]:
            return self.db_exporter.export(data, **kwargs)
        else:
            raise ValueError(f"Неподдерживаемый формат: {format_type}")


# Удобные функции для быстрого экспорта
def export_to_json(data: Any, output_dir: str = "data", **kwargs) -> str:
    """Быстрый экспорт в JSON"""
    exporter = JSONExporter(output_dir)
    return exporter.export(data, **kwargs)


def export_to_csv(data: Any, output_dir: str = "data", **kwargs) -> str:
    """Быстрый экспорт в CSV"""
    exporter = CSVExporter(output_dir)
    return exporter.export(data, **kwargs)


def export_to_database(data: Any, output_dir: str = "data", **kwargs) -> str:
    """Быстрый экспорт в базу данных"""
    exporter = DatabaseExporter(output_dir)
    return exporter.export(data, **kwargs)
