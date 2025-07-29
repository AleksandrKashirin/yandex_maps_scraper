"""
JSON экспортер для данных предприятий с полной поддержкой схемы ТЗ
"""

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from pydantic import ValidationError

from .base_exporter import BaseExporter, ExportMetadata


class JSONExporter(BaseExporter):
    """Экспортер в JSON формат согласно схеме технического задания"""

    def __init__(self, output_dir: Union[str, Path] = "data"):
        super().__init__(output_dir)
        self.schema_version = "1.0"

    def export(
        self,
        data: Any,
        filename: str = None,
        indent: int = 2,
        ensure_ascii: bool = False,
        include_metadata: bool = True,
        validate_schema: bool = True,
        compress: bool = False,
        **kwargs,
    ) -> str:
        """
        Экспорт данных в JSON формат

        Args:
            data: Данные для экспорта (Enterprise модель или dict)
            filename: Имя файла (генерируется автоматически если не указан)
            indent: Отступы для форматирования JSON
            ensure_ascii: Экранировать не-ASCII символы
            include_metadata: Включать метаданные экспорта
            validate_schema: Валидировать данные перед экспортом
            compress: Сжать JSON (минифицировать)
            **kwargs: Дополнительные параметры

        Returns:
            str: Путь к сохраненному файлу
        """

        # Валидация данных
        if validate_schema:
            validated_data = self._validate_data(data)
        else:
            validated_data = self._prepare_data(data)

        # Обогащение метаданными
        if include_metadata:
            validated_data = self._enrich_with_export_metadata(validated_data)

        # Генерация имени файла
        if not filename:
            enterprise_name = validated_data.get("name", "enterprise")
            filename = self.generate_filename(enterprise_name, "json")

        file_path = self.output_dir / filename

        # Настройки JSON сериализации
        json_options = {
            "ensure_ascii": ensure_ascii,
            "separators": (",", ":") if compress else None,
            "indent": None if compress else indent,
            **kwargs,
        }

        # Сохранение в файл
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(validated_data, f, default=self._json_serializer, **json_options)

        # Создание метаданных экспорта
        metadata = self.create_metadata(
            data,
            "json",
            str(file_path),
            schema_version=self.schema_version,
            compress=compress,
            validate_schema=validate_schema,
            **kwargs,
        )

        # Добавляем checksum
        metadata.checksum = self._calculate_checksum(file_path)

        # Сохраняем метаданные в отдельный файл
        if include_metadata:
            self._save_metadata_file(file_path, metadata)

        return str(file_path)

    def _validate_data(self, data: Any) -> Dict[str, Any]:
        """Валидация данных согласно схеме"""

        try:
            # Если это уже Pydantic модель
            if hasattr(data, "model_dump"):
                return data.model_dump(mode="json")

            # Если это словарь, пытаемся создать модель
            elif isinstance(data, dict):
                # Импортируем модель Enterprise
                try:
                    from ..models.enterprise import Enterprise

                    # Создаем и валидируем модель
                    enterprise = Enterprise.model_validate(data)
                    return enterprise.model_dump(mode="json")

                except ImportError:
                    # Если модели недоступны, используем базовую валидацию
                    return self._basic_validation(data)
                except ValidationError as e:
                    # Логируем ошибки валидации но не прерываем экспорт
                    validated_data = self._basic_validation(data)
                    validated_data["_validation_errors"] = [
                        str(error) for error in e.errors()
                    ]
                    return validated_data

            else:
                raise ValueError(f"Неподдерживаемый тип данных: {type(data)}")

        except Exception as e:
            # В случае ошибки возвращаем данные как есть с пометкой об ошибке
            error_data = self._prepare_data(data)
            error_data["_export_errors"] = [f"Validation error: {str(e)}"]
            return error_data

    def _basic_validation(self, data: Dict) -> Dict[str, Any]:
        """Базовая валидация данных без Pydantic"""

        validated = data.copy()

        # Проверяем обязательные поля
        required_fields = ["name", "scraping_date"]
        for field in required_fields:
            if field not in validated or not validated[field]:
                if field == "name":
                    validated[field] = "Unknown Enterprise"
                elif field == "scraping_date":
                    validated[field] = datetime.now().isoformat()

        # Приводим типы данных
        if "rating" in validated and validated["rating"] is not None:
            try:
                validated["rating"] = float(validated["rating"])
                if validated["rating"] < 0 or validated["rating"] > 5:
                    validated["rating"] = None
            except (ValueError, TypeError):
                validated["rating"] = None

        if "reviews_count" in validated and validated["reviews_count"] is not None:
            try:
                validated["reviews_count"] = int(validated["reviews_count"])
                if validated["reviews_count"] < 0:
                    validated["reviews_count"] = 0
            except (ValueError, TypeError):
                validated["reviews_count"] = None

        return validated

    def _prepare_data(self, data: Any) -> Dict[str, Any]:
        """Подготовка данных для экспорта"""

        if hasattr(data, "model_dump"):
            return data.model_dump(mode="json")
        elif isinstance(data, dict):
            return data.copy()
        else:
            return {"raw_data": str(data), "data_type": str(type(data))}

    def _enrich_with_export_metadata(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Обогащение данных метаданными экспорта"""

        enriched_data = data.copy()

        # Добавляем или обновляем метаданные
        if "metadata" not in enriched_data:
            enriched_data["metadata"] = {}

        metadata = enriched_data["metadata"]

        # Метаданные экспорта
        metadata.update(
            {
                "export_timestamp": datetime.now().isoformat(),
                "exporter_version": self.schema_version,
                "schema_version": self.schema_version,
                "export_format": "json",
            }
        )

        # Статистика по данным
        metadata["data_statistics"] = self._calculate_data_statistics(enriched_data)

        return enriched_data

    def _calculate_data_statistics(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Вычисление статистики по данным"""

        stats = {
            "total_fields": len(data),
            "filled_fields": len(
                [k for k, v in data.items() if v is not None and v != ""]
            ),
            "services_count": 0,
            "reviews_count": 0,
            "has_contact_info": False,
            "has_social_networks": False,
            "data_completeness_score": 0.0,
        }

        # Подсчет услуг
        if "services" in data and isinstance(data["services"], list):
            stats["services_count"] = len(data["services"])

        # Подсчет отзывов
        if "reviews" in data and isinstance(data["reviews"], list):
            stats["reviews_count"] = len(data["reviews"])

        # Проверка контактной информации
        contact_fields = ["phone", "website", "email"]
        stats["has_contact_info"] = any(data.get(field) for field in contact_fields)

        # Проверка социальных сетей
        if "social_networks" in data and isinstance(data["social_networks"], dict):
            stats["has_social_networks"] = any(data["social_networks"].values())

        # Общий балл полноты данных
        key_fields = [
            "name",
            "category",
            "address",
            "phone",
            "website",
            "rating",
            "reviews_count",
            "services",
            "social_networks",
            "working_hours",
        ]
        filled_key_fields = sum(1 for field in key_fields if data.get(field))
        stats["data_completeness_score"] = round(filled_key_fields / len(key_fields), 2)

        return stats

    def _json_serializer(self, obj: Any) -> Any:
        """Кастомный сериализатор для JSON"""

        # Обработка datetime объектов
        if isinstance(obj, datetime):
            return obj.isoformat()

        # Обработка Pydantic моделей
        if hasattr(obj, "model_dump"):
            return obj.model_dump(mode="json")

        # Обработка Path объектов
        if isinstance(obj, Path):
            return str(obj)

        # Обработка set
        if isinstance(obj, set):
            return list(obj)

        # Для остальных типов
        return str(obj)

    def _calculate_checksum(self, file_path: Union[str, Path]) -> str:
        """Вычисление MD5 чексуммы файла"""

        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def _save_metadata_file(self, data_file_path: Path, metadata: ExportMetadata):
        """Сохранение метаданных в отдельный файл"""

        metadata_path = data_file_path.with_suffix(".json.meta.json")

        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(
                metadata.model_dump(),
                f,
                ensure_ascii=False,
                indent=2,
                default=self._json_serializer,
            )

    def export_multiple(
        self,
        data_list: List[Any],
        filename: str = None,
        format_type: str = "array",
        **kwargs,
    ) -> str:
        """
        Экспорт нескольких предприятий в один файл

        Args:
            data_list: Список данных предприятий
            filename: Имя файла
            format_type: 'array' или 'object_collection'
            **kwargs: Дополнительные параметры

        Returns:
            str: Путь к файлу
        """

        if not data_list:
            raise ValueError("Список данных не может быть пустым")

        # Подготавливаем данные
        processed_data = []
        for data in data_list:
            try:
                validated = self._validate_data(data)
                processed_data.append(validated)
            except Exception as e:
                # Добавляем данные с ошибкой
                error_data = self._prepare_data(data)
                error_data["_processing_error"] = str(e)
                processed_data.append(error_data)

        # Формируем итоговую структуру
        if format_type == "array":
            final_data = processed_data
        elif format_type == "object_collection":
            final_data = {
                "enterprises": processed_data,
                "collection_metadata": {
                    "total_count": len(processed_data),
                    "export_timestamp": datetime.now().isoformat(),
                    "exporter_version": self.schema_version,
                    "format_type": format_type,
                },
            }
        else:
            raise ValueError(f"Неподдерживаемый format_type: {format_type}")

        # Генерируем имя файла
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"enterprises_collection_{timestamp}.json"

        file_path = self.output_dir / filename

        # Сохраняем
        json_options = {
            "ensure_ascii": kwargs.get("ensure_ascii", False),
            "indent": kwargs.get("indent", 2),
        }

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(final_data, f, default=self._json_serializer, **json_options)

        # Создаем метаданные
        metadata = self.create_metadata(
            data_list, "json", str(file_path), format_type=format_type, **kwargs
        )
        metadata.checksum = self._calculate_checksum(file_path)

        # Сохраняем метаданные
        self._save_metadata_file(file_path, metadata)

        return str(file_path)

    def export_summary(self, data: Any, filename: str = None) -> str:
        """Экспорт краткой сводки о предприятии"""

        # Подготавливаем данные
        if hasattr(data, "export_summary"):
            summary_data = data.export_summary()
        elif hasattr(data, "model_dump"):
            full_data = data.model_dump()
            summary_data = self._create_summary_from_dict(full_data)
        elif isinstance(data, dict):
            summary_data = self._create_summary_from_dict(data)
        else:
            raise ValueError("Невозможно создать сводку из данных")

        # Добавляем метаданные сводки
        summary_data["summary_metadata"] = {
            "created_at": datetime.now().isoformat(),
            "summary_type": "enterprise_overview",
            "exporter_version": self.schema_version,
        }

        # Генерируем имя файла
        if not filename:
            enterprise_name = summary_data.get("name", "enterprise")
            safe_name = "".join(
                c for c in enterprise_name if c.isalnum() or c in (" ", "-", "_")
            )
            safe_name = safe_name.replace(" ", "_")[:30]
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{safe_name}_summary_{timestamp}.json"

        file_path = self.output_dir / filename

        # Сохраняем
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(
                summary_data,
                f,
                ensure_ascii=False,
                indent=2,
                default=self._json_serializer,
            )

        return str(file_path)

    def _create_summary_from_dict(self, data: Dict) -> Dict:
        """Создание краткой сводки из словаря данных"""

        summary = {
            "name": data.get("name"),
            "category": data.get("category"),
            "address": data.get("address"),
            "rating": data.get("rating"),
            "reviews_count": data.get("reviews_count"),
            "phone": data.get("phone"),
            "website": data.get("website"),
            "services_count": len(data.get("services", [])),
            "has_social_networks": bool(data.get("social_networks")),
            "scraping_date": data.get("scraping_date"),
        }

        # Добавляем статистику
        if "services" in data:
            services = data["services"]
            if services:
                summary["price_range"] = self._calculate_price_range(services)

        if "reviews" in data:
            reviews = data["reviews"]
            if reviews:
                summary["reviews_summary"] = self._calculate_reviews_summary(reviews)

        return summary

    def _calculate_price_range(self, services: List[Dict]) -> Dict:
        """Вычисление диапазона цен по услугам"""

        prices = []
        for service in services:
            # Пытаемся извлечь числовые цены
            price_text = service.get("price", "")
            if price_text:
                import re

                numbers = re.findall(r"\d+", str(price_text))
                if numbers:
                    prices.extend([int(n) for n in numbers])

        if prices:
            return {
                "min_price": min(prices),
                "max_price": max(prices),
                "avg_price": round(sum(prices) / len(prices)),
            }

        return {"min_price": None, "max_price": None, "avg_price": None}

    def _calculate_reviews_summary(self, reviews: List[Dict]) -> Dict:
        """Вычисление сводки по отзывам"""

        ratings = [r.get("rating") for r in reviews if r.get("rating")]
        ratings = [r for r in ratings if isinstance(r, (int, float)) and 1 <= r <= 5]

        summary = {
            "total_reviews": len(reviews),
            "average_rating": None,
            "rating_distribution": {},
        }

        if ratings:
            summary["average_rating"] = round(sum(ratings) / len(ratings), 1)

            # Распределение оценок
            for i in range(1, 6):
                summary["rating_distribution"][f"{i}_stars"] = ratings.count(i)

        return summary

    def load_json(self, file_path: Union[str, Path]) -> Dict[str, Any]:
        """Загрузка данных из JSON файла"""

        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def validate_json_file(self, file_path: Union[str, Path]) -> Dict[str, Any]:
        """Валидация JSON файла"""

        validation_result = {
            "is_valid": True,
            "errors": [],
            "warnings": [],
            "file_info": {},
        }

        try:
            # Проверяем существование файла
            path = Path(file_path)
            if not path.exists():
                validation_result["is_valid"] = False
                validation_result["errors"].append("Файл не существует")
                return validation_result

            # Информация о файле
            validation_result["file_info"] = {
                "size_bytes": path.stat().st_size,
                "modified_at": datetime.fromtimestamp(path.stat().st_mtime).isoformat(),
            }

            # Загружаем и проверяем JSON
            data = self.load_json(path)

            # Проверяем обязательные поля
            if isinstance(data, dict):
                if "name" not in data:
                    validation_result["errors"].append("Отсутствует поле 'name'")

                # Проверяем метаданные
                if "metadata" in data:
                    metadata = data["metadata"]
                    if "export_timestamp" not in metadata:
                        validation_result["warnings"].append(
                            "Отсутствует timestamp экспорта"
                        )

            elif isinstance(data, list):
                validation_result["file_info"]["records_count"] = len(data)

                # Проверяем первую запись
                if data and isinstance(data[0], dict):
                    if "name" not in data[0]:
                        validation_result["errors"].append(
                            "В записях отсутствует поле 'name'"
                        )

            if validation_result["errors"]:
                validation_result["is_valid"] = False

        except json.JSONDecodeError as e:
            validation_result["is_valid"] = False
            validation_result["errors"].append(f"Ошибка парсинга JSON: {str(e)}")

        except Exception as e:
            validation_result["is_valid"] = False
            validation_result["errors"].append(f"Неожиданная ошибка: {str(e)}")

        return validation_result
