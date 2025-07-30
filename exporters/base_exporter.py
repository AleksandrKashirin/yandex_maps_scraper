"""
Базовые классы для экспортеров данных
"""

from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Union

from pydantic import BaseModel


class ExportMetadata(BaseModel):
    """Метаданные экспорта"""

    export_timestamp: datetime
    exporter_version: str = "1.0"
    source_url: str = None
    records_count: int = 0
    export_format: str
    file_size_bytes: int = None
    checksum: str = None
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
            exporter_version="1.0",
            source_url=source_url,
            records_count=records_count,
            export_format=export_format,
            file_size_bytes=file_size,
            export_options=kwargs,
        )

        self.metadata = metadata
        return metadata
