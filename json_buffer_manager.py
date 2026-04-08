import json
import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Tuple
import os

logger = logging.getLogger("JSONBufferManager")


class JSONBufferManager:
    """
    Менеджер буфера на основе JSON с ограничением размера.
    - Приоритет: новые данные → старый буфер
    - При превышении размера - предупреждение и остановка буферизации
    - Автоматическая очистка при синхронизации
    """

    def __init__(
        self,
        buffer_dir: str = "./buffer",
        max_size_mb: int = 500,
        max_records_per_file: int = 10000
    ):
        """
        Args:
            buffer_dir: Директория для буферных файлов
            max_size_mb: Максимальный размер всех буферных файлов в МБ
            max_records_per_file: Максимум записей в одном JSON файле
        """
        self.buffer_dir = Path(buffer_dir)
        self.buffer_dir.mkdir(parents=True, exist_ok=True)

        self.max_size_bytes = max_size_mb * 1024 * 1024
        self.max_records_per_file = max_records_per_file
        self.buffer_enabled = True

        self.events_file = self.buffer_dir / "buffer_events.json"
        self.analogs_file = self.buffer_dir / "buffer_analogs.json"
        self.metadata_file = self.buffer_dir / "buffer_metadata.json"

        self._init_files()
        logger.info(f" [JSON_БУФЕР] Инициализирован с лимитом {max_size_mb}MB")

    def _init_files(self):
        """Инициализация файлов буфера"""
        for file_path in [self.events_file, self.analogs_file]:
            if not file_path.exists():
                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(..., f, ensure_ascii=False)

        # Метаданные
        metadata = {
            "created": datetime.now().isoformat(),
            "last_sync": None,
            "buffer_enabled": True,
            "total_events": 0,
            "total_analogs": 0
        }
        if not self.metadata_file.exists():
            with open(self.metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)

    def _get_total_buffer_size(self) -> int:
        """Получить общий размер буфера в байтах"""
        total_size = 0
        for file_path in [self.events_file, self.analogs_file]:
            if file_path.exists():
                total_size += file_path.stat().st_size
        return total_size

    def _check_buffer_limit(self) -> Tuple[bool, Dict]:
        """Проверить превышение лимита буфера"""
        current_size = self._get_total_buffer_size()
        stats = {
            "current_size_mb": round(current_size / 1024 / 1024, 2),
            "max_size_mb": round(self.max_size_bytes / 1024 / 1024, 2),
            "usage_percent": round((current_size / self.max_size_bytes) * 100, 1)
        }

        if current_size > self.max_size_bytes:
            logger.critical(
                f" [JSON_БУФЕР] ❌ ПРЕВЫШЕН ЛИМИТ! "
                f"{stats['current_size_mb']}MB / {stats['max_size_mb']}MB "
                f"({stats['usage_percent']}%)"
            )
            self.buffer_enabled = False
            self._update_metadata({"buffer_enabled": False})
            return False, stats

        # Предупреждение при 80% заполнения
        if current_size > (self.max_size_bytes * 0.8):
            logger.warning(
                f" [JSON_БУФЕР] ⚠️ ВНИМАНИЕ: буфер на {stats['usage_percent']}% "
                f"({stats['current_size_mb']}MB / {stats['max_size_mb']}MB)"
            )

        return True, stats

    def add_event(self, node_id: str, value: str | None, is_good: bool, status: str) -> bool:
        """Добавить событие в буфер"""
        if not self.buffer_enabled:
            logger.error(
                " [JSON_БУФЕР] Буфер отключен - событие не сохранено!")
            return False

        try:
            allowed, stats = self._check_buffer_limit()
            if not allowed:
                return False

            with open(self.events_file, 'r') as f:
                data = json.load(f)

            # Добавляем новую запись
            data["records"].append({
                "node_id": node_id,
                "value": value,  # может быть None
                "timestamp": datetime.now().isoformat(),
                "synced": 0,
                "is_good": bool(is_good),
                "status": str(status),
            })

            # Если превышена вместимость файла - ротация
            if len(data["records"]) > self.max_records_per_file:
                self._rotate_file(self.events_file, "events")
                data["records"] = []

            with open(self.events_file, 'w') as f:
                json.dump(data, f)

            return True
        except Exception as e:
            logger.error(f" [JSON_БУФЕР] Ошибка добавления события: {e}")
            return False

    def add_analog(self, node_id: str, value: float, is_good: bool, status: str) -> bool:
        """Добавить аналоговое значение в буфер"""
        if not self.buffer_enabled:
            logger.error(" [JSON_БУФЕР] Буфер отключен - аналог не сохранен!")
            return False

        try:
            allowed, stats = self._check_buffer_limit()
            if not allowed:
                return False

            with open(self.analogs_file, 'r') as f:
                data = json.load(f)

            data["records"].append({
                "node_id": node_id,
                "value": None if value is None else float(value),
                "timestamp": datetime.now().isoformat(),
                "synced": 0,
                "is_good": bool(is_good),
                "status": str(status),
            })

            if len(data["records"]) > self.max_records_per_file:
                self._rotate_file(self.analogs_file, "analogs")
                data["records"] = []

            with open(self.analogs_file, 'w') as f:
                json.dump(data, f)

            return True
        except Exception as e:
            logger.error(f" [JSON_БУФЕР] Ошибка добавления аналога: {e}")
            return False

    def _rotate_file(self, file_path: Path, file_type: str):
        """Ротация файла при превышении количества записей"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            rotated_path = file_path.parent / \
                f"buffer_{file_type}_{timestamp}.json"

            with open(file_path, 'r') as f:
                data = json.load(f)

            with open(rotated_path, 'w') as f:
                json.dump(data, f)

            # Очищаем основной файл
            with open(file_path, 'w') as f:
                json.dump({"records": [], "synced": 0}, f)

            logger.info(f" [JSON_БУФЕР] Файл ротирован: {rotated_path.name}")
        except Exception as e:
            logger.error(f" [JSON_БУФЕР] Ошибка ротации файла: {e}")

    def get_unsync_data(self, limit: int = 1000) -> Dict[str, List]:
        """
        Получить несинхронизированные данные.
        ПРИОРИТЕТ: новые данные из основного файла → старые из ротированных
        """
        result = {"events": [], "analogs": []}

        try:
            # Сначала берем НОВЫЕ данные из основного файла события
            with open(self.events_file, 'r') as f:
                data = json.load(f)

            unsync_events = [r for r in data["records"] if r["synced"] == 0]
            result["events"] = unsync_events[:limit]

            # Потом СТАРЫЕ из ротированных файлов (если не хватает лимита)
            if len(result["events"]) < limit:
                rotated_events = sorted(
                    self.buffer_dir.glob("buffer_events_*.json"),
                    reverse=True  # новые сначала
                )
                for rotated_file in rotated_events:
                    if len(result["events"]) >= limit:
                        break
                    try:
                        with open(rotated_file, 'r') as f:
                            data = json.load(f)
                        unsync = [r for r in data["records"]
                                  if r["synced"] == 0]
                        result["events"].extend(
                            unsync[:limit - len(result["events"])])
                    except Exception as e:
                        logger.error(
                            f" [JSON_БУФЕР] Ошибка чтения {rotated_file.name}: {e}")

            # Аналогично для аналогов
            with open(self.analogs_file, 'r') as f:
                data = json.load(f)

            unsync_analogs = [r for r in data["records"] if r["synced"] == 0]
            result["analogs"] = unsync_analogs[:limit]

            if len(result["analogs"]) < limit:
                rotated_analogs = sorted(
                    self.buffer_dir.glob("buffer_analogs_*.json"),
                    reverse=True
                )
                for rotated_file in rotated_analogs:
                    if len(result["analogs"]) >= limit:
                        break
                    try:
                        with open(rotated_file, 'r') as f:
                            data = json.load(f)
                        unsync = [r for r in data["records"]
                                  if r["synced"] == 0]
                        result["analogs"].extend(
                            unsync[:limit - len(result["analogs"])])
                    except Exception as e:
                        logger.error(
                            f" [JSON_БУФЕР] Ошибка чтения {rotated_file.name}: {e}")

            return result
        except Exception as e:
            logger.error(f" [JSON_БУФЕР] Ошибка получения данных: {e}")
            return result

    def mark_synced(self, event_ids: List[str] = None, analog_ids: List[str] = None) -> bool:
        """
        Отметить данные как синхронизированные.
        Использует timestamps для идентификации.
        """
        try:
            # События
            if event_ids:
                for file_path in [self.events_file] + list(self.buffer_dir.glob("buffer_events_*.json")):
                    if not file_path.exists():
                        continue

                    with open(file_path, 'r') as f:
                        data = json.load(f)

                    for record in data["records"]:
                        if record["timestamp"] in event_ids:
                            record["synced"] = 1

                    with open(file_path, 'w') as f:
                        json.dump(data, f)

            # Аналоги
            if analog_ids:
                for file_path in [self.analogs_file] + list(self.buffer_dir.glob("buffer_analogs_*.json")):
                    if not file_path.exists():
                        continue

                    with open(file_path, 'r') as f:
                        data = json.load(f)

                    for record in data["records"]:
                        if record["timestamp"] in analog_ids:
                            record["synced"] = 1

                    with open(file_path, 'w') as f:
                        json.dump(data, f)

            return True
        except Exception as e:
            logger.error(
                f" [JSON_БУФЕР] Ошибка пометки синхронизированных: {e}")
            return False

    def cleanup_synced(self) -> int:
        """Удалить все синхронизированные данные из буфера"""
        try:
            deleted_records = 0

            # Очистка основных файлов
            for file_path in [self.events_file, self.analogs_file]:
                with open(file_path, 'r') as f:
                    data = json.load(f)

                original_count = len(data["records"])
                data["records"] = [r for r in data["records"] if r["synced"] == 0]
                deleted_records += original_count - len(data["records"])

                with open(file_path, 'w') as f:
                    json.dump(data, f)

            # Удаление ротированных файлов полностью (они уже синхронизированы)
            for rotated_file in self.buffer_dir.glob("buffer_*_*.json"):
                rotated_file.unlink()
                deleted_records += 1

            # Re-enable buffer if it was disabled
            if not self.buffer_enabled:
                self.buffer_enabled = True
                self._update_metadata({"buffer_enabled": True})
                logger.info(
                    " [JSON_БУФЕР] ✓ Буфер повторно включен после очистки")

            logger.info(
                f" [JSON_БУФЕР] Удалено {deleted_records} синхронизированных записей")
            return deleted_records
        except Exception as e:
            logger.error(f" [JSON_БУФЕР] Ошибка очистки: {e}")
            return 0

    def get_stats(self) -> Dict:
        """Получить статистику буфера"""
        try:
            current_size = self._get_total_buffer_size()

            with open(self.events_file, 'r') as f:
                events_data = json.load(f)
            with open(self.analogs_file, 'r') as f:
                analogs_data = json.load(f)

            events_count = len(events_data["records"])
            analogs_count = len(analogs_data["records"])
            unsync_events = len(
                [r for r in events_data["records"] if r["synced"] == 0])
            unsync_analogs = len(
                [r for r in analogs_data["records"] if r["synced"] == 0])

            # Считаем ротированные файлы
            rotated_files = len(list(self.buffer_dir.glob("buffer_*_*.json")))

            return {
                "buffer_enabled": self.buffer_enabled,
                "current_size_mb": round(current_size / 1024 / 1024, 2),
                "max_size_mb": round(self.max_size_bytes / 1024 / 1024, 2),
                "usage_percent": round((current_size / self.max_size_bytes) * 100, 1),
                "events_total": events_count,
                "events_unsynced": unsync_events,
                "analogs_total": analogs_count,
                "analogs_unsynced": unsync_analogs,
                "rotated_files": rotated_files
            }
        except Exception as e:
            logger.error(f" [JSON_БУФЕР] Ошибка получения статистики: {e}")
            return {}

    def _update_metadata(self, updates: Dict):
        """Обновить метаданные"""
        try:
            with open(self.metadata_file, 'r') as f:
                metadata = json.load(f)

            metadata.update(updates)
            metadata["last_update"] = datetime.now().isoformat()

            with open(self.metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)
        except Exception as e:
            logger.error(f" [JSON_БУФЕР] Ошибка обновления метаданных: {e}")
