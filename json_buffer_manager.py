import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional

logger = logging.getLogger("JSONBufferManager")


class JSONBufferManager:
    """
    JSON буфер с лимитом по размеру.
    Основные файлы:
      - events.json
      - analogs.json
      - measurements.json  (записи для Sp_msr_value_send)
      - metadata.json
    """

    def __init__(self, buffer_dir: str = "./buffer", max_size_mb: int = 500, max_records_per_file: int = 10000):
        self.buffer_dir = Path(buffer_dir)
        self.max_records_per_file = int(max_records_per_file)

        self.max_size_bytes = int(max_size_mb) * 1024 * 1024
        self.buffer_enabled = True

        self.buffer_dir.mkdir(parents=True, exist_ok=True)

        self.events_file = self.buffer_dir / "events.json"
        self.analogs_file = self.buffer_dir / "analogs.json"
        self.measurements_file = self.buffer_dir / "measurements.json"
        self.metadata_file = self.buffer_dir / "metadata.json"

        self._init_files()

    def _init_files(self) -> None:
        """Создаёт файлы буфера, если их нет."""
        for file_path in (self.events_file, self.analogs_file, self.measurements_file):
            if not file_path.exists():
                file_path.write_text(json.dumps(
                    {"records": []}, ensure_ascii=False), encoding="utf-8")

        if not self.metadata_file.exists():
            metadata = {
                "created": datetime.now().isoformat(),
                "last_sync": None,
                "buffer_enabled": True,
                "total_events": 0,
                "total_analogs": 0,
                "last_update": datetime.now().isoformat(),
            }
            self.metadata_file.write_text(json.dumps(
                metadata, ensure_ascii=False, indent=2), encoding="utf-8")

    def _get_total_buffer_size(self) -> int:
        total_size = 0
        for file_path in (self.events_file, self.analogs_file, self.measurements_file):
            if file_path.exists():
                total_size += file_path.stat().st_size
        return total_size

    def _check_buffer_limit(self) -> Tuple[bool, Dict]:
        current_size = self._get_total_buffer_size()
        stats = {
            "current_size_mb": round(current_size / 1024 / 1024, 2),
            "max_size_mb": round(self.max_size_bytes / 1024 / 1024, 2),
            "usage_percent": round((current_size / self.max_size_bytes) * 100, 1) if self.max_size_bytes else 0,
        }

        if current_size > self.max_size_bytes:
            logger.critical(
                f"[JSON_BUFFER] LIMIT EXCEEDED {stats['current_size_mb']}MB / {stats['max_size_mb']}MB ({stats['usage_percent']}%)"
            )
            self.buffer_enabled = False
            self._update_metadata({"buffer_enabled": False})
            return False, stats

        if current_size > (self.max_size_bytes * 0.8):
            logger.warning(
                f"[JSON_BUFFER] WARNING {stats['usage_percent']}% ({stats['current_size_mb']}MB / {stats['max_size_mb']}MB)"
            )

        return True, stats

    def start_buffering_session(self) -> None:
        """
    Вызывается 1 раз при переходе в режим буферизации (потеря SQL).
    Обнуляет активные файлы, чтобы сессия начиналась с 0.
        """
        try:
            for file_path in (self.events_file, self.analogs_file, self.measurements_file):
                file_path.write_text(
                    json.dumps({"records": []}, ensure_ascii=False),
                    encoding="utf-8",
                )
        except Exception as e:
            logger.error(
                f"[JSON_BUFFER] start_buffering_session error: {e}")

    def _suffix_from_last_record(self, records: List[Dict]) -> str:
        """
        YYYYMMDD_HHMMSS по timestamp п��следней записи в records.
        Если timestamp битый — текущее время.
        """
        try:
            ts = records[-1].get("timestamp")
            dt = datetime.fromisoformat(ts)
        except Exception:
            dt = datetime.now()
        return dt.strftime("%Y%m%d_%H%M%S")

    def close_buffering_session(self) -> Dict[str, str]:
        """
        Вызывается после успешной синхронизации (SQL восстановился и буфер выгружен).
        Сохраняет events.json/analogs.json/measurements.json в archive_<kind>_<LAST_TS>.json,
        затем обнуляет активные файлы.
        Возвращает имена созданных архивов.
        """
        created: Dict[str, str] = {}
        try:
            for kind, active_path in (
                ("events", self.events_file),
                ("analogs", self.analogs_file),
                ("measurements", self.measurements_file),
            ):
                if not active_path.exists():
                    continue

                data = json.loads(active_path.read_text(encoding="utf-8"))
                records = data.get("records", [])
                if not records:
                    # гарантируем, что активный файл пустой
                    active_path.write_text(
                        json.dumps({"records": []}, ensure_ascii=False),
                        encoding="utf-8",
                    )
                    continue

                suffix = self._suffix_from_last_record(records)
                archived_path = self.buffer_dir / \
                    f"archive_{kind}_{suffix}.json"

                archived_path.write_text(
                    json.dumps({"records": records}, ensure_ascii=False),
                    encoding="utf-8",
                )
                created[kind] = archived_path.name

                # обнуляем активный
                active_path.write_text(
                    json.dumps({"records": []}, ensure_ascii=False),
                    encoding="utf-8",
                )

        except Exception as e:
            logger.error(f"[JSON_BUFFER] close_buffering_session error: {e}")

        return created

    def add_event(self, node_id: str, value: Optional[str], is_good: bool, status: str) -> bool:
        if not self.buffer_enabled:
            return False

        allowed, _ = self._check_buffer_limit()
        if not allowed:
            return False

        try:
            data = json.loads(self.events_file.read_text(encoding="utf-8"))
            data["records"].append(
                {
                    "node_id": node_id,
                    "value": value,
                    "timestamp": datetime.now().isoformat(),
                    "synced": 0,
                    "is_good": bool(is_good),
                    "status": ("" if status is None else str(status))[:200],
                }
            )
            # простая ротация: если слишком много — обнуляем (можно улучшить позже)
            if len(data["records"]) > self.max_records_per_file:
                data["records"] = data["records"][-self.max_records_per_file:]

            self.events_file.write_text(json.dumps(
                data, ensure_ascii=False), encoding="utf-8")
            return True
        except Exception as e:
            logger.error(f"[JSON_BUFFER] add_event error: {e}")
            return False

    def add_analog(self, node_id: str, value: Optional[float], is_good: bool, status: str) -> bool:
        if not self.buffer_enabled:
            return False

        allowed, _ = self._check_buffer_limit()
        if not allowed:
            return False

        try:
            data = json.loads(self.analogs_file.read_text(encoding="utf-8"))
            data["records"].append(
                {
                    "node_id": node_id,
                    "value": None if value is None else float(value),
                    "timestamp": datetime.now().isoformat(),
                    "synced": 0,
                    "is_good": bool(is_good),
                    "status": ("" if status is None else str(status))[:200],
                }
            )
            if len(data["records"]) > self.max_records_per_file:
                data["records"] = data["records"][-self.max_records_per_file:]

            self.analogs_file.write_text(json.dumps(
                data, ensure_ascii=False), encoding="utf-8")
            return True
        except Exception as e:
            logger.error(f"[JSON_BUFFER] add_analog error: {e}")
            return False

    def add_measurement(
        self,
        ffc_id: int,
        msd_id: int,
        value: float,
        msr_time: Optional[str] = None,
    ) -> bool:
        """
        Буферизация одного измерения для последующей отправки через Sp_msr_value_send.

        Args:
            ffc_id:   id1 из конфига OPC тега (p_ffc_id).
            msd_id:   id2 из конфига OPC тега (p_msd_id).
            value:    значение измерения (float).
            msr_time: время измерения в виде ISO-8601 строки (локальное naive).
                      Если None — используется текущее время.
        """
        if not self.buffer_enabled:
            return False

        allowed, _ = self._check_buffer_limit()
        if not allowed:
            return False

        try:
            data = json.loads(self.measurements_file.read_text(encoding="utf-8"))
            now_iso = datetime.now().isoformat()
            data["records"].append(
                {
                    "ffc_id": int(ffc_id),
                    "msd_id": int(msd_id),
                    "value": float(value),
                    "msr_time": msr_time if msr_time is not None else now_iso,
                    "timestamp": now_iso,
                    "synced": 0,
                }
            )
            if len(data["records"]) > self.max_records_per_file:
                data["records"] = data["records"][-self.max_records_per_file:]

            self.measurements_file.write_text(json.dumps(
                data, ensure_ascii=False), encoding="utf-8")
            return True
        except Exception as e:
            logger.error(f"[JSON_BUFFER] add_measurement error: {e}")
            return False

    def get_unsync_measurements(self, limit: int = 1000) -> List[Dict]:
        """
        Возвращает не более *limit* несинхронизированных записей измерений.
        """
        try:
            data = json.loads(self.measurements_file.read_text(encoding="utf-8"))
            records = data.get("records", [])
            return [r for r in records if r.get("synced", 0) == 0][:limit]
        except Exception as e:
            logger.error(f"[JSON_BUFFER] get_unsync_measurements error: {e}")
            return []

    def mark_measurements_synced(self, timestamps: List[str]) -> int:
        """
        Помечает записи измерений как synced=1 по их timestamp.
        Возвращает количество помеченных записей.
        """
        if not timestamps or not self.measurements_file.exists():
            return 0
        try:
            data = json.loads(self.measurements_file.read_text(encoding="utf-8"))
            records = data.get("records", [])
            ids_set = set(timestamps)
            marked = 0
            for r in records:
                if r.get("synced", 0) == 0 and r.get("timestamp") in ids_set:
                    r["synced"] = 1
                    marked += 1
            if marked:
                self.measurements_file.write_text(
                    json.dumps(data, ensure_ascii=False), encoding="utf-8"
                )
                try:
                    self._update_metadata({"last_sync": datetime.now().isoformat()})
                except Exception:
                    pass
            return marked
        except Exception as e:
            logger.error(f"[JSON_BUFFER] mark_measurements_synced error: {e}")
            return 0

    def get_unsync_data(self, limit: int = 1000) -> Dict[str, List]:
        result = {"events": [], "analogs": []}
        try:
            events = json.loads(self.events_file.read_text(
                encoding="utf-8")).get("records", [])
            analogs = json.loads(self.analogs_file.read_text(
                encoding="utf-8")).get("records", [])

            result["events"] = [
                r for r in events if r.get("synced", 0) == 0][:limit]
            result["analogs"] = [
                r for r in analogs if r.get("synced", 0) == 0][:limit]
            return result
        except Exception as e:
            logger.error(f"[JSON_BUFFER] get_unsync_data error: {e}")
            return result

    def mark_synced(
        self,
        event_ids: Optional[List[str]] = None,
        analog_ids: Optional[List[str]] = None,
    ) -> int:
        """
        Помечает записи как synced=1 по их timestamp.
        Возвращает количество помеченных записей.
        """
        event_ids = event_ids or []
        analog_ids = analog_ids or []

        marked = 0

        def _mark_in_file(path: Path, ids: List[str]) -> int:
            if not ids or not path.exists():
                return 0
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                records = data.get("records", [])
                ids_set = set(ids)

                local_marked = 0
                for r in records:
                    if r.get("synced", 0) == 0 and r.get("timestamp") in ids_set:
                        r["synced"] = 1
                        local_marked += 1

                if local_marked:
                    path.write_text(json.dumps(
                        data, ensure_ascii=False), encoding="utf-8")

                return local_marked
            except Exception as e:
                logger.error(
                    f"[JSON_BUFFER] mark_synced error in {path.name}: {e}")
                return 0

        marked += _mark_in_file(self.events_file, event_ids)
        marked += _mark_in_file(self.analogs_file, analog_ids)

        if marked:
            try:
                self._update_metadata(
                    {"last_sync": datetime.now().isoformat()})
            except Exception:
                pass

        return marked

    def cleanup_synced(self) -> int:
        """
        Удаляет из активных файлов записи с synced=1.
        Возвращает сколько записей удалено.
        """
        deleted = 0

        def _cleanup(path: Path) -> int:
            if not path.exists():
                return 0
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                records = data.get("records", [])
                before = len(records)

                records = [r for r in records if r.get("synced", 0) == 0]
                after = len(records)

                if after != before:
                    data["records"] = records
                    path.write_text(
                        json.dumps(data, ensure_ascii=False),
                        encoding="utf-8",
                    )

                return before - after
            except Exception as e:
                logger.error(
                    f"[JSON_BUFFER] cleanup_synced error in {path.name}: {e}")
                return 0

        deleted += _cleanup(self.events_file)
        deleted += _cleanup(self.analogs_file)
        deleted += _cleanup(self.measurements_file)

        return deleted

    def get_stats(self) -> Dict:
        """Получить статистику буфера (без заглушек '...')."""
        try:
            current_size = self._get_total_buffer_size()

            def _count_unsynced(path: Path) -> tuple[int, int]:
                if not path.exists():
                    return 0, 0
                data = json.loads(path.read_text(encoding="utf-8"))
                records = data.get("records", [])
                total = len(records)
                unsynced = sum(1 for r in records if r.get("synced", 0) == 0)
                return total, unsynced

            events_total, events_unsynced = _count_unsynced(self.events_file)
            analogs_total, analogs_unsynced = _count_unsynced(
                self.analogs_file)
            measurements_total, measurements_unsynced = _count_unsynced(
                self.measurements_file)

            rotated_files = len(list(self.buffer_dir.glob("buffer_*_*.json")))

            return {
                "buffer_enabled": bool(getattr(self, "buffer_enabled", True)),
                "current_size_mb": round(current_size / 1024 / 1024, 2),
                "max_size_mb": round(self.max_size_bytes / 1024 / 1024, 2),
                "usage_percent": round((current_size / self.max_size_bytes) * 100, 1) if self.max_size_bytes else 0,
                "events_total": events_total,
                "events_unsynced": events_unsynced,
                "analogs_total": analogs_total,
                "analogs_unsynced": analogs_unsynced,
                "measurements_total": measurements_total,
                "measurements_unsynced": measurements_unsynced,
                "rotated_files": rotated_files,
            }
        except Exception as e:
            logger.error(f"[JSON_BUFFER] get_stats error: {e}")
            return {
                "buffer_enabled": False,
                "current_size_mb": 0.0,
                "max_size_mb": 0.0,
                "usage_percent": 0.0,
                "events_total": 0,
                "events_unsynced": 0,
                "analogs_total": 0,
                "analogs_unsynced": 0,
                "measurements_total": 0,
                "measurements_unsynced": 0,
                "rotated_files": 0,
            }

    def _update_metadata(self, updates: Dict) -> None:
        try:
            metadata = json.loads(
                self.metadata_file.read_text(encoding="utf-8"))
            metadata.update(updates)
            metadata["last_update"] = datetime.now().isoformat()
            self.metadata_file.write_text(json.dumps(
                metadata, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as e:
            logger.error(f"[JSON_BUFFER] metadata update error: {e}")
