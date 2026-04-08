import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
import pyodbc
from asyncua import Client, ua
from opc_tags_list import (
    EVENT_TAGS, ANALOG_TAGS, OPC_URL, OPC_USER, OPC_PASS, DB_DSN,
    ANALOG_SAVE_INTERVAL, DB_BATCH_SIZE, JSON_BUFFER_MAX_MB, JSON_BUFFER_MAX_RECORDS,
    OPC_CONN_NODEID
)
from json_buffer_manager import JSONBufferManager

# === ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ===
last_opc_connection_state = None        # type: bool | None
last_opc_disconnect_reason = None       # type: str | None
latest_analog_values = {}
event_queue = asyncio.Queue()
analog_queue = asyncio.Queue()
buffer = JSONBufferManager(
    buffer_dir="./buffer",
    max_size_mb=JSON_BUFFER_MAX_MB,
    max_records_per_file=JSON_BUFFER_MAX_RECORDS
)
opc_connected = False
# node_id -> last_good_value
last_good_analog_values = {}
# Пул потоков для БД операций
db_executor = ThreadPoolExecutor(max_workers=2)

# === ЛОГИРОВАНИЕ ===
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("OPC_App")
logger.setLevel(logging.INFO)

# === ПОДАВЛЯЕМ ШУМ ОТ БИБЛИОТЕК ===
logging.getLogger("asyncua").setLevel(logging.ERROR)        # было WARNING
logging.getLogger("uaprotocol").setLevel(logging.ERROR)     # было WARNING
logging.getLogger("asyncio").setLevel(logging.ERROR)
logging.getLogger("pyodbc").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("cryptography").setLevel(logging.ERROR)

# Подавляем debug сообщения от всех остальных библиотек
for logger_name in logging.root.manager.loggerDict:
    if logger_name not in ["OPC_App"]:
        logging.getLogger(logger_name).setLevel(logging.CRITICAL)
# === КЛАССЫ И ФУНКЦИИ ===


class SubscriptionHandler:
    async def datachange_notification(self, node, val, data):
        node_id = str(node)
        short_name = node_id.split('.')[-1]

        is_good, status = extract_quality(data)

        if node_id in EVENT_TAGS:
            out_val = str(val) if is_good else None
            await event_queue.put((node_id, out_val, is_good, status))
            logger.info(
                f" [СОБЫТИЕ] {short_name:.<25} -> {out_val} | {status}")

        elif node_id in ANALOG_TAGS:
            if is_good:
                last_good_analog_values[node_id] = val
                latest_analog_values[node_id] = (val, True, status)
            else:
                # запоминаем факт bad, но последнее good не затираем
                latest_analog_values[node_id] = (None, False, status)


async def emit_opc_connection_state(connected: bool, reason: str = ""):
    """
    Пишем sys.opc.connection только при изменении состояния.
    Дополнительно: если disconnect повторяется с той же причиной — не пишем.
    """
    global last_opc_connection_state, last_opc_disconnect_reason

    # connected/disconnected не изменился — не спамим
    if last_opc_connection_state is not None and connected == last_opc_connection_state:
        # но если мы всё ещё disconnected и причина изменилась — можно записать ОДИН раз
        if not connected:
            new_reason = reason.strip()
            if new_reason and new_reason != (last_opc_disconnect_reason or ""):
                last_opc_disconnect_reason = new_reason
            else:
                return
        else:
            return

    last_opc_connection_state = connected

    if connected:
        last_opc_disconnect_reason = None
        val = "1"
        is_good = True
        status = "ClientConnected"
    else:
        val = None  # по вашему правилу для bad событий пишем NULL
        is_good = False
        new_reason = reason.strip()
        last_opc_disconnect_reason = new_reason or last_opc_disconnect_reason
        status = f"ClientDisconnected: {last_opc_disconnect_reason or 'Unknown'}"

    await event_queue.put((OPC_CONN_NODEID, val, is_good, status))


def extract_quality(data) -> tuple[bool, str]:
    """
    Возвращает (is_good, status_str) из DataValue.StatusCode.
    """
    try:
        dv = data.monitored_item.Value
        sc = dv.StatusCode
        return sc.is_good(), str(sc)
    except Exception as e:
        return False, f"UnknownStatus({e})"


def safe_float(v):
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _insert_to_db(table_name, data_to_insert, is_float=False):
    """Вспомогательная функция для записи в БД (работает в потоке)"""
    try:
        conn = pyodbc.connect(DB_DSN, timeout=5)
        cursor = conn.cursor()

        for nid, val, is_good, status in data_to_insert:
            if is_float:
                db_val = None if val is None else float(val)
            else:
                db_val = None if val is None else str(val)

            cursor.execute(
                f"INSERT INTO {table_name} (NodeId, [Value], [Timestamp], QualityIsGood, QualityStatus) "
                f"VALUES (?, ?, GETDATE(), ?, ?)",
                (str(nid), db_val, 1 if is_good else 0, str(status))
            )

        conn.commit()
        conn.close()
        return True, None, len(data_to_insert)
    except Exception as e:
        return False, str(e), 0


async def db_writer(queue, table_name, is_float=False):
    """Писатель в БД с поддержкой JSON буферизации при отказе"""
    global buffer

    loop = asyncio.get_event_loop()
    consecutive_errors = 0
    max_consecutive_errors = 3
    last_error_logged = None
    db_connection_lost = False  # ← НОВОЕ: флаг потери связи

    while True:
        batch = []
        try:
            item = await queue.get()
            batch.append(item)

            while len(batch) < DB_BATCH_SIZE:
                try:
                    item = queue.get_nowait()
                    batch.append(item)
                except asyncio.QueueEmpty:
                    break
        except Exception as e:
            logger.error(f"Ошибка получения данных из очереди: {e}")
            continue

        if batch:
            success = True
            try:
                if is_float:
                    data_to_insert = [(str(nid), safe_float(val), bool(
                        is_good), status) for nid, val, is_good, status in batch]
                else:
                    data_to_insert = [(str(nid), None if val is None else str(val), bool(
                        is_good), status) for nid, val, is_good, status in batch]

                success, error, inserted = await loop.run_in_executor(
                    db_executor,
                    _insert_to_db,
                    table_name,
                    data_to_insert,
                    is_float
                )

                if success:
                    consecutive_errors = 0
                    last_error_logged = None

                    # ← НОВОЕ: если связь была потеряна, сообщаем о восстановлении
                    if db_connection_lost:
                        logger.info(f"✅ Связь с SQL Server восстановлена")
                        db_connection_lost = False

                    logger.info(f" [БАЗА] {table_name}: +{len(batch)} строк ✓")
                else:
                    consecutive_errors += 1

                    # ← ИЗМЕНЕНО: логируем потерю связи только один раз
                    if consecutive_errors == 1:
                        logger.error(
                            f"❌ Связь с SQL Server потеряна - буферизация активирована")
                        db_connection_lost = True
                        last_error_logged = error

                    # Буферизация в JSON при ошибке БД
                    for nid, val, is_good, status in batch:
                        if is_float:
                            buffer.add_analog(
                                str(nid), safe_float(val), is_good, status)
                        else:
                            buffer.add_event(
                                str(nid), None if val is None else str(val), is_good, status)

            except Exception as e:
                logger.error(f"❌ Критическая ошибка в db_writer: {e}")
                for nid, val in batch:
                    if is_float:
                        buffer.add_analog(nid, val)
                    else:
                        buffer.add_event(nid, val)
            finally:
                for _ in range(len(batch)):
                    queue.task_done()

                if not success:
                    await asyncio.sleep(1)


async def periodic_analog_recorder():
    while True:
        await asyncio.sleep(ANALOG_SAVE_INTERVAL)

        # Всегда пробегаем по ANALOG_TAGS, чтобы писать ровные срезы
        for node_id in ANALOG_TAGS:
            if opc_connected:
                # если есть текущее (val, is_good, status) — используем его,
                # если нет — fallback на last_good
                triple = latest_analog_values.get(node_id)
                if triple and triple[1] is True:
                    val, is_good, status = triple
                    await analog_queue.put((node_id, val, True, status))
                else:
                    val = last_good_analog_values.get(node_id)
                    await analog_queue.put((node_id, val, False, "NoGoodYetOrBad"))
            else:
                val = last_good_analog_values.get(node_id)
                await analog_queue.put((node_id, val, False, "OPC Offline (stale)"))

        logger.info(
            f" [АНАЛОГ] Срез отправлен в очередь: {len(ANALOG_TAGS)} тегов")


async def sync_buffer_to_db():
    """Синхронизация буферизованных данных при восстановлении связи."""
    global buffer
    loop = asyncio.get_event_loop()
    sync_attempts = 0
    last_error_logged = None
    sync_in_progress = False  # ← НОВОЕ: флаг синхронизации

    while True:
        await asyncio.sleep(20)

        stats = buffer.get_stats()
        unsync_count = stats.get("events_unsynced", 0) + \
            stats.get("analogs_unsynced", 0)

        if unsync_count == 0:
            sync_attempts = 0
            last_error_logged = None
            sync_in_progress = False
            continue

        sync_attempts += 1

        # ← ИЗМЕНЕНО: показываем сообщение о начале синхронизации
        if sync_attempts == 1:
            logger.info(
                f" [СИНХРО] Начало синхронизации буфера: {unsync_count} записей")
            sync_in_progress = True

        try:
            conn = pyodbc.connect(DB_DSN, timeout=5)
            cursor = conn.cursor()

            buffer_data = buffer.get_unsync_data(limit=2000)
            synced_events = []
            synced_analogs = []

            # Синхронизация событий
            if buffer_data["events"]:
                try:
                    for record in buffer_data["events"]:
                        cursor.execute(
                            "INSERT INTO dbo.OpcEvents (NodeId, [Value], [Timestamp], QualityIsGood, QualityStatus) "
                            "VALUES (?, ?, GETDATE(), ?, ?)",
                            (record["node_id"], record.get("value"), 1 if record.get(
                                "is_good") else 0, record.get("status"))
                        )
                        synced_events.append(record["timestamp"])
                    conn.commit()
                    logger.info(
                        f" [СИНХРО] ✓ События: +{len(buffer_data['events'])} записей")
                except Exception as e:
                    conn.rollback()

            # Синхронизация аналогов
            if buffer_data["analogs"]:
                try:
                    for record in buffer_data["analogs"]:
                        cursor.execute(
                            "INSERT INTO dbo.OpcAnalog (NodeId, [Value], [Timestamp], QualityIsGood, QualityStatus) "
                            "VALUES (?, ?, GETDATE(), ?, ?)",
                            (record["node_id"], record.get("value"), 1 if record.get(
                                "is_good") else 0, record.get("status"))
                        )
                        synced_analogs.append(record["timestamp"])
                    conn.commit()
                    logger.info(
                        f" [СИНХРО] ✓ Аналоги: +{len(buffer_data['analogs'])} записей")
                except Exception as e:
                    conn.rollback()

            conn.close()

            if synced_events or synced_analogs:
                buffer.mark_synced(
                    event_ids=synced_events,
                    analog_ids=synced_analogs
                )

                deleted = buffer.cleanup_synced()
                if deleted > 0:
                    sync_attempts = 0
                    last_error_logged = None

                    # ← НОВОЕ: сообщение об успешной синхронизации
                    if sync_in_progress:
                        logger.info(
                            f"✅ Связь с SQL Server восстановлена - синхронизация завершена")
                        sync_in_progress = False

                    logger.info(
                        f" [JSON_БУФЕР] ✓ Синхронизировано и очищено: -{deleted} записей")

                    new_stats = buffer.get_stats()
                    logger.info(
                        f" [JSON_БУФЕР] Статус: "
                        f"{new_stats['usage_percent']}% "
                        f"({new_stats['current_size_mb']}MB / {new_stats['max_size_mb']}MB)"
                    )

        except Exception as e:
            if last_error_logged != str(e):
                if sync_attempts <= 2:
                    logger.warning(
                        f" [СИНХРО] БД еще недоступна (попытка #{sync_attempts})")
                elif sync_attempts == 3:
                    logger.error(
                        f" [СИНХРО] БД остается недоступной - данные буферизуются")
                last_error_logged = str(e)


async def monitor_buffer():
    """Мониторинг состояния буфера каждые 60 сек"""
    global buffer

    while True:
        await asyncio.sleep(60)
        stats = buffer.get_stats()

        status = "✓" if stats["buffer_enabled"] else "❌ ОТКЛЮЧЕН"
        logger.info(
            f" [JSON_БУФЕР] {status} | "
            f"Размер: {stats['current_size_mb']}MB/{stats['max_size_mb']}MB ({stats['usage_percent']}%) | "
            f"События: {stats['events_unsynced']}/{stats['events_total']} | "
            f"Аналоги: {stats['analogs_unsynced']}/{stats['analogs_total']} | "
            f"Ротир. файлов: {stats['rotated_files']}"
        )


async def performance_monitor():
    """Мониторинг производительности системы"""
    while True:
        await asyncio.sleep(60)

        event_queue_size = event_queue.qsize()
        analog_queue_size = analog_queue.qsize()
        buffer_stats = buffer.get_stats()

        logger.info(
            f" [ПРОИЗВОДИТЕЛЬНОСТЬ] "
            f"Event Queue: {event_queue_size} | "
            f"Analog Queue: {analog_queue_size} | "
            f"Buffer: {buffer_stats['current_size_mb']}MB / "
            f"{buffer_stats['max_size_mb']}MB"
        )

        # Предупреждение если очередь растет
        if event_queue_size > 1000 or analog_queue_size > 1000:
            logger.warning(" [ПРОИЗВОДИТЕЛЬНОСТЬ] ⚠️ Очередь переполняется!")


async def health_check():
    """Проверка здоровья БД"""
    loop = asyncio.get_event_loop()

    while True:
        await asyncio.sleep(300)  # каждые 5 минут
        try:
            result = await loop.run_in_executor(
                db_executor,
                lambda: pyodbc.connect(DB_DSN)
            )
            if result:
                result.close()
            logger.info(" [ЗДОРОВЬЕ] БД доступна ✓")
        except Exception as e:
            logger.error(f" [ЗДОРОВЬЕ] ❌ БД недоступна: {e}")


async def run_opc():
    global opc_connected
    url = OPC_URL

    while True:
        client = Client(url=url)
        client.set_user(OPC_USER)
        client.set_password(OPC_PASS)

        try:
            async with client:
                opc_connected = True
                await emit_opc_connection_state(True)

                logger.info(
                    "Считывание начальных значений аналоговых тегов...")
                for nid in ANALOG_TAGS:
                    try:
                        node = client.get_node(nid)
                        val = await node.read_value()
                        last_good_analog_values[nid] = val
                        latest_analog_values[nid] = (val, True, "InitialRead")
                    except Exception as e:
                        logger.error(f"Не удалось прочитать {nid}: {e}")

                handler = SubscriptionHandler()
                sub = await client.create_subscription(1000, handler)
                all_nodes = [client.get_node(nid)
                             for nid in (EVENT_TAGS + ANALOG_TAGS)]
                await sub.subscribe_data_change(all_nodes)
                logger.info(
                    f"✓ OPC подписка активна на {len(all_nodes)} тегов")

                while True:
                    await asyncio.sleep(10)
                    await client.get_node(ua.ObjectIds.Server_ServerStatus_State).read_value()

        except Exception as e:
            opc_connected = False
            await emit_opc_connection_state(False, f"{type(e).__name__}: {e}")
            logger.warning("Реконнект OPC: %s: %s", type(e).__name__, e)
            await asyncio.sleep(5)


async def main():
    """Главная функция - запуск всех корутин"""
    logger.info("=" * 60)
    logger.info("🚀 ЗАПУСК OPC-TO-SQL ПРИЛОЖЕНИЯ")
    logger.info(
        f"   Tags: {len(EVENT_TAGS)} события + {len(ANALOG_TAGS)} аналогов")
    logger.info(f"   Buffer: {JSON_BUFFER_MAX_MB}MB")
    logger.info("=" * 60)

    await asyncio.gather(
        run_opc(),
        periodic_analog_recorder(),
        health_check(),
        sync_buffer_to_db(),
        monitor_buffer(),
        performance_monitor(),
        db_writer(event_queue, "dbo.OpcEvents"),
        db_writer(analog_queue, "dbo.OpcAnalog", is_float=True)
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("⛔ Приложение остановлено пользователем")
