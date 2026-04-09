from json_buffer_manager import JSONBufferManager
from opc_tags_list import (
    ANALOG_TAGS, OPC_URL, OPC_USER, OPC_PASS, ANALOG_SAVE_INTERVAL,
    JSON_BUFFER_MAX_MB, JSON_BUFFER_MAX_RECORDS, OPC_CONN_NODEID,
)
from opc_tags_events import (EVENT_TAGS)
from asyncua import Client, ua
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
import time

import db_mssql
import bdrv_client

ANALOG_TAGS_BY_OPC = {t["opc"]: t for t in ANALOG_TAGS}
EVENT_TAGS_BY_OPC = {t["opc"]: t for t in EVENT_TAGS}
ALL_TAG_OPC_IDS = list(EVENT_TAGS_BY_OPC.keys()) + \
    list(ANALOG_TAGS_BY_OPC.keys())

# === ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ===
last_opc_connection_state = None        # type: bool | None
last_opc_disconnect_reason = None       # type: str | None
latest_analog_values = {}
event_queue = asyncio.Queue()
analog_queue = asyncio.Queue()
msr_queue: asyncio.Queue = asyncio.Queue()  # (ffc_id, msd_id, value, source_ts)
buffer = JSONBufferManager(
    buffer_dir="./buffer",
    max_size_mb=JSON_BUFFER_MAX_MB,
    max_records_per_file=JSON_BUFFER_MAX_RECORDS
)
db_is_available: bool | None = None
opc_connected = False           # node_id -> last_good_value
last_good_analog_values = {}    # Пул потоков для БД операций
db_executor = ThreadPoolExecutor(max_workers=2)
buffering_active = False            # node_id -> last write epoch seconds
last_analog_write_ts: dict[str, float] = {}
# --- Analog debounce/throttle (1 tag -> не чаще 1/мин, но пишем последнее) ---
ANALOG_MIN_WRITE_INTERVAL_SEC = 60
ANALOG_WRITE_INTERVAL_SEC = 60

# node_id -> (val, is_good, status)
pending_analog: dict[str, tuple[object, bool, str]] = {}
# node_id -> when we are allowed to write next (epoch seconds)
analog_next_write_at: dict[str, float] = {}
# node_id currently scheduled for delayed flush
analog_flush_scheduled: set[str] = set()

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
        is_good, status = extract_quality(data)

        # Extract OPC source timestamp for measurements
        try:
            source_ts = data.monitored_item.Value.SourceTimestamp
        except Exception:
            source_ts = None

        # --- EVENTS ---
        if node_id in EVENT_TAGS_BY_OPC:
            db_val = None if (val is None or not is_good) else str(val)
            await event_queue.put((node_id, db_val, bool(is_good), clip(status)))
            return

        # --- ANALOGS ---
        tag = ANALOG_TAGS_BY_OPC.get(node_id)
        if tag is None:
            return

        deadband = float(tag.get("deadband_abs") or 0.0)

        # bad -> всегда пишем NULL
        if not is_good:
            await analog_queue.put((node_id, None, False, clip(status)))
            return

        v = safe_float(val)
        if v is None:
            await analog_queue.put((node_id, None, False, f"BadValue({status})"))
            return

        # deadband=0 -> не фильтруем
        if deadband <= 0:
            await analog_queue.put((node_id, v, True, clip(status)))
            await msr_queue.put((tag["id1"], tag["id2"], v, source_ts))
            return

        prev = safe_float(last_good_analog_values.get(node_id))
        if prev is None or abs(v - prev) >= deadband:
            last_good_analog_values[node_id] = v
            await analog_queue.put((node_id, v, True, clip(status)))
            await msr_queue.put((tag["id1"], tag["id2"], v, source_ts))


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

    await event_queue.put((OPC_CONN_NODEID, val, is_good, clip(status)))


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


def clip(s: str, n: int = 200) -> str:
    s = "" if s is None else str(s)
    return s if len(s) <= n else s[:n]


def _insert_event_one(nid: str, val: str | None, is_good: bool, status: str):
    sql = (
        "INSERT INTO dbo.OpcEvents (NodeId, [Value], [Timestamp], QualityIsGood, QualityStatus) "
        "VALUES (?, ?, GETDATE(), ?, ?)"
    )
    params = (nid, val, 1 if is_good else 0, clip(status))
    db_mssql.execute(sql, params)


def _insert_analog_one(nid: str, val, is_good: bool, status: str):
    sql = (
        "INSERT INTO dbo.OpcAnalog (NodeId, [Value], [Timestamp], QualityIsGood, QualityStatus) "
        "VALUES (?, ?, GETDATE(), ?, ?)"
    )
    # val уже должен быть float или None
    params = (nid, val, 1 if is_good else 0, clip(status))
    db_mssql.execute(sql, params)


async def db_writer_events():
    global buffer, buffering_active
    loop = asyncio.get_event_loop()
    consecutive_errors = 0

    while True:
        nid, val, is_good, status = await event_queue.get()
        try:
            db_val = None if val is None else str(val)
            # Нормализация: NULL в Value всегда считаем bad
            if db_val is None:
                is_good = False

            await loop.run_in_executor(
                db_executor,
                lambda: _insert_event_one(
                    str(nid), db_val, bool(is_good), str(status))
            )

            consecutive_errors = 0

        except Exception as e:
            consecutive_errors += 1
            if consecutive_errors == 1 and not buffering_active:
                logger.error(
                    "❌ Связь с SQL Server потеряна - буферизация активирована")
                buffer.start_buffering_session()
                buffering_active = True

            buffer.add_event(str(nid), db_val, bool(is_good), clip(status))
            await asyncio.sleep(1)

        finally:
            event_queue.task_done()


async def _flush_one_analog(node_id: str):
    """Ждёт до analog_next_write_at[node_id], потом пишет последнее pending значение."""
    global pending_analog, analog_next_write_at, analog_flush_scheduled, buffer, buffering_active

    loop = asyncio.get_event_loop()

    try:
        while True:
            now = time.time()
            due = analog_next_write_at.get(node_id, now)

            # ждём до разрешённого времени
            delay = due - now
            if delay > 0:
                await asyncio.sleep(delay)

            # берём последнее значение
            triple = pending_analog.get(node_id)
            if triple is None:
                return  # нечего писать

            val, is_good, status = triple
            db_val = safe_float(val)  # float или None

            try:
                await loop.run_in_executor(
                    db_executor,
                    lambda: _insert_analog_one(
                        node_id, db_val, bool(is_good), str(status))
                )
            except Exception as e:
                if not buffering_active:
                    logger.error(
                        "❌ Связь с SQL Server потеряна - буферизация активирована")
                    buffer.start_buffering_session()
                    buffering_active = True

                buffer.add_analog(node_id, db_val, bool(is_good), clip(status))
                await asyncio.sleep(1)
                # После ошибки оставим pending и попробуем позже.
                analog_next_write_at[node_id] = time.time(
                ) + ANALOG_WRITE_INTERVAL_SEC
                continue

            # Успешно записали. Сдвигаем окно на +60 сек и проверяем:
            # если за время ожидания пришли новые изменения — pending уже обновлён,
            # значит нужно снова подождать и записать следующее "последнее" через 60 сек.
            analog_next_write_at[node_id] = time.time() + \
                ANALOG_WRITE_INTERVAL_SEC

            # Если pending не менялся — можно не крутиться.
            # Но у нас нет счётчика версий, поэтому просто выходим, а новый flush будет запланирован
            # при следующем изменении.
            pending_analog.pop(node_id, None)
            return

    finally:
        analog_flush_scheduled.discard(node_id)


async def db_writer_analogs():
    """Принимает изменения по аналогам и планирует отложенную запись 1 раз в минуту на tag (последнее значение)."""
    global pending_analog, analog_next_write_at, analog_flush_scheduled

    while True:
        nid, val, is_good, status = await analog_queue.get()
        try:
            node_id = str(nid)
            # сохраняем последнее значение (good & bad)
            pending_analog[node_id] = (val, bool(is_good), str(status))

            now = time.time()
            # если тега ещё не было — разрешаем запись сразу (сделает insert и откроет окно 60 сек)
            if node_id not in analog_next_write_at:
                analog_next_write_at[node_id] = now

            # Если ещё не запланирован flush — запланировать
            if node_id not in analog_flush_scheduled:
                analog_flush_scheduled.add(node_id)
                asyncio.create_task(_flush_one_analog(node_id))

        finally:
            analog_queue.task_done()


async def db_writer_measurements():
    """
    Принимает измерения из msr_queue и отправляет их в SQL Server через
    Sp_msr_value_send (1 вызов = 1 транзакция).
    При ошибке буферизует запись и продолжает работу.
    """
    global buffer, buffering_active

    loop = asyncio.get_event_loop()

    while True:
        ffc_id, msd_id, value, source_ts = await msr_queue.get()
        try:
            await loop.run_in_executor(
                db_executor,
                lambda ffc=ffc_id, msd=msd_id, v=value, ts=source_ts: bdrv_client.send_value(ffc, msd, v, ts),
            )
        except Exception as e:
            if not buffering_active:
                logger.error(
                    "❌ Связь с SQL Server потеряна (msr) - буферизация активирована")
                buffer.start_buffering_session()
                buffering_active = True

            # Serialize msr_time using the same conversion as send_value() so
            # drain_buffer() later sends exactly the same datetime value.
            ts_str = bdrv_client.to_db_naive(source_ts).isoformat()
            buffer.add_measurement(ffc_id, msd_id, float(value), ts_str)
            logger.debug("msr buffered ffc=%s msd=%s: %s", ffc_id, msd_id, e)
        finally:
            msr_queue.task_done()


async def periodic_analog_recorder():
    while True:
        await asyncio.sleep(ANALOG_SAVE_INTERVAL)

        # Всегда пробегаем по ANALOG_TAGS, чтобы писать ровные срезы
        for t in ANALOG_TAGS:
            node_id = t["opc"]
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
    """Синхронизация буферизованных данных при восстановлении связи.
    Правило: throttle=0 (пишем всё), 1 запись = 1 INSERT.
    """
    global buffer, buffering_active
    global db_is_available

    loop = asyncio.get_event_loop()
    sync_attempts = 0
    last_error_logged = None
    sync_in_progress = False

    while True:
        await asyncio.sleep(20)
        # Не пытаемся синхронизировать, если health_check уже сказал, что БД недоступна
        if db_is_available is False:
            continue

        stats = buffer.get_stats()
        unsync_count = (
            stats.get("events_unsynced", 0)
            + stats.get("analogs_unsynced", 0)
            + stats.get("measurements_unsynced", 0)
        )

        if unsync_count == 0:
            sync_attempts = 0
            last_error_logged = None
            sync_in_progress = False
            continue

        sync_attempts += 1
        if sync_attempts == 1:
            logger.info(
                f" [СИНХРО] Начало синхронизации буфера: {unsync_count} записей")
            sync_in_progress = True

        try:
            buffer_data = buffer.get_unsync_data(limit=2000)
            if not buffer_data.get("events") and not buffer_data.get("analogs"):
                # Check if there are buffered measurements to drain
                msr_records = buffer.get_unsync_measurements(limit=1)
                if not msr_records:
                    logger.warning(
                        " [СИНХРО] ⚠️ get_unsync_data() вернул пусто при наличии unsync_count. "
                        "Возможно, файлы буфера повреждены или stats не совпадают с содержимым."
                    )
                    continue

            synced_events = []
            synced_analogs = []

            # --- События (1 запись = 1 INSERT) ---
            if buffer_data.get("events"):
                for record in buffer_data["events"]:
                    nid = str(record["node_id"])
                    is_good = bool(record.get("is_good"))
                    status = clip(record.get("status") or "")

                    # bad -> NULL
                    val = record.get("value")
                    db_val = None if val is None else str(val)

                    # Нормализация: NULL в Value всегда считаем bad
                    if db_val is None:
                        is_good = False

                    await loop.run_in_executor(
                        db_executor,
                        lambda nid=nid, db_val=db_val, is_good=is_good, status=status: _insert_event_one(
                            nid, db_val, is_good, status
                        )
                    )

                    synced_events.append(record["timestamp"])

                logger.info(
                    f" [СИНХРО] ✓ События: +{len(buffer_data['events'])} записей")

            # --- Аналоги (1 запись = 1 INSERT, throttle=0) ---
            if buffer_data.get("analogs"):
                for record in buffer_data["analogs"]:
                    nid = str(record["node_id"])
                    is_good = bool(record.get("is_good"))
                    status = clip(record.get("status") or "")

                    # bad -> NULL
                    val = record.get("value")
                    db_val = safe_float(val) if is_good else None

                    await loop.run_in_executor(
                        db_executor,
                        lambda nid=nid, db_val=db_val, is_good=is_good, status=status: _insert_analog_one(
                            nid, db_val, is_good, status
                        )
                    )

                    synced_analogs.append(record["timestamp"])

                logger.info(
                    f" [СИНХРО] ✓ Аналоги: +{len(buffer_data['analogs'])} записей")

            # --- Измерения (1 запись = 1 EXEC Sp_msr_value_send) ---
            msr_sent = await loop.run_in_executor(
                db_executor,
                lambda: bdrv_client.drain_buffer(buffer, limit=2000),
            )
            if msr_sent:
                logger.info(f" [СИНХРО] ✓ Измерения: +{msr_sent} записей")

            # Если реально что-то синхронизировали — отмечаем synced и закрываем буфер-сессию
            if synced_events or synced_analogs or msr_sent:
                buffer.mark_synced(event_ids=synced_events,
                                   analog_ids=synced_analogs)

                # ВСЕГДА создаём архив активных файлов после успешной синхронизации
                created = buffer.close_buffering_session()
                logger.info(f" [JSON_БУФЕР] Архив буфера сохранён: {created}")
                buffering_active = False

                deleted = buffer.cleanup_synced()

                sync_attempts = 0
                last_error_logged = None

                if sync_in_progress:
                    logger.info(
                        "✅ Связь с SQL Server восстановлена - синхронизация завершена")
                    sync_in_progress = False

                logger.info(f" [JSON_БУФЕР] cleanup_synced(): {deleted}")

                new_stats = buffer.get_stats()
                logger.info(
                    f" [JSON_БУФЕР] Статус: "
                    f"{new_stats.get('usage_percent', 0)}% "
                    f"({new_stats.get('current_size_mb', 0)}MB / {new_stats.get('max_size_mb', 0)}MB)"
                )

        except Exception as e:
            db_is_available = False
            err_text = f"{type(e).__name__}: {e}"
            if last_error_logged != err_text:
                logger.warning(f" [СИНХРО] Исключение: {err_text}")

                if sync_attempts <= 2:
                    logger.warning(
                        f" [СИНХРО] БД еще недоступна (попытка #{sync_attempts})")
                elif sync_attempts == 3:
                    logger.error(
                        " [СИНХРО] БД остается недоступной - данные буферизуются")

                last_error_logged = err_text


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
            f"Измерения: {stats.get('measurements_unsynced', 0)}/{stats.get('measurements_total', 0)} | "
            f"Ротир. файлов: {stats['rotated_files']}"
        )


async def performance_monitor():
    """Мониторинг производительности системы"""
    while True:
        await asyncio.sleep(60)

        event_queue_size = event_queue.qsize()
        analog_queue_size = analog_queue.qsize()
        msr_queue_size = msr_queue.qsize()
        buffer_stats = buffer.get_stats()

        logger.info(
            f" [ПРОИЗВОДИТЕЛЬНОСТЬ] "
            f"Event Queue: {event_queue_size} | "
            f"Analog Queue: {analog_queue_size} | "
            f"Msr Queue: {msr_queue_size} | "
            f"Buffer: {buffer_stats['current_size_mb']}MB / "
            f"{buffer_stats['max_size_mb']}MB"
        )

        # Предупреждение если очередь растет
        if event_queue_size > 1000 or analog_queue_size > 1000 or msr_queue_size > 1000:
            logger.warning(" [ПРОИЗВОДИТЕЛЬНОСТЬ] ⚠️ Очередь переполняется!")


async def health_check():
    global buffer, buffering_active
    global db_is_available
    loop = asyncio.get_event_loop()
    prev_ok: bool | None = None

    while True:
        await asyncio.sleep(15)

        ok = await loop.run_in_executor(db_executor, db_mssql.health_ping)
        db_is_available = ok  # ВСЕГДА обновляем последнее состояние БД

        # логируем только при смене состояния (чтобы не спамить)
        if prev_ok is None or ok != prev_ok:
            if ok:
                logger.info(" [ДОСТУПНОСТЬ БД] БД доступна ✓")
            else:
                logger.error(" [ДОСТУПНОСТЬ БД] ❌ БД недоступна")
            prev_ok = ok
        # если БД стала недоступна — заранее включаем буферизацию
        if not ok and not buffering_active:
            logger.error(
                "❌ Связь с SQL Server потеряна (health_check) - буферизация активирована")
            buffer.start_buffering_session()
            buffering_active = True


async def run_opc():
    global opc_connected
    url = OPC_URL

    while True:
        opc_client = Client(url=url)
        opc_client.set_user(OPC_USER)
        opc_client.set_password(OPC_PASS)

        try:
            async with opc_client:
                opc_connected = True
                await emit_opc_connection_state(True)

                logger.info(
                    "Считывание начальных значений аналоговых тегов...")
                for t in ANALOG_TAGS:
                    nid = t["opc"]
                    try:
                        node = opc_client.get_node(nid)
                        val = await node.read_value()
                        last_good_analog_values[nid] = val
                        latest_analog_values[nid] = (val, True, "InitialRead")
                    except Exception as e:
                        # ВАЖНО: только логируем, НЕ валим соединение
                        logger.warning(
                            f"InitialRead failed for {nid}: {type(e).__name__}: {e}")

            # дальше подписка (она должна выполниться даже если initial read частично не удался)
                handler = SubscriptionHandler()
                sub = await opc_client.create_subscription(1000, handler)

                event_ids = [t["opc"] for t in EVENT_TAGS]
                analog_ids = [t["opc"] for t in ANALOG_TAGS]
                all_nodes = [opc_client.get_node(nid)
                             for nid in (event_ids + analog_ids)]

                await sub.subscribe_data_change(all_nodes)
                logger.info(
                    f"✓ OPC подписка активна на {len(all_nodes)} тегов")

                while True:
                    await asyncio.sleep(10)
                    await opc_client.get_node(ua.ObjectIds.Server_ServerStatus_State).read_value()

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
        #        periodic_analog_recorder(),
        health_check(),
        sync_buffer_to_db(),
        monitor_buffer(),
        performance_monitor(),
        db_writer_events(),
        db_writer_analogs(),
        db_writer_measurements(),)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("⛔ Приложение остановлено пользователем")
