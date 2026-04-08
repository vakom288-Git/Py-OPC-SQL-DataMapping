# === НАСТРОЙКИ ПОДКЛЮЧЕНИЯ ===
import os as _os
OPC_URL = "opc.tcp://192.168.100.18:4840"
OPC_USER = ""
OPC_PASS = ""
# OPC_USER = "admin"
# OPC_PASS = "admin"
OPC_CONN_NODEID = "sys.opc.connection"

DB_CONFIG = {
    "server": "192.168.1.72",
    "port": 1433,
    "database": "gpna-bdrv_test_empty",
    "user": _os.environ.get("DB_USER", ""),
    "password": _os.environ.get("DB_PASSWORD", ""),
    "trusted_connection": "",              # важно для SQL Login
    "trust_server_certificate": "yes",
}

# === НАСТРОЙКИ ПЕРИОДИЧНОСТИ И БУФЕРИЗАЦИИ ===
ANALOG_SAVE_INTERVAL = 10      # секунд - как часто снимать срез аналогов
DB_BATCH_SIZE = 500            # максимум записей в одном batch INSERT
JSON_BUFFER_MAX_MB = 500       # максимальный размер буфера в МБ
JSON_BUFFER_MAX_RECORDS = 10000  # максимум записей в одном JSON файле

# === СПИСКИ ТЕГОВ ===

ANALOG_TAGS = [
    {
        "opc": "ns=1;i=303",		# AisStruna[4].value
        "name": "масса РГС-50 №1",
        "id1": 37,
        "id2": 101,
        "id3": 1,
        "val": 0.0,
        "id4": 1,
        "deadband_abs": 0.5,  # 0 => не фильтровать
    },
    {
        "opc": "ns=1;i=314",		# AisStruna[5].value
        "name": "объем РГС-50 №1",
        "id1": 37,
        "id2": 102,
        "id3": 2,
        "val": 0.0,
        "id4": 1,
        "deadband_abs": 0.5,  # 0 => не фильтровать
    },
    {
        "opc": "ns=1;i=281",		# AisStruna[2].value
        "name": "температура РГС-50 №1",
        "id1": 37,
        "id2": 103,
        "id3": 3,
        "val": 0.0,
        "id4": 1,
        "deadband_abs": 0.5,  # 0 => не фильтровать
    },
    {
        "opc": "ns=1;i=292",		# AisStruna[3].value
        "name": "плотность РГС-50 №1",
        "id1": 37,
        "id2": 104,
        "id3": 4,
        "val": 0.0,
        "id4": 1,
        "deadband_abs": 0.5,  # 0 => не фильтровать
    },
    {
        "opc": "ns=1;i=270",		# AisStruna[1].value
        "name": "Уровень РГС-50 №1",
        "id1": 37,
        "id2": 105,
        "id3": 5,
        "val": 0.0,
        "id4": 1,
        "deadband_abs": 0.5,  # 0 => не фильтровать
    },
    {
        "opc": "ns=1;i=325",		# AisStruna[6].value
        "name": "уровень подтоварной воды РГС-50 №1масса РГС-50 №1",
        "id1": 37,
        "id2": 106,
        "id3": 6,
        "val": 0.0,
        "id4": 1,
        "deadband_abs": 0.5,  # 0 => не фильтровать
    },
    {
        "opc": "ns=1;i=369",		# AisStruna[10].value
        "name": "масса РГС-50 №2",
        "id1": 37,
        "id2": 201,
        "id3": 1,
        "val": 0.0,
        "id4": 2,
        "deadband_abs": 0.5,  # 0 => не фильтровать
    },
    {
        "opc": "ns=1;i=380",		# AisStruna[11].value
        "name": "объем РГС-50 №2",
        "id1": 37,
        "id2": 202,
        "id3": 2,
        "val": 0.0,
        "id4": 1,
        "deadband_abs": 0.5,  # 0 => не фильтровать
    },
    {
        "opc": "ns=1;i=347",		# AisStruna[8].value
        "name": "температура РГС-50 №2",
        "id1": 37,
        "id2": 203,
        "id3": 3,
        "val": 0.0,
        "id4": 2,
        "deadband_abs": 0.5,  # 0 => не фильтровать
    },
    {
        "opc": "ns=1;i=361",		# AisStruna[9].value
        "name": "плотность РГС-50 №2",
        "id1": 37,
        "id2": 204,
        "id3": 4,
        "val": 0.0,
        "id4": 2,
        "deadband_abs": 0.5,  # 0 => не фильтровать
    },
    {
        "opc": "ns=1;i=336",		# AisStruna[7].value
        "name": "Уровень РГС-50 №2",
        "id1": 37,
        "id2": 205,
        "id3": 5,
        "val": 0.0,
        "id4": 2,
        "deadband_abs": 0.5,  # 0 => не фильтровать
    },
    {
        "opc": "ns=1;i=391",		# AisStruna[12].value
        "name": "уровень подтоварной воды РГС-50 №2",
        "id1": 37,
        "id2": 206,
        "id3": 6,
        "val": 0.0,
        "id4": 2,
        "deadband_abs": 0.5,  # 0 => не фильтровать
    },
]
