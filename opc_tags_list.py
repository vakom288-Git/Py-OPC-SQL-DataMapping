# === НАСТРОЙКИ ПОДКЛЮЧЕНИЯ ===
OPC_URL = "opc.tcp://192.168.2.215:4880"
OPC_USER = "admin"
OPC_PASS = "admin"
OPC_CONN_NODEID = "sys.opc.connection"

DB_DSN = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=192.168.1.72,1433;"
    "DATABASE=DB_NEW_TEST;"
#    "UID=sa;"                           # ← SQL Server Login
#    "PWD=your_password_here;"           # ← Пароль SQL Server
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)

# === СПИСКИ ТЕГОВ ===
EVENT_TAGS = [
    "ns=1;s=Data_Conf.Dev1.DO1332-1 (32ch DO).DO_3_1",
    "ns=1;s=Data_Conf.Dev1.DO1332-1 (32ch DO).DO_3_2",
    "ns=1;s=Data_Conf.Dev1.DO1332-1 (32ch DO).DO_3_3",
    "ns=1;s=Data_Conf.Dev1.DO1332-1 (32ch DO).DO_3_4",
    "ns=1;s=Data_Conf.Dev1.DO1332-1 (32ch DO).DO_3_5",
    "ns=1;s=Data_Conf.Dev1.DO1332-1 (32ch DO).DO_3_6",
    "ns=1;s=Data_Conf.Dev1.DO1332-1 (32ch DO).DO_3_7",
    "ns=1;s=Data_Conf.Dev1.DO1332-1 (32ch DO).DO_3_8",
]

ANALOG_TAGS = [
    "ns=1;s=Data_Conf.Dev1.AI1316-5 (8ch TC).AI_5_1",
    "ns=1;s=Data_Conf.Dev1.AI1316-5 (8ch TC).AI-5_2",
    "ns=1;s=Data_Conf.Dev1.AI1316-5 (8ch TC).AI-5_3",
    "ns=1;s=Data_Conf.Dev1.AI1316-5 (8ch TC).AI-5_4",
    "ns=1;s=Data_Conf.Dev1.AI1316-5 (8ch TC).AI-5_5",
    "ns=1;s=Data_Conf.Dev1.AI1316-5 (8ch TC).AI-5_6",
    "ns=1;s=Data_Conf.Dev1.AI1316-5 (8ch TC).AI-5_7",
    "ns=1;s=Data_Conf.Dev1.AI1316-5 (8ch TC).AI-5_8"
]

# === НАСТРОЙКИ ПЕРИОДИЧНОСТИ И БУФЕРИЗАЦИИ ===
ANALOG_SAVE_INTERVAL = 10      # секунд - как часто снимать срез аналогов
DB_BATCH_SIZE = 500            # максимум записей в одном batch INSERT
JSON_BUFFER_MAX_MB = 500       # максимальный размер буфера в МБ
JSON_BUFFER_MAX_RECORDS = 10000 # максимум записей в одном JSON файле