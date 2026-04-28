import logging

# Base de datos
DB_DRIVER = "ODBC Driver 17 for SQL Server"
DB_SERVER = r"AlexPC\SQLEXPRESS"
DB_NAME = "INFO_BURSATIL"

# Rutas de ficheros
JSON_TICKERS_PATH = r"C:\Labs\ThePyTrader\datasets\tickers_mapping.json"

# Cartera
ACCIONES_EXCLUIDAS = ["BATS", "Diageo"]

# Logging
LOG_LEVEL = logging.INFO
LOG_FILE = r"C:\Labs\ThePyTrader\logs\thepytrader.log"
