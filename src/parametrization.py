import os
import logging

# Base de datos
DB_DRIVER = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")
DB_SERVER = os.getenv("DB_SERVER", r"AlexPC\SQLEXPRESS")
DB_NAME = os.getenv("DB_NAME", "INFO_BURSATIL")

# Rutas de ficheros
JSON_TICKERS_PATH = os.getenv(
    "JSON_TICKERS_PATH", r"C:\Labs\ThePyTrader\datasets\tickers_mapping.json"
)

# Cartera
_acciones_env = os.getenv("ACCIONES_EXCLUIDAS")
ACCIONES_EXCLUIDAS = _acciones_env.split(",") if _acciones_env else ["BATS", "Diageo"]

# Logging
LOG_LEVEL = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper())
LOG_FILE = os.getenv("LOG_FILE", r"C:\Labs\ThePyTrader\logs\thepytrader.log")
