from sqlalchemy import create_engine
from urllib.parse import quote_plus

driver = "ODBC Driver 17 for SQL Server"
server = r"AlexPC\SQLEXPRESS"
database = "INFO_BURSATIL"

odbc_str = (
    f"DRIVER={{{driver}}};"
    f"SERVER={server};"
    f"DATABASE={database};"
    "Trusted_Connection=yes;"
)


def get_database_engine():
    engine = create_engine(
        "mssql+pyodbc:///?odbc_connect=" + quote_plus(odbc_str), fast_executemany=True
    )
    return engine