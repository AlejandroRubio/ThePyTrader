from sqlalchemy import create_engine
from urllib.parse import quote_plus
from parametrization import DB_DRIVER, DB_SERVER, DB_NAME

odbc_str = (
    f"DRIVER={{{DB_DRIVER}}};"
    f"SERVER={DB_SERVER};"
    f"DATABASE={DB_NAME};"
    "Trusted_Connection=yes;"
)


def get_database_engine():
    engine = create_engine(
        "mssql+pyodbc:///?odbc_connect=" + quote_plus(odbc_str), fast_executemany=True
    )
    return engine