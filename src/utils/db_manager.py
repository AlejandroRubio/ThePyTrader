# db_manager.py

import pyodbc


class DBConnectionManager:
    def __init__(
        self,
        driver: str,
        server: str,
        database: str,
        username: str | None = None,
        password: str | None = None,
        trusted_connection: bool = False,
    ) -> None:
        """
        Manager de conexión reutilizable para SQL Server.

        - Si trusted_connection = True -> usa autenticación Windows.
        - Si trusted_connection = False -> usa UID / PWD.
        """
        self.driver = driver
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.trusted_connection = trusted_connection

    def get_connection_string(self) -> str:
        """Construye y devuelve la cadena de conexión ODBC."""
        if self.trusted_connection:
            return (
                f"DRIVER={{{self.driver}}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                "Trusted_Connection=yes;"
            )
        else:
            return (
                f"DRIVER={{{self.driver}}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"UID={self.username};"
                f"PWD={self.password};"
            )

    def connect(self):
        """Devuelve una conexión pyodbc abierta."""
        conn_string = self.get_connection_string()
        return pyodbc.connect(conn_string)
