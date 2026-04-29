import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import date

from services.db_manager import get_database_engine
from logger import get_logger

logger = get_logger(__name__)

engine = get_database_engine()


def obtener_metales_eur_oz_3y() -> pd.DataFrame:
    """
    Devuelve histórico diario (3 años) de:
    - Oro   (€/onza troy)
    - Plata (€/onza troy)
    - Cobre (€/onza troy, convertido desde USD/libra)

    Returns:
        DataFrame: fecha, oro_eur_oz, plata_eur_oz, cobre_eur_oz
    """

    symbols = {
        "oro": "GC=F",
        "plata": "SI=F",
        "cobre": "HG=F",
    }

    series = []

    # Tipo de cambio EUR/USD
    fx = yf.download("EURUSD=X", period="3y", interval="1d", progress=False)
    fx = fx.reset_index()

    if isinstance(fx.columns, pd.MultiIndex):
        fx.columns = [col[0] for col in fx.columns]

    fx = fx[["Date", "Close"]].rename(columns={"Date": "fecha", "Close": "eurusd"})

    for metal, ticker in symbols.items():
        df = yf.download(
            ticker,
            period="3y",
            interval="1d",
            progress=False,
            auto_adjust=False
        )

        if df.empty:
            logger.warning("No hay datos para %s", metal)
            continue

        df = df.reset_index()

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [col[0] for col in df.columns]

        df = df[["Date", "Close"]].rename(columns={"Date": "fecha", "Close": "precio_usd"})

        # Unir con tipo de cambio
        df = df.merge(fx, on="fecha", how="left")

        # Convertir a EUR
        df["precio_eur"] = df["precio_usd"] / df["eurusd"]

        # Ajuste específico para cobre
        if metal == "cobre":
            # USD/libra → USD/onza troy
            df["precio_eur"] = df["precio_eur"] / 14.5833

        df = df[["fecha", "precio_eur"]].rename(columns={
            "precio_eur": f"{metal}_eur_oz"
        })

        series.append(df)

    if not series:
        return pd.DataFrame()

    resultado = series[0]
    for s in series[1:]:
        resultado = resultado.merge(s, on="fecha", how="outer")

    resultado = resultado.sort_values("fecha").reset_index(drop=True)

    return resultado





def insertar_metales_en_bd(df: pd.DataFrame):
    """
    Inserta datos en SQL Server usando SQLAlchemy (con UPSERT vía MERGE)

    Args:
        df: DataFrame con columnas tipo:
            fecha, oro_eur_oz, plata_eur_oz, cobre_eur_oz
        connection_string: string de conexión SQLAlchemy

    """

    # Transformar a formato largo
    df_long = df.melt(
        id_vars=["fecha"],
        var_name="metal",
        value_name="precio"
    )

    # Limpiar nombres de metal
    df_long["metal"] = df_long["metal"].str.replace("_eur_oz", "", regex=False)

    # Añadir columnas adicionales
    df_long["divisa"] = "EUR"
    df_long["unidad"] = "oz_troy"
    df_long["fecha_carga"] = date.today()

    # Eliminar nulos
    df_long = df_long.dropna(subset=["precio"])

    # Convertir fecha a datetime.date
    df_long["fecha"] = pd.to_datetime(df_long["fecha"]).dt.date

    merge_sql = text("""
        MERGE dbo.metales_cotizacion AS target
        USING (SELECT
            :fecha AS fecha,
            :metal AS metal,
            :precio AS precio,
            :divisa AS divisa,
            :unidad AS unidad,
            :fecha_carga AS fecha_carga
        ) AS source
        ON target.fecha = source.fecha AND target.metal = source.metal

        WHEN MATCHED THEN
            UPDATE SET
                precio = source.precio,
                divisa = source.divisa,
                unidad = source.unidad,
                fecha_carga = source.fecha_carga

        WHEN NOT MATCHED THEN
            INSERT (fecha, metal, precio, divisa, unidad, fecha_carga)
            VALUES (source.fecha, source.metal, source.precio, source.divisa, source.unidad, source.fecha_carga);
    """)

    records = df_long.to_dict(orient="records")
    for r in records:
        r["precio"] = float(r["precio"])

    with engine.begin() as conn:
        conn.execute(merge_sql, records)

    logger.info("Insertadas/actualizadas %d filas", len(df_long))


def procesado_metales_completo():
    metales_df = obtener_metales_eur_oz_3y() 
    logger.debug("\n%s", metales_df.head())
    logger.debug("\n%s", metales_df.tail())
    insertar_metales_en_bd(metales_df)
