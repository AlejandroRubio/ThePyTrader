from datetime import datetime
import pandas as pd
import numpy as np
from sqlalchemy import text
from typing import Iterable
from services.db_manager import get_database_engine
from services.price_manager import obtener_ultimos_precios_cartera
from parametrization import ACCIONES_EXCLUIDAS
from logger import get_logger

logger = get_logger(__name__)

engine = get_database_engine()


def obtener_acciones_compras_df() -> pd.DataFrame | None:
    """
    Devuelve un DataFrame con todo el contenido de dbo.acciones_compras.
    """
    conn = None

    query = """
        SELECT * FROM dbo.acciones_compras
    """

    try:
        df = pd.read_sql(query, engine)
        logger.info("Obtenidas un total de %d compras", len(df))
        return df

    except Exception:
        logger.exception("Error durante la conexión o la consulta")
        return None

    finally:
        if conn is not None:
            conn.close()


def obtener_acciones_ventas_df() -> pd.DataFrame | None:
    """
    Devuelve un DataFrame con todo el contenido de dbo.acciones_compras.
    """
    conn = None

    query = """
        SELECT * FROM dbo.acciones_ventas
    """

    try:
        df = pd.read_sql(query, engine)
        logger.info("Obtenidas un total de %d ventas", len(df))
        return df

    except Exception:
        logger.exception("Error durante la conexión o la consulta")
        return None

    finally:
        if conn is not None:
            conn.close()


def calcular_cartera_actual(df_compras, df_ventas):
    # Ordenar por fecha para asegurar FIFO
    df_compras = df_compras.sort_values(by="fecha").copy()
    df_ventas = df_ventas.sort_values(by="fecha").copy()

    # Agrupar ventas por acción
    ventas_grouped = df_ventas.groupby("accion")["numero_acciones"].sum()

    # Copia del dataframe de compras para restar
    cartera = df_compras.copy()

    # Procesar restas por acción
    for accion, total_vendido in ventas_grouped.items():
        # Filtrar compras de esa acción
        mask = cartera["accion"] == accion
        compras_accion = cartera[mask].copy()

        for idx, row in compras_accion.iterrows():
            if total_vendido <= 0:
                break

            if row["numero_acciones"] <= total_vendido:
                # Se consume completamente esta compra
                total_vendido -= row["numero_acciones"]
                cartera.at[idx, "numero_acciones"] = 0
            else:
                # Solo se resta una parte
                cartera.at[idx, "numero_acciones"] -= total_vendido
                total_vendido = 0

    # Filtrar compras con acciones remanentes
    cartera_final = cartera[cartera["numero_acciones"] > 0].reset_index(drop=True)
    return cartera_final


def resumir_cartera_por_accion(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrupa una cartera de acciones por 'accion' y calcula:
    - total de acciones
    - total de comisión
    - precio medio de compra (ponderado)

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame origen con las columnas:
        ['accion', 'numero_acciones', 'valor_accion', 'comision']

    Returns
    -------
    pd.DataFrame
        DataFrame resumido por acción
    """

    # Copia defensiva para no modificar el df original
    df = df.copy()

    # Importe total por operación
    df["importe"] = df["numero_acciones"] * df["valor_accion"]

    resumen = df.groupby("accion", as_index=False).agg(
        total_acciones=("numero_acciones", "sum"),
        total_comision=("comision", "sum"),
        importe_total=("importe", "sum"),
    )

    # Precio medio ponderado
    resumen["precio_medio"] = resumen["importe_total"] / resumen["total_acciones"]

    # Limpieza
    resumen = resumen.drop(columns="importe_total")

    return resumen


def anadir_ticker_desde_bd(df: pd.DataFrame) -> pd.DataFrame:
    """
    Añade una columna con el ticker a un DataFrame usando una tabla de SQL Server.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame origen (debe contener la columna `col_accion`)
    connection_string : str
        Cadena de conexión SQLAlchemy para SQL Server.
        Ejemplo:
        'mssql+pyodbc://usuario:password@servidor/basedatos?driver=ODBC+Driver+17+for+SQL+Server'
    Returns
    -------
    pd.DataFrame
        DataFrame con la nueva columna `col_ticker`
    """

    col_accion = "accion"
    col_ticker = "ticker"

    if col_accion not in df.columns:
        raise ValueError(f"La columna '{col_accion}' no existe en el DataFrame.")

    # Copia defensiva
    df_out = df.copy()

    # Leer mapeo desde SQL Server
    query = f"""
        SELECT
            nombre_empresa AS accion,
            ticker AS ticker
        FROM dbo.info_tickers
    """

    df_mapeo = pd.read_sql(query, engine)

    # Convertir a diccionario
    accion_a_ticker = dict(zip(df_mapeo["accion"], df_mapeo["ticker"]))

    # Mapear
    df_out[col_ticker] = df_out[col_accion].map(accion_a_ticker)

    # Aviso opcional si faltan tickers
    faltantes = df_out[df_out[col_ticker].isna()][col_accion].dropna().unique()
    if len(faltantes) > 0:
        logger.warning("Acciones sin ticker en la BD: %s", list(faltantes))

    return df_out


def calcular_rendimiento_y_ganancia_por_accion(
    df: pd.DataFrame,
    col_accion: str = "accion",
    col_total_acciones: str = "total_acciones",
    col_precio_medio: str = "precio_medio",
    col_ultimo_precio: str = "ultimo_precio",
    col_comision: str = "total_comision",
    incluir_comisiones: bool = True,
    devolver_df: bool = True,
) -> pd.DataFrame:
    """
    Calcula por acción:
    - total_ganado (P&L en euros/moneda): (ultimo_precio - precio_medio) * total_acciones
      (opcionalmente restando comisiones)
    - rendimiento_pct: total_ganado / coste_total * 100
      (si coste_total == 0 => rendimiento_pct = NaN)

    IMPORTANTE:
    - Si el coste de compra (precio_medio * total_acciones) es 0, no se puede calcular
      un % de rentabilidad meaningful -> se devuelve NaN.

    Requisitos: df debe tener columnas con acciones agregadas (una fila por accion),
    incluyendo total_acciones, precio_medio, ultimo_precio (y opcionalmente total_comision).

    Returns
    -------
    pd.DataFrame
        df con columnas añadidas:
        - coste_total
        - valor_actual
        - total_ganado
        - rendimiento_pct
    """

    out = df.copy() if devolver_df else df

    # Asegurar numéricos
    for c in [col_total_acciones, col_precio_medio, col_ultimo_precio]:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    if incluir_comisiones and col_comision in out.columns:
        out[col_comision] = pd.to_numeric(out[col_comision], errors="coerce").fillna(
            0.0
        )
    else:
        # si no se incluyen o no existe columna, tratamos comisión como 0
        out[col_comision] = 0.0

    # Coste total de compra (sin comisiones) y valor actual
    out["coste_total"] = out[col_total_acciones] * out[col_precio_medio]
    out["valor_actual"] = out[col_total_acciones] * out[col_ultimo_precio]

    # P&L / total ganado
    out["total_ganado"] = (out[col_ultimo_precio] - out[col_precio_medio]) * out[
        col_total_acciones
    ]
    if incluir_comisiones:
        out["total_ganado"] = out["total_ganado"] - out[col_comision]

    # Rendimiento % (si coste_total == 0 -> NaN)
    out["rendimiento_pct"] = np.where(
        out["coste_total"].abs() > 0,
        (out["total_ganado"] / out["coste_total"]) * 100.0,
        np.nan,
    )

    # Limpieza del rendimiento
    out["rendimiento_pct"] = out["rendimiento_pct"].fillna(0)

    return out


def imprimir_resumen_cartera(
    df: pd.DataFrame,
    col_coste_total: str = "coste_total",
    col_valor_actual: str = "valor_actual",
    col_total_ganado: str = "total_ganado",
) -> None:
    """
    Imprime el resumen global de la cartera:
    - cantidad total invertida
    - valor total actual
    - beneficio total

    Requisitos:
    df debe contener las columnas:
    - coste_total
    - valor_actual
    - total_ganado
    """

    # Asegurar numéricos
    coste_total = pd.to_numeric(df[col_coste_total], errors="coerce").fillna(0).sum()
    valor_actual = pd.to_numeric(df[col_valor_actual], errors="coerce").fillna(0).sum()
    beneficio_total = (
        pd.to_numeric(df[col_total_ganado], errors="coerce").fillna(0).sum()
    )

    logger.info("RESUMEN GLOBAL DE LA CARTERA")
    logger.info("-" * 35)
    logger.info("Total invertido : %s", f"{coste_total:,.2f}")
    logger.info("Valor actual    : %s", f"{valor_actual:,.2f}")
    logger.info("Beneficio total : %s", f"{beneficio_total:,.2f}")


def eliminar_acciones(
    df: pd.DataFrame, acciones_a_excluir: Iterable[str], col_accion: str = "accion"
) -> pd.DataFrame:
    """
    Elimina del DataFrame las filas cuya acción esté en el listado indicado.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame origen
    acciones_a_excluir : Iterable[str]
        Lista / set / tupla de nombres de acciones a eliminar
    col_accion : str
        Nombre de la columna que identifica la acción (por defecto 'accion')

    Returns
    -------
    pd.DataFrame
        DataFrame filtrado (sin las acciones excluidas)
    """

    # Copia defensiva
    df_out = df.copy()

    if not acciones_a_excluir:
        return df_out

    # Normalizamos a set para búsquedas rápidas
    acciones_set = {str(a).strip() for a in acciones_a_excluir}

    # Filtrado
    df_out = df_out[~df_out[col_accion].astype(str).str.strip().isin(acciones_set)]

    return df_out


def insertar_posiciones_abiertas(df: pd.DataFrame):
    """
    Inserta un DataFrame en la tabla dbo.posiciones_abiertas usando SQLAlchemy
    Vacía la tabla antes de insertar
    """

    # 1️⃣ Vaciar la tabla
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM dbo.posiciones_abiertas"))

    # 2️⃣ Preparar dataframe para SQL
    df_sql = pd.DataFrame(
        {
            "id": df["ticker"],
            "accion": df["accion"],
            "numero_acciones": df["total_acciones"],
            "fecha_compra": datetime.now(),
            "valor_compra": df["precio_medio"],
            "comision_compra": df["total_comision"],
            "total_compra": df["coste_total"],
            "fecha_actual": datetime.now(),
            "valor_actual": df["ultimo_precio"],
            "total_actual": df["valor_actual"],
            "ultima_variacion": df["rendimiento_pct"],
        }
    )

    # 3️⃣ Insertar
    df_sql.to_sql(
        name="posiciones_abiertas",
        con=engine,
        schema="dbo",
        if_exists="append",
        index=False,
    )


def procesado_cartera_completo():
     # Paso 1: Obtención datos origen: ventas y compras
    compras = obtener_acciones_compras_df()
    ventas = obtener_acciones_ventas_df()

    # Paso 2: Procesamiento cartera
    # 2.1 Cálculo posiciones abiertas
    posiciones_abiertas = calcular_cartera_actual(compras, ventas)
    # 2.2 Agrupación por acción
    cartera = resumir_cartera_por_accion(posiciones_abiertas)
    # 2.3 Añadir tickers
    cartera_con_tikcer = anadir_ticker_desde_bd(cartera)

    # Paso 3: Obtención de cotizaciones
    precios, df_con_precios = obtener_ultimos_precios_cartera(cartera_con_tikcer)

    # Paso 4: Cálculo rendimiento
    df_final = calcular_rendimiento_y_ganancia_por_accion(df_con_precios)

    df_final = eliminar_acciones(df_final, ACCIONES_EXCLUIDAS)

    # Paso 5: Impresion de datos 
    imprimir_resumen_cartera(df_final)

    # Paso 6: Inserción BD posiciones abiertas
    insertar_posiciones_abiertas(df_final)