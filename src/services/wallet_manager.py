from datetime import date, datetime
import pandas as pd
import numpy as np
from sqlalchemy import text
from typing import Iterable
from services.db_manager import get_database_engine
from services.price_manager import obtener_ultimos_precios_cartera
from parametrization import ACCIONES_EXCLUIDAS, COTIZACIONES_FECHA_INICIO
from logger import get_logger

logger = get_logger(__name__)

engine = get_database_engine()


def _ajustar_valor_libras(df: pd.DataFrame) -> pd.DataFrame:
    if "divisa" not in df.columns or "valor_accion" not in df.columns:
        return df
    mask = df["divisa"] == "Libra"
    if mask.any():
        df = df.copy()
        df.loc[mask, "valor_accion"] /= 100
        logger.info("Ajustados %d registros de Libra (peniques → libras)", mask.sum())
    return df


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
        df = _ajustar_valor_libras(df)
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
    Devuelve un DataFrame con todo el contenido de dbo.acciones_ventas.
    """
    conn = None

    query = """
        SELECT * FROM dbo.acciones_ventas
    """

    try:
        df = pd.read_sql(query, engine)
        df = _ajustar_valor_libras(df)
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

    # Agrupar ventas por acción y broker (el FIFO debe ser independiente por
    # broker, ya que una misma acción puede tener posiciones distintas en
    # brokers distintos)
    ventas_grouped = df_ventas.groupby(["accion", "broker"])["numero_acciones"].sum()

    # Copia del dataframe de compras para restar
    cartera = df_compras.copy()

    # Procesar restas por acción y broker
    for (accion, broker), total_vendido in ventas_grouped.items():
        # Filtrar compras de esa acción en ese broker
        mask = (cartera["accion"] == accion) & (cartera["broker"] == broker)
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
            accion ,
            ticker 
        FROM dbo.info_acciones_base
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


def detalle_operaciones_por_accion(nombre_accion: str, broker: str) -> None:
    """
    Muestra el detalle FIFO de las ventas de una acción en un broker concreto,
    indicando qué lotes de compra (posiblemente de varias fechas) cubren cada
    venta, con la comisión de compra prorrateada según el bloque de acciones
    consumido.

    Se exige el broker como criterio de búsqueda porque la misma acción puede
    tener posiciones independientes (y por tanto FIFO independientes) en
    distintos brokers.
    """
    compras = obtener_acciones_compras_df()
    ventas = obtener_acciones_ventas_df()

    if compras is None or ventas is None:
        logger.info("No se pudieron obtener los datos de compras/ventas. Abortando.")
        return

    nombre_normalizado = nombre_accion.strip().casefold()
    broker_normalizado = broker.strip().casefold()

    filtro_compras = (compras["accion"].str.strip().str.casefold() == nombre_normalizado) & (
        compras["broker"].str.strip().str.casefold() == broker_normalizado
    )
    filtro_ventas = (ventas["accion"].str.strip().str.casefold() == nombre_normalizado) & (
        ventas["broker"].str.strip().str.casefold() == broker_normalizado
    )
    lotes = compras[filtro_compras].copy()
    ventas_broker = ventas[filtro_ventas].copy()

    if lotes.empty and ventas_broker.empty:
        logger.info(f"No hay operaciones registradas para la acción '{nombre_accion}' en el broker '{broker}'.")
        return

    for col in ("numero_acciones", "valor_accion", "comision"):
        lotes[col] = pd.to_numeric(lotes[col], errors="coerce")
        ventas_broker[col] = pd.to_numeric(ventas_broker[col], errors="coerce")

    lotes = lotes.sort_values("fecha")
    lotes["acciones_restantes"] = lotes["numero_acciones"]
    ventas_broker = ventas_broker.sort_values("fecha")

    logger.info(f"=== {nombre_accion} - Broker: {broker} ===")

    if ventas_broker.empty:
        logger.info("Sin ventas registradas para este broker.")
        return

    for num_venta, (_, venta) in enumerate(ventas_broker.iterrows(), start=1):
        acciones_pendientes = venta["numero_acciones"]

        logger.info(
            f"Venta {num_venta}: fecha_venta={venta['fecha']}, "
            f"acciones_vendidas={venta['numero_acciones']}, valor_venta={venta['valor_accion']}, "
            f"comision_venta={venta['comision']}, broker={broker}"
        )
        logger.info("  id_compra, fecha_compra, acciones_asignadas, valor_compra, comision_compra")

        for idx, lote in lotes.iterrows():
            if acciones_pendientes <= 0:
                break
            if lote["acciones_restantes"] <= 0:
                continue

            asignadas = min(lote["acciones_restantes"], acciones_pendientes)
            proporcion = asignadas / lote["numero_acciones"]
            comision_asignada = lote["comision"] * proporcion

            logger.info(
                f"  {lote['id']}, {lote['fecha']}, {asignadas}, "
                f"{lote['valor_accion']}, {round(comision_asignada, 2)}"
            )

            lotes.at[idx, "acciones_restantes"] -= asignadas
            acciones_pendientes -= asignadas

        if acciones_pendientes > 0:
            logger.info(
                f"Venta {num_venta} de '{nombre_accion}' ({broker}) no puede cubrirse "
                f"completamente: faltan {acciones_pendientes} acciones de compra"
            )


def procesado_cartera_completo():
     # Paso 1: Obtención datos origen: ventas y compras
    compras = obtener_acciones_compras_df()
    ventas = obtener_acciones_ventas_df()

    if compras is None or ventas is None:
        logger.error("No se pudieron obtener los datos de compras/ventas. Abortando.")
        return

    # Paso 2: Procesamiento cartera
    # 2.1 Cálculo posiciones abiertas
    posiciones_abiertas = calcular_cartera_actual(compras, ventas)
    # 2.2 Agrupación por acción
    cartera = resumir_cartera_por_accion(posiciones_abiertas)
    # 2.3 Añadir tickers
    cartera_con_ticker = anadir_ticker_desde_bd(cartera)

    # Paso 3: Obtención de cotizaciones
    precios, df_con_precios = obtener_ultimos_precios_cartera(cartera_con_ticker)

    # Paso 4: Cálculo rendimiento
    df_final = calcular_rendimiento_y_ganancia_por_accion(df_con_precios)

    df_final = eliminar_acciones(df_final, ACCIONES_EXCLUIDAS)

    # Paso 5: Impresion de datos 
    imprimir_resumen_cartera(df_final)

    # Paso 6: Inserción BD posiciones abiertas
    insertar_posiciones_abiertas(df_final)


def obtener_acciones_compras_euro_df() -> pd.DataFrame | None:
    """
    Devuelve las compras de acciones con el valor ya convertido a EUR
    (vista dbo.acciones_compras_euro).
    """
    try:
        df = pd.read_sql("SELECT * FROM dbo.acciones_compras_euro", engine)
        logger.info("Obtenidas un total de %d compras (EUR)", len(df))
        return df
    except Exception:
        logger.exception("Error durante la conexión o la consulta")
        return None


def obtener_acciones_ventas_euro_df() -> pd.DataFrame | None:
    """
    Devuelve las ventas de acciones con el valor ya convertido a EUR
    (vista dbo.acciones_venta_euro).
    """
    try:
        df = pd.read_sql("SELECT * FROM dbo.acciones_venta_euro", engine)
        logger.info("Obtenidas un total de %d ventas (EUR)", len(df))
        return df
    except Exception:
        logger.exception("Error durante la conexión o la consulta")
        return None


def obtener_historico_cotizaciones_euro_df() -> pd.DataFrame | None:
    """
    Devuelve el histórico de cotizaciones de acciones con el valor ya
    convertido a EUR (vista dbo.historico_cotizaciones_acciones_euro).
    """
    try:
        df = pd.read_sql(
            "SELECT fecha, accion, valor_accion FROM dbo.historico_cotizaciones_acciones_euro",
            engine,
        )
        logger.info("Obtenidas un total de %d cotizaciones históricas (EUR)", len(df))
        return df
    except Exception:
        logger.exception("Error durante la conexión o la consulta")
        return None


def _construir_ledger_fifo(df_compras: pd.DataFrame, df_ventas: pd.DataFrame):
    """
    Recorre en orden cronológico las compras y ventas de cada (accion, broker)
    aplicando FIFO y construye dos estructuras:

    - lotes: una fila por lote de compra (accion, broker, fecha_compra,
      numero_acciones original, valor_accion, comision).
    - eventos_consumo: una fila por cada asignación de un lote de compra a
      una venta, con las acciones consumidas, la fecha de venta, el valor de
      venta y las comisiones (compra y venta) prorrateadas según el bloque
      de acciones consumido.

    A partir de estas dos estructuras se puede reconstruir el estado de la
    cartera (posiciones abiertas y beneficio realizado) en cualquier fecha
    pasada, considerando únicamente los eventos con fecha_venta anterior o
    igual a la fecha de referencia.
    """
    compras = df_compras.sort_values("fecha").reset_index(drop=True).copy()
    ventas = df_ventas.sort_values("fecha").reset_index(drop=True).copy()

    compras["lote_id"] = compras.index
    compras["acciones_restantes"] = compras["numero_acciones"]

    lotes = compras[
        ["lote_id", "accion", "broker", "fecha", "numero_acciones", "valor_accion", "comision"]
    ].rename(columns={"fecha": "fecha_compra"})

    eventos = []

    for (accion, broker), ventas_grupo in ventas.groupby(["accion", "broker"]):
        mask = (compras["accion"] == accion) & (compras["broker"] == broker)
        lotes_grupo = compras[mask]

        for _, venta in ventas_grupo.iterrows():
            acciones_pendientes = venta["numero_acciones"]

            for idx in lotes_grupo.index:
                if acciones_pendientes <= 0:
                    break

                restantes = compras.at[idx, "acciones_restantes"]
                if restantes <= 0:
                    continue

                asignadas = min(restantes, acciones_pendientes)
                proporcion_compra = asignadas / compras.at[idx, "numero_acciones"]
                proporcion_venta = asignadas / venta["numero_acciones"]

                eventos.append(
                    {
                        "accion": accion,
                        "broker": broker,
                        "lote_id": compras.at[idx, "lote_id"],
                        "fecha_compra": compras.at[idx, "fecha"],
                        "valor_compra": compras.at[idx, "valor_accion"],
                        "fecha_venta": venta["fecha"],
                        "valor_venta": venta["valor_accion"],
                        "acciones": asignadas,
                        "comision_compra": compras.at[idx, "comision"] * proporcion_compra,
                        "comision_venta": venta["comision"] * proporcion_venta,
                    }
                )

                compras.at[idx, "acciones_restantes"] -= asignadas
                acciones_pendientes -= asignadas

    eventos_consumo = pd.DataFrame(
        eventos,
        columns=[
            "accion", "broker", "lote_id", "fecha_compra", "valor_compra",
            "fecha_venta", "valor_venta", "acciones", "comision_compra", "comision_venta",
        ],
    )

    return lotes, eventos_consumo


def _serie_acumulada_por_fecha(altas: pd.Series, bajas: pd.Series, fechas: pd.DatetimeIndex) -> pd.Series:
    """
    Combina altas (+) y bajas (-) indexadas por fecha en una única serie de
    deltas diarios, calcula el acumulado cronológico y lo proyecta sobre el
    rango completo de `fechas` (relleno hacia delante, empezando en 0).
    """
    deltas = altas.add(-bajas, fill_value=0.0) if not bajas.empty else altas
    deltas.index = pd.to_datetime(deltas.index).normalize()
    deltas = deltas.groupby(deltas.index).sum().sort_index()

    acumulado = deltas.cumsum()
    return acumulado.reindex(fechas, method="ffill").fillna(0.0)


def _calcular_serie_invertido(lotes: pd.DataFrame, eventos: pd.DataFrame, fechas: pd.DatetimeIndex) -> pd.Series:
    altas = (lotes["numero_acciones"] * lotes["valor_accion"]).groupby(lotes["fecha_compra"]).sum()
    if eventos.empty:
        bajas = pd.Series(dtype=float)
    else:
        bajas = (eventos["acciones"] * eventos["valor_compra"]).groupby(eventos["fecha_venta"]).sum()

    return _serie_acumulada_por_fecha(altas, bajas, fechas)


def _calcular_serie_beneficio_liquidado(eventos: pd.DataFrame, fechas: pd.DatetimeIndex) -> pd.Series:
    if eventos.empty:
        return pd.Series(0.0, index=fechas)

    beneficio = (
        (eventos["valor_venta"] - eventos["valor_compra"]) * eventos["acciones"]
        - eventos["comision_compra"]
        - eventos["comision_venta"]
    )
    altas = beneficio.groupby(eventos["fecha_venta"]).sum()
    return _serie_acumulada_por_fecha(altas, pd.Series(dtype=float), fechas)


def _calcular_serie_valorado(
    lotes: pd.DataFrame, eventos: pd.DataFrame, df_cotizaciones: pd.DataFrame, fechas: pd.DatetimeIndex
) -> pd.Series:
    """
    Para cada acción, valora las acciones en cartera día a día a su precio de
    mercado (histórico, con forward-fill para días sin cotización). Si un día
    no tiene ninguna cotización disponible (ni siquiera anterior), se usa como
    valor de repuesto el precio medio de compra de las acciones que se tienen
    en cartera ese día, de forma que la valoración nunca caiga a 0 solo por
    falta de datos de mercado.
    """
    acciones = set(lotes["accion"].unique())

    precios = df_cotizaciones.copy()
    precios["fecha"] = pd.to_datetime(precios["fecha"]).dt.normalize()

    total = pd.Series(0.0, index=fechas)

    for accion in acciones:
        lotes_accion = lotes.loc[lotes["accion"] == accion]
        eventos_accion = eventos.loc[eventos["accion"] == accion] if not eventos.empty else eventos

        altas_acciones = lotes_accion.groupby("fecha_compra")["numero_acciones"].sum()
        altas_coste = (
            (lotes_accion["numero_acciones"] * lotes_accion["valor_accion"])
            .groupby(lotes_accion["fecha_compra"])
            .sum()
        )

        if eventos_accion.empty:
            bajas_acciones = pd.Series(dtype=float)
            bajas_coste = pd.Series(dtype=float)
        else:
            bajas_acciones = eventos_accion.groupby("fecha_venta")["acciones"].sum()
            bajas_coste = (
                (eventos_accion["acciones"] * eventos_accion["valor_compra"])
                .groupby(eventos_accion["fecha_venta"])
                .sum()
            )

        acciones_en_cartera = _serie_acumulada_por_fecha(altas_acciones, bajas_acciones, fechas)
        coste_en_cartera = _serie_acumulada_por_fecha(altas_coste, bajas_coste, fechas)

        precio_medio_compra = (coste_en_cartera / acciones_en_cartera).replace(
            [np.inf, -np.inf], 0.0
        ).fillna(0.0)

        precio_mercado = (
            precios.loc[precios["accion"] == accion]
            .drop_duplicates("fecha")
            .set_index("fecha")["valor_accion"]
            .reindex(fechas, method="ffill")
        )

        if precio_mercado.isna().all() and acciones_en_cartera.abs().sum() > 0:
            logger.warning(
                "Sin cotizaciones históricas para '%s': se usará el precio medio de compra como valor de mercado",
                accion,
            )

        precio_final = precio_mercado.fillna(precio_medio_compra)

        total += acciones_en_cartera * precio_final

    return total


def calcular_historico_cartera() -> pd.DataFrame:
    """
    Calcula, para cada día desde COTIZACIONES_FECHA_INICIO hasta hoy:
    - total_invertido: coste de compra de las posiciones abiertas ese día
    - total_valorado: valor de mercado (cotización histórica) de esas posiciones
    - total_beneficio_liquidado: beneficio/pérdida acumulado de las ventas realizadas hasta ese día
    - total_beneficio_no_liquidado: total_valorado - total_invertido
    """
    compras = obtener_acciones_compras_euro_df()
    ventas = obtener_acciones_ventas_euro_df()
    cotizaciones = obtener_historico_cotizaciones_euro_df()

    if compras is None or ventas is None or cotizaciones is None:
        logger.error("No se pudieron obtener los datos necesarios. Abortando.")
        return pd.DataFrame()

    lotes, eventos = _construir_ledger_fifo(compras, ventas)

    fecha_inicio = datetime.strptime(COTIZACIONES_FECHA_INICIO, "%d/%m/%Y").date()
    fecha_fin = date.today()
    fechas = pd.date_range(start=fecha_inicio, end=fecha_fin, freq="D")

    total_invertido = _calcular_serie_invertido(lotes, eventos, fechas)
    total_beneficio_liquidado = _calcular_serie_beneficio_liquidado(eventos, fechas)
    total_valorado = _calcular_serie_valorado(lotes, eventos, cotizaciones, fechas)
    total_beneficio_no_liquidado = total_valorado - total_invertido

    return pd.DataFrame(
        {
            "fecha": fechas,
            "total_invertido": total_invertido.values,
            "total_valorado": total_valorado.values,
            "total_beneficio_liquidado": total_beneficio_liquidado.values,
            "total_beneficio_no_liquidado": total_beneficio_no_liquidado.values,
        }
    )


def insertar_historico_cartera_en_bd(df: pd.DataFrame):
    """
    Inserta el histórico diario de rendimiento de la cartera en
    dbo.historico_rendimientos. Vacía la tabla antes de insertar.
    """

    if df.empty:
        logger.warning("No hay histórico de cartera que insertar")
        return

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE dbo.historico_rendimientos"))

    df.to_sql(
        name="historico_rendimientos",
        con=engine,
        schema="dbo",
        if_exists="append",
        index=False,
    )

    logger.info("Insertados %d registros de histórico de cartera", len(df))


def procesado_historico_cartera_completo():
    historico = calcular_historico_cartera()
    insertar_historico_cartera_en_bd(historico)