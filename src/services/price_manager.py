import pandas as pd
import yfinance as yf


import pandas as pd
import yfinance as yf


def obtener_ultimos_precios_cartera(
    df: pd.DataFrame,
    col_ticker: str = "ticker",
    devolver_df: bool = True
) -> tuple[dict[str, float | None], pd.DataFrame | None]:
    """
    Obtiene el último precio de cotización para todos los tickers presentes
    en el DataFrame, haciendo UNA sola llamada a yfinance.

    Cambios clave para evitar NaN:
    - Usa period="5d" en vez de "1d"
    - Coge el último Close NO nulo disponible

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame de cartera (debe contener la columna `col_ticker`)
    col_ticker : str
        Nombre de la columna que contiene el ticker (por defecto 'ticker')
    devolver_df : bool
        Si True, devuelve también el DataFrame con una columna 'ultimo_precio'

    Returns
    -------
    (precios_por_ticker, df_out)
        precios_por_ticker: dict[str, float|None]
        df_out: DataFrame con columna 'ultimo_precio' o None
    """

    # 1) Lista única de tickers válidos
    tickers = (
        df[col_ticker]
        .dropna()
        .astype(str)
        .str.strip()
        .replace("", pd.NA)
        .dropna()
        .unique()
        .tolist()
    )

    if not tickers:
        precios = {}
        df_out = df.copy() if devolver_df else None
        if devolver_df:
            df_out["ultimo_precio"] = None
        return precios, df_out

    # 2) UNA sola llamada a yfinance (5 días para evitar mercado cerrado / festivos / horarios)
    data = yf.download(
        tickers=" ".join(tickers),
        period="5d",
        interval="1d",
        group_by="ticker",
        threads=True,
        progress=False,
        auto_adjust=True  # suele dar menos problemas con Yahoo
    )

    # 3) Extraer último Close válido por ticker
    precios: dict[str, float | None] = {}

    # Caso especial: un solo ticker (yfinance no anida columnas por ticker)
    if len(tickers) == 1:
        t = tickers[0]
        try:
            closes = data["Close"].dropna()
            precios[t] = float(closes.iloc[-1]) if not closes.empty else None
        except Exception:
            precios[t] = None
    else:
        for t in tickers:
            try:
                closes = data[t]["Close"].dropna()
                precios[t] = float(closes.iloc[-1]) if not closes.empty else None
            except Exception:
                precios[t] = None

    # 4) (Opcional) Añadir columna al DataFrame
    df_out = None
    if devolver_df:
        df_out = df.copy()
        df_out["ultimo_precio"] = df_out[col_ticker].map(precios)

    return precios, df_out


def test_yfnance():
    # Crear objeto del ticker
    ticker = yf.Ticker("SAN.MC")
    # Obtener histórico de últimos 5 días
    hist = ticker.history(period="5d")
    print(hist)