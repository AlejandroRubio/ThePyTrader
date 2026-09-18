"""Ajustes de entorno que deben ejecutarse antes de usar yfinance.

Se importa lo primero en main.py. Redirige las cachés de yfinance
(tz / cookies / ISIN, todas SQLite) a una carpeta propia y escribible,
evitando dos fallos habituales al ejecutar el .exe en Windows:

  * "Error creating TzCache folder ... [WinError 183]" cuando en
    %LOCALAPPDATA% ya existe un ARCHIVO llamado 'py-yfinance'
    (o la carpeta por defecto no es escribible).
  * Cachés corruptas heredadas de ejecuciones anteriores.
"""
import os
import tempfile


def _preparar_certificados_ssl() -> None:
    """Apunta curl_cffi / OpenSSL al bundle de CAs de certifi.

    En el .exe congelado curl_cffi no siempre localiza el cacert.pem,
    provocando 'SSL certificate problem: unable to get local issuer
    certificate'. Fijar estas variables lo resuelve.
    """
    try:
        import certifi
    except Exception:
        return
    ca = certifi.where()
    if ca and os.path.isfile(ca):
        os.environ.setdefault("CURL_CA_BUNDLE", ca)
        os.environ.setdefault("SSL_CERT_FILE", ca)
        os.environ.setdefault("REQUESTS_CA_BUNDLE", ca)


def _preparar_cache_yfinance() -> None:
    try:
        import yfinance as yf
    except Exception:
        return

    base = os.environ.get("LOCALAPPDATA") or tempfile.gettempdir()
    cache_dir = os.path.join(base, "ThePyTrader", "yf-cache")

    # yfinance hace os.makedirs sin exist_ok: si existe un archivo con ese
    # nombre (no una carpeta) revienta. Lo limpiamos preventivamente.
    for ruta in (os.path.join(base, "py-yfinance"), cache_dir):
        if os.path.isfile(ruta):
            try:
                os.remove(ruta)
            except OSError:
                pass

    try:
        os.makedirs(cache_dir, exist_ok=True)
        yf.set_tz_cache_location(cache_dir)
    except Exception:
        # Si algo falla, yfinance seguirá funcionando sin caché de zona horaria.
        pass


_preparar_certificados_ssl()
_preparar_cache_yfinance()
