# Opción 5 — Obtener histórico cotizaciones

Este documento explica la lógica de la **Opción 5 ("Obtener histórico cotizaciones")** del menú principal de ThePyTrader.

## 1. Punto de entrada

Desde [src/main.py](../src/main.py), al seleccionar la opción `5` se invoca:

```python
procesado_historico_cotizaciones_completo()
```

definida en [src/services/price_manager.py](../src/services/price_manager.py), importada en `main.py` desde `services.price_manager`.

El flujo obtiene el listado de acciones distintas registradas en `acciones_compras` y descarga, **acción por acción**, su histórico diario de cotización desde una fecha configurada hasta hoy, insertando cada resultado en base de datos inmediatamente para minimizar la pérdida de datos en caso de fallo.

## 2. Parametrización

Definida en [src/parametrization.py](../src/parametrization.py):

- `COTIZACIONES_FECHA_INICIO` (por defecto `"01/01/2020"`, formato `dd/mm/aaaa`): fecha desde la cual se descarga el histórico de cotizaciones de cada acción.

## 3. Paso 1 — Obtención de acciones distintas

[`obtener_acciones_compra_distintas`](../src/services/price_manager.py) construye un DataFrame con las columnas `accion`, `divisa`, `ticker`:

1. Lee todo el contenido de `dbo.acciones_compras`.
2. Si la tabla tiene columna `divisa`, agrupa por `accion` y toma el primer valor de `divisa` de cada grupo. Si no existe dicha columna, toma las acciones únicas y asigna `divisa = "EUR"` por defecto.
3. Lee el mapeo `accion → ticker` desde `dbo.info_acciones_base` y lo añade como nueva columna mediante `.map()`.
4. Si alguna acción no tiene ticker asociado, se registra un aviso (`logger.warning`) — esas acciones se omitirán más adelante, ya que sin ticker no se puede consultar Yahoo Finance.

## 4. Paso 2 — Vaciado de la tabla destino

Antes de procesar ninguna acción, `procesado_historico_cotizaciones_completo` vacía completamente la tabla:

```sql
TRUNCATE TABLE dbo.historico_cotizaciones_acciones
```

Esto se hace **una única vez al principio** (no en cada acción), de forma que la opción pueda re-ejecutarse tantas veces como se quiera sin duplicados, y a la vez las inserciones posteriores (una por acción) vayan acumulándose sobre una tabla limpia.

## 5. Paso 3 — Descarga e inserción acción por acción

Para cada fila de `accion`/`divisa`/`ticker` obtenida en el paso 1:

1. Si la acción no tiene `ticker`, se omite con un aviso.
2. Se descarga el histórico diario con `yfinance` (`yf.download(ticker, start=COTIZACIONES_FECHA_INICIO, end=hoy+1, interval="1d")`).
3. Si no hay datos, se registra un aviso y se pasa a la siguiente acción.
4. Se renombran las columnas `Date`/`Close` a `fecha`/`valor_accion`, y se añaden las columnas `accion`, `ticker` y `divisa`.
5. Se inserta inmediatamente el resultado de esa acción en `dbo.historico_cotizaciones_acciones` mediante [`insertar_historico_cotizaciones_en_bd`](../src/services/price_manager.py) (`to_sql(..., if_exists="append")`).
6. Se registra por log el número de registros insertados para esa acción.

Todo el bloque de descarga + inserción de cada acción está envuelto en un `try/except`: si falla (p. ej. error de red, ticker inválido en Yahoo Finance, error de inserción), se registra la excepción con `logger.exception` y **se continúa con la siguiente acción**, en vez de abortar todo el proceso. De esta forma, un fallo puntual en una acción no impide que se carguen el resto.

## 6. Resumen del pipeline

```
main.py (opción "5")
  └─ procesado_historico_cotizaciones_completo()
       ├─ obtener_acciones_compra_distintas()        ← dbo.acciones_compras + dbo.info_acciones_base
       ├─ TRUNCATE TABLE dbo.historico_cotizaciones_acciones   (una sola vez)
       └─ por cada acción (con try/except independiente):
              ├─ yf.download(ticker, start=COTIZACIONES_FECHA_INICIO, end=hoy+1)   ← Yahoo Finance
              └─ insertar_historico_cotizaciones_en_bd()        INSERT → dbo.historico_cotizaciones_acciones
```

## 7. Tablas y fuentes de datos

| Tabla / fuente | Uso |
|---|---|
| `dbo.acciones_compras` | Lectura — listado de acciones distintas (y divisa) |
| `dbo.info_acciones_base` | Lectura — mapeo `accion → ticker` |
| Yahoo Finance (vía `yfinance`) | Lectura externa — histórico diario de cotización por ticker, desde `COTIZACIONES_FECHA_INICIO` hasta hoy |
| `dbo.historico_cotizaciones_acciones` | Escritura — se vacía una vez al inicio y se inserta acción por acción |

## 8. Notas relevantes

- El procesamiento acción a acción (en vez de acumular todo en memoria e insertar al final) es deliberado: si el proceso falla a mitad de ejecución, las acciones ya procesadas quedan insertadas en la tabla.
- La tabla `dbo.historico_cotizaciones_acciones` tiene una restricción `UNIQUE (fecha, accion)`; al truncarse la tabla una sola vez al principio de cada ejecución, no se generan conflictos de duplicados dentro de la misma ejecución.
- Las acciones sin ticker asociado en `dbo.info_acciones_base` se omiten automáticamente, ya que no es posible consultar su cotización en Yahoo Finance sin un ticker válido.
