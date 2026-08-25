# Opción 4 — Obtener tipos de cambio

Este documento explica la lógica de la **Opción 4 ("Obtener tipos de cambio")** del menú principal de ThePyTrader.

## 1. Punto de entrada

Desde [src/main.py](../src/main.py), al seleccionar la opción `4` se invoca:

```python
procesado_tipos_cambio_completo()
```

definida en [src/services/price_manager.py:194](../src/services/price_manager.py#L194), importada en `main.py` desde `services.price_manager`.

El flujo tiene dos pasos: descargar el histórico diario de tipos de cambio EUR → divisa para cada divisa configurada, y persistirlo en base de datos.

```python
def procesado_tipos_cambio_completo():
    tipos_cambio_df = obtener_tipos_cambio_divisas()
    logger.debug("\n%s", tipos_cambio_df.head())
    insertar_tipos_cambio_en_bd(tipos_cambio_df)
```

## 2. Parametrización

Definidas en [src/parametrization.py](../src/parametrization.py):

- `TIPOS_CAMBIO_FECHA_INICIO` (por defecto `"01/01/2020"`, formato `dd/mm/aaaa`): fecha desde la cual se descarga el histórico.
- `LISTADO_DIVISAS` (por defecto `["USD", "GBP", "AUD"]`): divisas destino para las que se obtiene el tipo de cambio. La divisa origen es siempre EUR.

## 3. Paso 1 — Descarga de tipos de cambio

[`obtener_tipos_cambio_divisas`](../src/services/price_manager.py#L111) construye un DataFrame en formato largo con las columnas `fecha`, `divisa_origen`, `divisa_destino`, `tipo_cambio`:

1. Calcula `fecha_inicio` parseando `TIPOS_CAMBIO_FECHA_INICIO` (`%d/%m/%Y`) y `fecha_fin` como el día siguiente a hoy (el parámetro `end` de `yfinance` es exclusivo, por lo que se suma un día para incluir la cotización de hoy).
2. Para cada divisa de `LISTADO_DIVISAS`:
   - Construye el ticker de Yahoo Finance con el patrón `EUR{divisa}=X` (p. ej. `EURUSD=X`, `EURGBP=X`, `EURAUD=X`).
   - Descarga el histórico diario (`yf.download`, `interval="1d"`) entre `fecha_inicio` y `fecha_fin`.
   - Si no hay datos para esa divisa, registra un aviso (`logger.warning`) y continúa con la siguiente.
   - Renombra `Date`/`Close` a `fecha`/`tipo_cambio`, y añade `divisa_origen = "EUR"` y `divisa_destino = <divisa>`.
3. Concatena los resultados de todas las divisas en un único DataFrame. Si ninguna divisa obtuvo datos, devuelve un DataFrame vacío con las columnas esperadas.

## 4. Paso 2 — Persistencia en base de datos

[`insertar_tipos_cambio_en_bd`](../src/services/price_manager.py#L161) guarda el resultado en `dbo.historico_tipos_cambio`:

1. Si el DataFrame está vacío, registra un aviso y no hace nada.
2. **Vacía completamente la tabla** (`TRUNCATE TABLE dbo.historico_tipos_cambio`) — así la opción puede ejecutarse tantas veces como se quiera sin generar duplicados ni errores de clave.
3. Prepara el DataFrame de salida con las columnas `fecha`, `divisa_origen`, `divisa_destino`, `tipo_cambio` y `fecha_actualizacion` (fecha/hora de ejecución).
4. Inserta todo el resultado con `to_sql(..., if_exists="append")`.

## 5. Resumen del pipeline

```
main.py (opción "4")
  └─ procesado_tipos_cambio_completo()
       ├─ obtener_tipos_cambio_divisas()      ← Yahoo Finance (EUR{divisa}=X, por cada divisa de LISTADO_DIVISAS)
       └─ insertar_tipos_cambio_en_bd()        TRUNCATE + INSERT → dbo.historico_tipos_cambio
```

## 6. Tablas y fuentes de datos

| Tabla / fuente | Uso |
|---|---|
| Yahoo Finance (vía `yfinance`): `EUR{divisa}=X` | Lectura externa — histórico diario desde `TIPOS_CAMBIO_FECHA_INICIO` hasta hoy |
| `dbo.historico_tipos_cambio` | Escritura — se vacía (`TRUNCATE`) y se reinserta por completo en cada ejecución |

## 7. Notas relevantes

- A diferencia de la Opción 2 (metales), que hace un `MERGE` (upsert) incremental, aquí se opta por vaciar y recargar toda la tabla en cada ejecución — más simple y evita duplicados sin depender de restricciones de unicidad en la tabla.
- Si una divisa concreta no devuelve datos de Yahoo Finance, se omite sin abortar el resto del proceso.
