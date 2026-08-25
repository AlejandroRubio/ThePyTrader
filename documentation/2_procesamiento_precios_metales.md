# Opción 2 — Procesamiento precios metales

Este documento explica la lógica de la **Opción 2 ("Procesamiento precios metales")** del menú principal de ThePyTrader.

## 1. Punto de entrada

Desde [src/main.py](../src/main.py), al seleccionar la opción `2` se invoca:

```python
procesado_metales_completo()
```

definida en [src/services/metal_manager.py:160](../src/services/metal_manager.py#L160), importada en `main.py` desde `services.metal_manager`.

El flujo tiene dos pasos: descargar el histórico de precios de metales (oro, plata, cobre) convertidos a euros/onza troy, e insertarlos/actualizarlos en base de datos.

```python
def procesado_metales_completo():
    metales_df = obtener_metales_eur_oz_3y()
    logger.debug("\n%s", metales_df.head())
    logger.debug("\n%s", metales_df.tail())
    insertar_metales_en_bd(metales_df)
```

## 2. Paso 1 — Descarga y conversión de precios

[`obtener_metales_eur_oz_3y`](../src/services/metal_manager.py#L14) construye un DataFrame diario con las columnas `fecha`, `oro_eur_oz`, `plata_eur_oz`, `cobre_eur_oz`. A pesar de lo que indica el nombre de la función ("3y"), en el código actual descarga **6 años** de histórico (`period="6y"`).

### 2.1 Tickers de origen (Yahoo Finance)

| Metal | Ticker Yahoo Finance |
|---|---|
| Oro | `GC=F` |
| Plata | `SI=F` |
| Cobre | `HG=F` |

### 2.2 Tipo de cambio

Se descarga primero el histórico del par `EURUSD=X` (`yf.download`, `period="6y"`, `interval="1d"`) y se queda con las columnas `Date`/`Close`, renombradas a `fecha`/`eurusd`. Este DataFrame se usa para convertir todos los precios (cotizados en USD) a EUR.

### 2.3 Descarga y conversión por metal

Para cada metal (`oro`, `plata`, `cobre`):

1. Descarga el histórico diario del ticker correspondiente (`auto_adjust=False`).
2. Si no hay datos, registra un aviso (`logger.warning`) y pasa al siguiente metal.
3. Renombra `Date`/`Close` a `fecha`/`precio_usd`.
4. Hace un `merge` (left join) con el DataFrame de tipo de cambio por `fecha`.
5. Convierte a euros: `precio_eur = precio_usd / eurusd`.
6. **Ajuste específico para el cobre**: el ticker `HG=F` cotiza en USD por libra (pound), no por onza troy, así que se aplica una conversión adicional:
   `precio_eur = precio_eur / 14.5833` (1 onza troy ≈ 14.5833 libras... realmente el factor convierte USD/libra a USD/onza troy, dado que 1 libra = 1/14.5833 onzas troy).
7. Se queda solo con `fecha` y el precio en euros, renombrando la columna a `{metal}_eur_oz` (p. ej. `oro_eur_oz`).

### 2.4 Combinación final

Las series de los tres metales se combinan con `merge(..., how="outer")` sobre `fecha`, de forma que el DataFrame resultante tiene una fila por cada fecha en la que hay cotización de al menos uno de los tres metales. Se ordena por `fecha` ascendente y se devuelve.

Si ningún metal tiene datos, la función devuelve un `DataFrame` vacío.

## 3. Paso 2 — Persistencia en base de datos (UPSERT)

[`insertar_metales_en_bd(df)`](../src/services/metal_manager.py#L94) guarda el resultado en la tabla `dbo.metales_cotizacion`:

1. **Transforma el DataFrame a formato largo** (`pd.melt`), pasando de columnas `oro_eur_oz`/`plata_eur_oz`/`cobre_eur_oz` a filas con columnas `fecha`, `metal`, `precio` (una fila por combinación fecha+metal).
2. Limpia el nombre del metal quitando el sufijo `_eur_oz` (p. ej. `oro_eur_oz` → `oro`).
3. Añade columnas fijas: `divisa = "EUR"`, `unidad = "oz_troy"`, `fecha_carga = date.today()`.
4. Elimina filas sin precio (`dropna(subset=["precio"])`).
5. Normaliza `fecha` a tipo `date`.
6. Ejecuta un **`MERGE`** SQL (`dbo.metales_cotizacion`) fila a fila, usando como clave de emparejamiento `(fecha, metal)`:
   - Si ya existe una fila con esa `fecha` y `metal` → **actualiza** `precio`, `divisa`, `unidad`, `fecha_carga`.
   - Si no existe → **inserta** una fila nueva.

Es decir, a diferencia del procesamiento de cartera (que borra y reinserta toda la tabla), aquí se hace un **upsert incremental** que preserva el histórico y solo actualiza/añade las fechas relevantes.

## 4. Resumen del pipeline

```
main.py (opción "2")
  └─ procesado_metales_completo()
       ├─ obtener_metales_eur_oz_3y()          ← Yahoo Finance (EURUSD=X, GC=F, SI=F, HG=F)
       │      convierte USD→EUR y, para el cobre, USD/libra→USD/oz troy
       └─ insertar_metales_en_bd()              MERGE (UPSERT) → dbo.metales_cotizacion
```

## 5. Tablas y fuentes de datos

| Tabla / fuente | Uso |
|---|---|
| Yahoo Finance (vía `yfinance`): `EURUSD=X`, `GC=F`, `SI=F`, `HG=F` | Lectura externa — histórico diario de 6 años |
| `dbo.metales_cotizacion` | Escritura — UPSERT (`MERGE`) por `(fecha, metal)`. Definición en [database/dbo.metales_cotizacion.Table.sql](../database/dbo.metales_cotizacion.Table.sql) |

## 6. Notas relevantes

- El nombre de la función `obtener_metales_eur_oz_3y` es engañoso: actualmente descarga `period="6y"`, no 3 años.
- Si Yahoo Finance no devuelve datos para algún metal, ese metal simplemente se omite del resultado (no aborta el resto del proceso).
- El factor de conversión del cobre (`/ 14.5833`) asume que el ticker `HG=F` cotiza en USD por libra y lo convierte a USD por onza troy antes de pasarlo a euros.
