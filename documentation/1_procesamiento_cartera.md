# Opción 1 — Procesamiento cartera

Este documento explica la lógica de la **Opción 1 ("Procesamiento cartera")** del menú principal de ThePyTrader.

## 1. Punto de entrada

El menú principal se muestra en [src/main.py](../src/main.py) (función `mostrar_menu`). Al seleccionar la opción `1`, se invoca:

```python
procesado_cartera_completo()
```

definida en [src/services/wallet_manager.py:466](../src/services/wallet_manager.py#L466), importada en `main.py` desde `services.wallet_manager`.

Esta función orquesta un pipeline de 6 pasos: obtiene compras y ventas, calcula la cartera actual mediante FIFO, la enriquece con tickers y cotizaciones, calcula el rendimiento, imprime un resumen y persiste el resultado en base de datos.

## 2. Paso 1 — Obtención de datos origen

```python
compras = obtener_acciones_compras_df()
ventas = obtener_acciones_ventas_df()
```

- [obtener_acciones_compras_df](../src/services/wallet_manager.py#L27) ejecuta `SELECT * FROM dbo.acciones_compras`.
- [obtener_acciones_ventas_df](../src/services/wallet_manager.py#L52) ejecuta `SELECT * FROM dbo.acciones_ventas`.

Ambas usan `pd.read_sql` sobre el engine SQLAlchemy compartido (`get_database_engine()`, definido en [src/services/db_manager.py](../src/services/db_manager.py), con la cadena de conexión parametrizada en [src/parametrization.py](../src/parametrization.py)).

A cada DataFrame se le aplica [`_ajustar_valor_libras`](../src/services/wallet_manager.py#L16): si la columna `divisa` vale `"Libra"`, se divide `valor_accion` entre 100 (conversión de peniques a libras).

Si cualquiera de las dos consultas falla (devuelve `None`), `procesado_cartera_completo` registra un error y aborta sin continuar.

## 3. Paso 2 — Procesamiento de la cartera

### 3.1 Cálculo de posiciones abiertas (algoritmo FIFO)

[`calcular_cartera_actual(compras, ventas)`](../src/services/wallet_manager.py#L77) determina qué lotes de compra siguen "vivos" tras descontar las ventas, aplicando **FIFO (primero en entrar, primero en salir)**:

1. Ordena compras y ventas por `fecha` ascendente — imprescindible para que el consumo sea cronológico.
2. Agrupa las ventas por **`(accion, broker)`** sumando `numero_acciones`. Se agrupa también por broker porque una misma acción puede tener posiciones independientes en brokers distintos.
3. Para cada combinación `(accion, broker)`, recorre los lotes de compra correspondientes en orden de fecha y va restando las acciones vendidas:
   - Si el lote tiene menos (o igual número de) acciones que lo que queda por vender, el lote se consume por completo (`numero_acciones = 0`).
   - Si el lote tiene más acciones que lo pendiente, solo se resta la cantidad necesaria y el lote queda parcialmente vivo.
4. Al final se conservan solo los lotes con `numero_acciones > 0`: estas son las **posiciones abiertas**.

### 3.2 Agrupación por acción — precio medio ponderado

[`resumir_cartera_por_accion`](../src/services/wallet_manager.py#L114) agrupa las posiciones abiertas **solo por `accion`** (combinando aquí los distintos brokers) y calcula:

- `total_acciones` = suma de `numero_acciones`
- `total_comision` = suma de `comision`
- `precio_medio` = Σ(número_acciones × valor_accion) / Σ(número_acciones)

Es decir, el precio medio ponderado por el volumen de cada lote.

### 3.3 Enriquecimiento con ticker

[`anadir_ticker_desde_bd`](../src/services/wallet_manager.py#L154) lee el mapeo `accion → ticker` con:

```sql
SELECT accion, ticker FROM dbo.info_acciones_base
```

y lo añade como nueva columna `ticker` mediante un `.map()`. Si alguna acción no tiene ticker asociado en la BD, se registra un aviso en el log y la columna queda con valor nulo para esa fila.

## 4. Paso 3 — Obtención de cotizaciones actuales

[`obtener_ultimos_precios_cartera`](../src/services/price_manager.py#L8) (en `price_manager.py`) obtiene el último precio de cierre de cada ticker:

- Construye la lista única de tickers válidos de la cartera.
- Hace **una sola llamada** a `yfinance` (`yf.download`) pidiendo los últimos **5 días** (`period="5d"`) en vez de solo 1 día, para evitar valores nulos por festivos o mercados cerrados.
- Para cada ticker, toma el último `Close` no nulo disponible.
- Devuelve un diccionario `{ticker: precio}` y el DataFrame de cartera con la columna `ultimo_precio` añadida.

## 5. Paso 4 — Cálculo de rendimiento y ganancia

[`calcular_rendimiento_y_ganancia_por_accion`](../src/services/wallet_manager.py#L205) calcula, por acción:

- `coste_total = total_acciones × precio_medio`
- `valor_actual = total_acciones × ultimo_precio`
- `total_ganado = (ultimo_precio − precio_medio) × total_acciones − total_comision` (la comisión se resta por defecto, mediante el parámetro `incluir_comisiones=True`)
- `rendimiento_pct = (total_ganado / coste_total) × 100`, o `0` si `coste_total` es cero (para evitar división por cero).

A continuación, `procesado_cartera_completo` elimina del resultado las acciones excluidas mediante [`eliminar_acciones`](../src/services/wallet_manager.py#L310), filtrando contra la lista `ACCIONES_EXCLUIDAS` definida en `parametrization.py` (configurable por variable de entorno).

## 6. Paso 5 — Resumen global (solo log)

[`imprimir_resumen_cartera`](../src/services/wallet_manager.py#L277) suma `coste_total`, `valor_actual` y `total_ganado` de todas las acciones restantes y muestra por log:

```
Total invertido : ...
Valor actual    : ...
Beneficio total : ...
```

Este paso no persiste nada, solo informa por consola/log.

## 7. Paso 6 — Persistencia en base de datos

[`insertar_posiciones_abiertas`](../src/services/wallet_manager.py#L346) guarda el resultado final en `dbo.posiciones_abiertas`:

1. **Vacía completamente la tabla** (`DELETE FROM dbo.posiciones_abiertas`) — cada ejecución sustituye por entero el snapshot anterior, no es una actualización incremental.
2. Inserta una fila por acción con las columnas: `id` (ticker), `accion`, `numero_acciones` (`total_acciones`), `fecha_compra` (fecha de ejecución), `valor_compra` (`precio_medio`), `comision_compra` (`total_comision`), `total_compra` (`coste_total`), `fecha_actual` (fecha de ejecución), `valor_actual` (`ultimo_precio`), `total_actual` (`valor_actual`), `ultima_variacion` (`rendimiento_pct`).

## 8. Resumen del pipeline

```
main.py (opción "1")
  └─ procesado_cartera_completo()
       ├─ obtener_acciones_compras_df()          ← dbo.acciones_compras
       ├─ obtener_acciones_ventas_df()            ← dbo.acciones_ventas
       ├─ calcular_cartera_actual()                FIFO por (accion, broker)
       ├─ resumir_cartera_por_accion()             precio medio ponderado por accion
       ├─ anadir_ticker_desde_bd()                 ← dbo.info_acciones_base
       ├─ obtener_ultimos_precios_cartera()        ← Yahoo Finance (yfinance)
       ├─ calcular_rendimiento_y_ganancia_por_accion()
       ├─ eliminar_acciones()                      filtra ACCIONES_EXCLUIDAS
       ├─ imprimir_resumen_cartera()                (solo log)
       └─ insertar_posiciones_abiertas()            DELETE + INSERT → dbo.posiciones_abiertas
```

## 9. Tablas y fuentes de datos

| Tabla / fuente | Uso |
|---|---|
| `dbo.acciones_compras` | Lectura — histórico de compras |
| `dbo.acciones_ventas` | Lectura — histórico de ventas |
| `dbo.info_acciones_base` | Lectura — mapeo `accion → ticker` |
| `dbo.posiciones_abiertas` | Escritura — se vacía y se reinserta en cada ejecución |
| Yahoo Finance (vía `yfinance`) | Lectura externa — último precio de cierre por ticker |

## 10. Notas relevantes

- El cálculo FIFO se realiza por `(accion, broker)`, pero el resumen posterior (`resumir_cartera_por_accion`) agrupa solo por `accion`, combinando en el precio medio final las posiciones que pudieran existir en distintos brokers.
- La tabla `dbo.posiciones_abiertas` se trunca y reinserta por completo en cada ejecución: no conserva histórico de snapshots anteriores ni hace *upsert* incremental.
