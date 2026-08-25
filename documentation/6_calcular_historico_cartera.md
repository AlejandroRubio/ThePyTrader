# Opción 6 — Calcular histórico de cartera

Este documento explica la lógica de la **Opción 6 ("Calcular histórico de cartera")** del menú principal de ThePyTrader.

## 1. Punto de entrada

Desde [src/main.py](../src/main.py), al seleccionar la opción `6` se invoca:

```python
procesado_historico_cartera_completo()
```

definida en [src/services/wallet_manager.py](../src/services/wallet_manager.py), importada en `main.py` desde `services.wallet_manager`.

El objetivo es calcular, **día a día** desde `COTIZACIONES_FECHA_INICIO` hasta hoy, la evolución de la cartera: cuánto se tenía invertido, cuánto valía a precio de mercado, y cuánto beneficio se había realizado (ventas) y no realizado (posiciones abiertas) en cada fecha. El resultado se persiste en `dbo.historico_rendimientos`.

```python
def procesado_historico_cartera_completo():
    historico = calcular_historico_cartera()
    insertar_historico_cartera_en_bd(historico)
```

## 2. Parametrización

Reutiliza `COTIZACIONES_FECHA_INICIO` (definida en [src/parametrization.py](../src/parametrization.py) para la [Opción 5](5_obtener_historico_cotizaciones.md)) como fecha de inicio del rango diario a calcular.

## 3. Fuentes de datos: vistas en EUR

A diferencia de otras opciones, aquí no se leen directamente las tablas base `acciones_compras` / `acciones_ventas`, sino tres **vistas** (analizadas en `database/`) que ya devuelven el valor convertido a euros:

- **`dbo.acciones_compras_euro`**: igual que `acciones_compras`, pero `valor_accion` se convierte a EUR haciendo `LEFT JOIN` con `dbo.historico_tipos_cambio` (si `divisa = 'EUR'` o es nula, se deja igual; si no, se divide por el tipo de cambio EUR→divisa de esa fecha).
- **`dbo.acciones_venta_euro`**: misma lógica de conversión aplicada sobre `acciones_ventas`.
- **`dbo.historico_cotizaciones_acciones_euro`**: misma lógica de conversión aplicada sobre `historico_cotizaciones_acciones` (cargada por la [Opción 5](5_obtener_historico_cotizaciones.md)).

Estas tres vistas se leen mediante:

- [`obtener_acciones_compras_euro_df`](../src/services/wallet_manager.py)
- [`obtener_acciones_ventas_euro_df`](../src/services/wallet_manager.py)
- [`obtener_historico_cotizaciones_euro_df`](../src/services/wallet_manager.py)

Si alguna de las tres consultas falla, `calcular_historico_cartera` registra un error y devuelve un DataFrame vacío.

## 4. Paso 1 — Reconstrucción del ledger FIFO

[`_construir_ledger_fifo(compras, ventas)`](../src/services/wallet_manager.py) generaliza el algoritmo FIFO usado en la [Opción 1](1_procesamiento_cartera.md) y en la [Opción 3](3_detalle_operaciones_por_accion.md), pero en vez de quedarse solo con el remanente final, **registra explícitamente qué lote de compra cubre cada venta**, con fecha, cantidad y comisiones prorrateadas. Genera dos estructuras:

- **`lotes`**: una fila por lote de compra (`accion`, `broker`, `fecha_compra`, `numero_acciones` original, `valor_accion`, `comision`).
- **`eventos_consumo`**: una fila por cada asignación lote→venta, con `accion`, `broker`, `fecha_compra`, `valor_compra`, `fecha_venta`, `valor_venta`, `acciones` (cantidad consumida) y las comisiones de compra/venta prorrateadas según el bloque de acciones consumido (igual criterio de prorrateo que la Opción 3).

Con estas dos tablas es posible reconstruir el estado de la cartera en **cualquier fecha pasada**: basta con considerar, para cada lote, solo los eventos de consumo cuya `fecha_venta` sea anterior o igual a la fecha de referencia.

## 5. Paso 2 — Series diarias

A partir del ledger, se calculan tres series indexadas por fecha (una por día, desde `COTIZACIONES_FECHA_INICIO` hasta hoy):

### 5.1 `total_invertido` — [`_calcular_serie_invertido`](../src/services/wallet_manager.py)

Modela el coste de las posiciones abiertas como una serie de altas y bajas:
- **Alta**: en la `fecha_compra` de cada lote, se suma `numero_acciones × valor_accion` (coste de compra).
- **Baja**: en la `fecha_venta` de cada evento de consumo, se resta `acciones × valor_compra` (se retira el coste de la parte vendida, valorada a su precio de compra original).

Se agrupan las altas/bajas por fecha, se calcula el acumulado cronológico y se proyecta sobre el calendario diario completo mediante *forward-fill* (el coste no cambia entre eventos).

### 5.2 `total_beneficio_liquidado` — [`_calcular_serie_beneficio_liquidado`](../src/services/wallet_manager.py)

Para cada evento de consumo (cada venta parcial de un lote), el beneficio realizado es:

```
(valor_venta − valor_compra) × acciones − comision_compra − comision_venta
```

Se agrupan estos beneficios por `fecha_venta`, se acumulan cronológicamente y se proyectan sobre el calendario diario (forward-fill): el beneficio realizado solo cambia en los días en que hay una venta.

### 5.3 `total_valorado` — [`_calcular_serie_valorado`](../src/services/wallet_manager.py)

Para cada acción de la cartera:
1. Se calculan las acciones en cartera día a día (mismo criterio de altas/bajas que `total_invertido`, pero en número de acciones en vez de importe).
2. Se obtiene la cotización diaria de esa acción desde `historico_cotizaciones_acciones_euro`, proyectada sobre el calendario completo con *forward-fill* (para cubrir fines de semana/festivos sin cotización).
3. Se multiplica acciones en cartera × precio, día a día, y se suma la contribución de todas las acciones.

Si una acción de la cartera no tiene ninguna cotización histórica cargada, se registra un aviso (`logger.warning`) y su valoración diaria se trata como 0 — indica que conviene ejecutar antes la [Opción 5](5_obtener_historico_cotizaciones.md) para esa acción.

### 5.4 `total_beneficio_no_liquidado`

Se calcula directamente como:

```
total_beneficio_no_liquidado = total_valorado − total_invertido
```

## 6. Paso 3 — Persistencia en base de datos

[`insertar_historico_cartera_en_bd`](../src/services/wallet_manager.py) guarda el resultado en `dbo.historico_rendimientos`:

1. Si el DataFrame está vacío, registra un aviso y no hace nada.
2. **Vacía completamente la tabla** (`TRUNCATE TABLE dbo.historico_rendimientos`) — la tabla no tiene clave única, por lo que cada ejecución recalcula y sustituye el histórico completo.
3. Inserta todas las filas (`fecha`, `total_invertido`, `total_valorado`, `total_beneficio_liquidado`, `total_beneficio_no_liquidado`) con `to_sql(..., if_exists="append")`.

## 7. Resumen del pipeline

```
main.py (opción "6")
  └─ procesado_historico_cartera_completo()
       └─ calcular_historico_cartera()
              ├─ obtener_acciones_compras_euro_df()          ← dbo.acciones_compras_euro
              ├─ obtener_acciones_ventas_euro_df()            ← dbo.acciones_venta_euro
              ├─ obtener_historico_cotizaciones_euro_df()     ← dbo.historico_cotizaciones_acciones_euro
              ├─ _construir_ledger_fifo()                     lotes + eventos de consumo (FIFO)
              ├─ _calcular_serie_invertido()                  coste diario de posiciones abiertas
              ├─ _calcular_serie_beneficio_liquidado()        beneficio realizado acumulado
              ├─ _calcular_serie_valorado()                   valor de mercado diario
              └─ total_beneficio_no_liquidado = valorado − invertido
       └─ insertar_historico_cartera_en_bd()                  TRUNCATE + INSERT → dbo.historico_rendimientos
```

## 8. Tablas y fuentes de datos

| Tabla / vista | Uso |
|---|---|
| `dbo.acciones_compras_euro` (vista) | Lectura — compras con valor convertido a EUR |
| `dbo.acciones_venta_euro` (vista) | Lectura — ventas con valor convertido a EUR |
| `dbo.historico_cotizaciones_acciones_euro` (vista) | Lectura — cotización histórica diaria en EUR |
| `dbo.historico_rendimientos` | Escritura — se vacía (`TRUNCATE`) y se reinserta por completo en cada ejecución |

## 9. Notas relevantes

- Esta opción depende de que las opciones [4](4_obtener_tipos_de_cambio.md) (tipos de cambio, usados por las vistas `_euro` para convertir divisas distintas de EUR) y [5](5_obtener_historico_cotizaciones.md) (histórico de cotizaciones) se hayan ejecutado previamente; sin esos datos, la valoración y/o la conversión de divisa de ciertas acciones puede quedar incompleta.
- El rango de fechas calculado empieza siempre en `COTIZACIONES_FECHA_INICIO`, independientemente de si hay compras anteriores a esa fecha; los días previos a la primera operación simplemente tendrán todos los importes a 0.
- El *forward-fill* de precios asume que, en ausencia de cotización para un día concreto (fin de semana, festivo), el valor de mercado es el del último cierre disponible.
