# Opción 3 — Detalle de operaciones por acción

Este documento explica la lógica de la **Opción 3 ("Detalle de operaciones por acción")** del menú principal de ThePyTrader.

## 1. Punto de entrada

Desde [src/main.py:29-32](../src/main.py#L29-L32), al seleccionar la opción `3` se piden dos datos por consola y se invoca la función correspondiente:

```python
elif opcion == "3":
    nombre_accion = input("Introduce el nombre de la acción: ").strip()
    broker = input("Introduce el broker: ").strip()
    detalle_operaciones_por_accion(nombre_accion, broker)
```

`detalle_operaciones_por_accion` está definida en [src/services/wallet_manager.py:383](../src/services/wallet_manager.py#L383), importada en `main.py` desde `services.wallet_manager`.

A diferencia de la opción 1 (que calcula un resumen agregado de toda la cartera), esta opción muestra el **detalle FIFO venta a venta** de una única acción en un único broker: para cada venta, qué lotes de compra (posiblemente de fechas distintas) la cubren y qué comisión de compra se prorratea a cada lote consumido. No persiste nada en base de datos, solo escribe en el log.

Se exige indicar el broker (además de la acción) porque, igual que en el cálculo de la opción 1, una misma acción puede tener posiciones —y por tanto un FIFO— independiente en cada broker.

## 2. Paso 1 — Obtención y filtrado de datos

Reutiliza las mismas funciones de acceso a datos que la opción 1:

```python
compras = obtener_acciones_compras_df()   # SELECT * FROM dbo.acciones_compras
ventas = obtener_acciones_ventas_df()     # SELECT * FROM dbo.acciones_ventas
```

Si alguna consulta falla (`None`), se registra un mensaje y la función termina.

A continuación normaliza los criterios de búsqueda (`strip()` + `casefold()`, es decir, sin espacios y sin distinguir mayúsculas/minúsculas) y filtra tanto `compras` como `ventas` por `accion` y `broker`:

```python
filtro_compras = (compras["accion"].str.strip().str.casefold() == nombre_normalizado) & \
                  (compras["broker"].str.strip().str.casefold() == broker_normalizado)
filtro_ventas = (ventas["accion"].str.strip().str.casefold() == nombre_normalizado) & \
                (ventas["broker"].str.strip().str.casefold() == broker_normalizado)
```

Si no hay ni compras ni ventas para esa combinación, se informa por log y se aborta.

Las columnas numéricas (`numero_acciones`, `valor_accion`, `comision`) se convierten a numérico con `pd.to_numeric(errors="coerce")` tanto en `lotes` (compras filtradas) como en `ventas_broker` (ventas filtradas).

## 3. Paso 2 — Preparación de los lotes de compra

```python
lotes = lotes.sort_values("fecha")
lotes["acciones_restantes"] = lotes["numero_acciones"]
ventas_broker = ventas_broker.sort_values("fecha")
```

- Los lotes de compra se ordenan por fecha ascendente (orden FIFO) y se les añade una columna auxiliar `acciones_restantes` que irá disminuyendo a medida que se consuman por las ventas.
- Las ventas también se ordenan por fecha ascendente, para procesarlas en el mismo orden cronológico en que ocurrieron.

Si no hay ventas para esa acción/broker, se informa por log ("Sin ventas registradas para este broker.") y termina — solo tiene sentido mostrar detalle de ventas.

## 4. Paso 3 — Recorrido FIFO venta a venta

Para cada venta (en orden cronológico, numeradas desde 1), se imprime primero su cabecera:

```
Venta N: fecha_venta=..., acciones_vendidas=..., valor_venta=..., comision_venta=..., broker=...
```

Después se recorren los lotes de compra (también en orden cronológico) y se le van asignando acciones hasta cubrir la venta completa:

1. Si ya no quedan acciones pendientes de la venta (`acciones_pendientes <= 0`), se detiene el recorrido de lotes.
2. Si un lote ya está agotado (`acciones_restantes <= 0`), se salta al siguiente.
3. Se asigna a la venta el mínimo entre lo que queda del lote y lo que falta por cubrir de la venta:
   `asignadas = min(acciones_restantes_del_lote, acciones_pendientes_de_la_venta)`
4. Se calcula la **proporción del lote consumida** y se prorratea la comisión de compra de ese lote proporcionalmente:
   `proporcion = asignadas / numero_acciones_totales_del_lote`
   `comision_asignada = comision_del_lote * proporcion`
5. Se imprime una línea de detalle por cada lote que participa en la venta:
   ```
   id_compra, fecha_compra, acciones_asignadas, valor_compra, comision_compra
   ```
6. Se actualizan `acciones_restantes` del lote (se resta lo asignado) y `acciones_pendientes` de la venta (se resta lo asignado), y se continúa con el siguiente lote si aún queda pendiente.

Si, tras recorrer todos los lotes disponibles, la venta no ha podido cubrirse completamente (`acciones_pendientes > 0` al final), se registra un aviso indicando cuántas acciones de compra faltan — señal de datos incompletos (p. ej. compras no registradas) o de un desajuste entre compras y ventas.

Este proceso se repite venta a venta, y los lotes van "arrastrando" su estado (`acciones_restantes`) de una venta a la siguiente, ya que es el mismo DataFrame `lotes` el que se va actualizando en cada iteración — así se respeta el consumo FIFO acumulado a lo largo de todas las ventas de esa acción/broker.

## 5. Resumen del pipeline

```
main.py (opción "3")
  ├─ input: nombre de la acción
  ├─ input: broker
  └─ detalle_operaciones_por_accion(nombre_accion, broker)
       ├─ obtener_acciones_compras_df()      ← dbo.acciones_compras
       ├─ obtener_acciones_ventas_df()       ← dbo.acciones_ventas
       ├─ filtrado por accion + broker (normalizado con strip + casefold)
       ├─ ordenar lotes y ventas por fecha (FIFO)
       └─ por cada venta (orden cronológico):
              recorrer lotes de compra en orden cronológico,
              asignar acciones y prorratear comisión,
              log del detalle de asignación
```

## 6. Tablas y fuentes de datos

| Tabla | Uso |
|---|---|
| `dbo.acciones_compras` | Lectura — lotes de compra de la acción/broker indicados |
| `dbo.acciones_ventas` | Lectura — ventas de la acción/broker indicados |

Esta opción **no escribe en base de datos**; toda la salida es informativa (logs por consola).

## 7. Relación con la Opción 1

Comparte la misma filosofía FIFO que [`calcular_cartera_actual`](../src/services/wallet_manager.py#L77) (usada en la [Opción 1](1_procesamiento_cartera.md)), pero mientras aquella solo calcula el remanente final de acciones por lote (sin explicar qué venta consumió qué lote), `detalle_operaciones_por_accion` expone el desglose completo venta a venta, incluyendo el prorrateo de comisiones de compra — útil para auditar o justificar manualmente el resultado agregado que produce la opción 1.
