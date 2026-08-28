# Opción 7 — Calcular histórico de cartera por acción

Este documento explica la lógica de la **Opción 7 ("Calcular histórico de cartera por acción")** del menú principal de ThePyTrader.

## 1. Punto de entrada

Desde [src/main.py](../src/main.py), al seleccionar la opción `7` se pide el nombre de la acción y se invoca:

```python
elif opcion == "7":
    nombre_accion = input("Introduce el nombre de la acción: ").strip()
    procesado_historico_cartera_por_accion_completo(nombre_accion)
```

`procesado_historico_cartera_por_accion_completo` está definida en [src/services/wallet_manager.py](../src/services/wallet_manager.py).

Esta opción calcula exactamente lo mismo que la [Opción 6](6_calcular_historico_cartera.md) (evolución diaria de inversión, valoración y beneficio), pero **restringido a una única acción**, y persiste el resultado en una tabla independiente sin afectar al histórico de las demás acciones.

## 2. Reutilización del algoritmo de la Opción 6

En vez de duplicar la lógica de cálculo, la Opción 7 reutiliza directamente las piezas ya construidas para la Opción 6, que ya eran genéricas (operan sobre los DataFrames de compras/ventas/cotizaciones que reciben, sin asumir que cubren toda la cartera):

- [`_construir_ledger_fifo`](../src/services/wallet_manager.py)
- [`_calcular_serie_invertido`](../src/services/wallet_manager.py)
- [`_calcular_serie_beneficio_liquidado`](../src/services/wallet_manager.py)
- [`_calcular_serie_valorado`](../src/services/wallet_manager.py)

La única pieza nueva es el **filtrado por acción**, aplicado *antes* de pasar los datos a ese mismo algoritmo.

### 2.1 Filtrado por acción — `_filtrar_por_accion`

[`_filtrar_por_accion(compras, ventas, cotizaciones, accion)`](../src/services/wallet_manager.py) filtra los tres DataFrames a una única acción:

1. Normaliza el nombre buscado con `strip()` + `casefold()` (mismo criterio que usa [`detalle_operaciones_por_accion`](3_detalle_operaciones_por_accion.md) en la Opción 3, para no depender de mayúsculas/minúsculas ni espacios).
2. Filtra `compras` y `ventas` por coincidencia normalizada de `accion`.
3. Si no hay ni compras ni ventas para esa acción, devuelve `accion_real = None` (indicando que no se encontró nada).
4. Si se encuentra, toma el nombre "real" de la acción tal como aparece en los datos (`accion_real`) y filtra también `cotizaciones` por el mismo criterio normalizado, **renombrando la columna `accion` de las cotizaciones a `accion_real`** — así encaja exactamente con el valor usado en `compras`/`ventas`, ya que el resto del algoritmo empareja acciones por igualdad exacta de cadena.

### 2.2 `calcular_historico_cartera(accion=None)`

La función de la Opción 6 se generalizó añadiendo un parámetro opcional `accion`:

```python
def calcular_historico_cartera(accion: str | None = None) -> pd.DataFrame:
    ...
    if accion is not None:
        compras, ventas, cotizaciones, accion_real = _filtrar_por_accion(compras, ventas, cotizaciones, accion)
        if accion_real is None:
            logger.info("No hay operaciones registradas para la acción '%s'.", accion)
            return pd.DataFrame()
    ...
```

- Sin `accion` (Opción 6): se comporta exactamente igual que antes, calculando la cartera completa.
- Con `accion` (Opción 7): filtra los datos de origen a esa acción y ejecuta el mismo pipeline (ledger FIFO → series diarias de invertido/valorado/beneficio liquidado → `total_beneficio_no_liquidado = total_valorado - total_invertido`), y añade una columna `accion` al resultado final con el nombre real encontrado en los datos.

Si la acción indicada no tiene ninguna compra ni venta registrada, se informa por log y se devuelve un DataFrame vacío (no se inserta nada en base de datos).

## 3. Persistencia en base de datos

A diferencia de la Opción 6 (que vacía toda la tabla `historico_rendimientos` en cada ejecución), aquí solo se sustituyen los registros de la acción calculada:

[`insertar_historico_cartera_por_accion_en_bd(df, accion)`](../src/services/wallet_manager.py):

1. Si el DataFrame está vacío, registra un aviso y no hace nada.
2. **Elimina únicamente los registros previos de esa acción**: `DELETE FROM dbo.historico_rendimientos_por_accion WHERE accion = :accion` (no un `TRUNCATE` de toda la tabla) — así el histórico ya calculado de otras acciones permanece intacto.
3. Inserta el nuevo histórico de la acción con `to_sql(..., if_exists="append")`.

[`procesado_historico_cartera_por_accion_completo(accion)`](../src/services/wallet_manager.py) orquesta ambos pasos, usando siempre el nombre "real" de la acción (tal como aparece en los datos, no el texto tecleado por el usuario) tanto para el `DELETE` como para el valor insertado en la columna `accion`, evitando así desajustes de mayúsculas/espacios entre ejecuciones.

## 4. Resumen del pipeline

```
main.py (opción "7")
  ├─ input: nombre de la acción
  └─ procesado_historico_cartera_por_accion_completo(nombre_accion)
       ├─ calcular_historico_cartera(accion=nombre_accion)
       │      ├─ obtener_acciones_compras_euro_df()          ← dbo.acciones_compras_euro
       │      ├─ obtener_acciones_ventas_euro_df()            ← dbo.acciones_venta_euro
       │      ├─ obtener_historico_cotizaciones_euro_df()     ← dbo.historico_cotizaciones_acciones_euro
       │      ├─ _filtrar_por_accion()                        filtra los 3 DataFrames a la acción indicada
       │      ├─ _construir_ledger_fifo()                     (mismo algoritmo que la Opción 6)
       │      ├─ _calcular_serie_invertido() / _calcular_serie_beneficio_liquidado() / _calcular_serie_valorado()
       │      └─ total_beneficio_no_liquidado = valorado − invertido
       └─ insertar_historico_cartera_por_accion_en_bd()        DELETE (solo esa acción) + INSERT → dbo.historico_rendimientos_por_accion
```

## 5. Tablas y fuentes de datos

| Tabla / vista | Uso |
|---|---|
| `dbo.acciones_compras_euro` (vista) | Lectura — compras de la acción indicada, con valor convertido a EUR |
| `dbo.acciones_venta_euro` (vista) | Lectura — ventas de la acción indicada, con valor convertido a EUR |
| `dbo.historico_cotizaciones_acciones_euro` (vista) | Lectura — cotización histórica diaria de la acción indicada, en EUR |
| `dbo.historico_rendimientos_por_accion` | Escritura — se eliminan solo los registros de esa acción y se reinsertan |

## 6. Relación con la Opción 6

La Opción 7 no reimplementa ningún cálculo: aplica el mismo motor de cálculo (`_construir_ledger_fifo` + series diarias) que la [Opción 6](6_calcular_historico_cartera.md) sobre un subconjunto de datos ya filtrado, y solo difiere en el destino de la persistencia (`historico_rendimientos_por_accion`, con borrado selectivo por acción, en vez de `historico_rendimientos` con `TRUNCATE` completo). Todas las notas relevantes de la Opción 6 (dependencia de las Opciones 4 y 5, uso de `COTIZACIONES_FECHA_INICIO`, *forward-fill* de precios y su fallback al precio medio de compra cuando no hay cotización) aplican igualmente aquí.
