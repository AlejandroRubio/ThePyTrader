# ThePyTrader 🚀


![Project Logo](images/logo.png)


**ThePyTrader** es una herramienta en Python para calcular y analizar una cartera de inversión a partir de movimientos de **compras y ventas**, enriquecer la información con **tickers**, obtener **precios actualizados**, calcular **rendimiento y ganancia**, y finalmente **persistir las posiciones abiertas en una base de datos**.

---

## ✨ Funcionalidad 1: Procesamiento cartera

El flujo de ejecución del proyecto es el siguiente:

1. Carga de datos de compras
2. Carga de datos de ventas
3. Cálculo de posiciones abiertas
4. Resumen de cartera por acción
5. Asignación de tickers desde la tabla `info_acciones_base` en base de datos
6. Obtención de precios actuales
7. Cálculo de rendimiento y ganancia
8. Impresión de resultados
9. Inserción de posiciones abiertas en base de datos

---

## 💰 Funcionalidad 2: Procesamiento precios metales

El flujo de ejecución de esta opción es el siguiente:

1. Descarga del histórico diario (6 años) de oro, plata y cobre, junto con el tipo de cambio EUR/USD, vía yfinance
2. Conversión de los precios a EUR/onza troy (con ajuste específico para el cobre, cotizado originalmente en USD/libra)
3. Inserción/actualización (upsert) del histórico en la tabla `metales_cotizacion`

---

## 🔍 Funcionalidad 3: Detalle de operaciones por acción

El flujo de ejecución de esta opción es el siguiente:

1. Solicitud al usuario del nombre de la acción y del broker a consultar
2. Obtención desde base de datos de las compras (`acciones_compras`) y ventas (`acciones_ventas`) de esa acción para el broker indicado
3. Aplicación del algoritmo FIFO para emparejar cada venta con el/los lote/s de compra correspondientes (de más antiguo a más reciente)
4. Prorrateo de la comisión de compra según el bloque de acciones consumido de cada lote
5. Impresión del detalle por venta: lotes de compra utilizados, acciones asignadas, precio de compra y comisión de compra prorrateada

---

## 💱 Funcionalidad 4: Obtener tipos de cambio

El flujo de ejecución de esta opción es el siguiente:

1. Descarga, para cada divisa configurada en `LISTADO_DIVISAS`, del histórico diario del tipo de cambio EUR → divisa (vía yfinance), desde la fecha `TIPOS_CAMBIO_FECHA_INICIO` hasta hoy
2. Vaciado (truncate) de la tabla `historico_tipos_cambio`
3. Inserción de todos los tipos de cambio obtenidos en dicha tabla

---

## 📈 Funcionalidad 5: Obtener histórico cotizaciones

El flujo de ejecución de esta opción es el siguiente:

1. Obtención del listado de acciones distintas presentes en `acciones_compras`, junto con su divisa y su ticker (desde `info_acciones_base`)
2. Vaciado (truncate) de la tabla `historico_cotizaciones_acciones`
3. Descarga, acción por acción, del histórico diario de cotización (vía yfinance) desde la fecha `COTIZACIONES_FECHA_INICIO` hasta hoy, insertando el resultado inmediatamente tras cada acción para que un fallo puntual no afecte a las acciones ya procesadas

---

## 📊 Funcionalidad 6: Calcular histórico de cartera

El flujo de ejecución de esta opción es el siguiente:

1. Obtención de compras, ventas (vistas `acciones_compras_euro` / `acciones_venta_euro`, ya en EUR) y cotizaciones históricas (`historico_cotizaciones_acciones_euro`)
2. Reconstrucción del ledger FIFO (qué lote de compra cubre cada venta, con fecha y comisiones prorrateadas)
3. Cálculo, día a día desde `COTIZACIONES_FECHA_INICIO` hasta hoy, de la inversión y valoración de las posiciones abiertas y del beneficio realizado acumulado por las ventas
4. Vaciado (truncate) e inserción del histórico diario resultante en la tabla `historico_rendimientos`

---

## 📉 Funcionalidad 7: Calcular histórico de cartera por acción

Misma lógica que la Funcionalidad 6, pero restringida a una única acción indicada por el usuario. El flujo de ejecución de esta opción es el siguiente:

1. Solicitud al usuario del nombre de la acción
2. Filtrado de compras, ventas y cotizaciones históricas a esa acción (reutilizando el mismo algoritmo FIFO y de cálculo de series diarias que la Funcionalidad 6)
3. Cálculo, día a día desde `COTIZACIONES_FECHA_INICIO` hasta hoy, de la inversión, valoración y beneficio (realizado y no realizado) de esa acción
4. Eliminación de los registros previos de esa acción e inserción del nuevo histórico en la tabla `historico_rendimientos_por_accion` (el resto de acciones ya calculadas no se ven afectadas)

---

## 🧱 Estructura del proyecto

```
.
├── main.py
├── integrations/
│   └── investing_scrapper.py
├── services/
│   ├── wallet_manager.py
│   └── price_manager.py
├── utils/
│   └── file_utils.py
└── datasets/
    └── investing_urls/
        └── default.csv
```

---

## ⚙️ Requisitos

- Python 3.10 o superior
- Conexión a internet para obtención de precios
- Dependencias listadas en `requirements.txt`

Dependencias comunes:
- pandas
- numpy
- yfinance (si se usa como fuente de precios)
- requests / beautifulsoup4 (si se usa scraping)
- driver de base de datos correspondiente

---

## ▶️ Uso

Ejecutar el script principal:

```bash
python main.py
```

El script mostrará un resumen de la cartera y almacenará las posiciones abiertas en la base de datos configurada.

-----

## 🏷️ Tickers

Los tickers se asignan consultando la tabla `info_acciones_base` en base de datos, que mapea el nombre de cada acción a su ticker (columnas `accion`, `ticker`).

---

## 💸 Precios de mercado

El proyecto puede obtener precios desde:
- Scraping de Investing
- APIs como yfinance

---

## 🧮 Rendimiento

Se calcula por acción:
- Precio medio de compra
- Precio actual
- Ganancia absoluta
- Rendimiento porcentual

Se pueden excluir acciones manualmente antes del cálculo final.

---

## 🗄️ Base de datos

Dentro de la carpeta database están los ficheros SQL con las tablas requeridas para almacenar los datos en BD:

Tablas base:
- acciones_compras — histórico de compras de acciones (fecha, número de acciones, valor, comisión y broker)
- acciones_ventas — histórico de ventas de acciones (misma estructura que `acciones_compras`)
- posiciones_abiertas — snapshot de las posiciones abiertas actuales de la cartera (cantidad, precio medio, valor actual y rendimiento), recalculado en cada ejecución de la Funcionalidad 1
- metales_cotizacion — histórico de cotizaciones de oro, plata y cobre en EUR/onza troy
- historico_tipos_cambio — histórico diario de tipos de cambio EUR → divisa
- historico_cotizaciones_acciones — histórico diario de cotización de cada acción
- historico_rendimientos — histórico diario de inversión, valoración y beneficio (realizado y no realizado) de la cartera
- historico_rendimientos_por_accion — mismo histórico diario que `historico_rendimientos`, desglosado por acción
- info_acciones_base — mapeo de cada acción a su ticker de mercado
- info_acciones_tipologia — clasificación de cada acción (sector, subsector, país y estrategia)


---

## 📄 Licencia

Define aquí la licencia del proyecto.
