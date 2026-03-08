import sys

from services.price_manager import obtener_ultimos_precios_cartera
from services.wallet_manager import (
    anadir_ticker_desde_bd,
    obtener_acciones_compras_df,
    obtener_acciones_ventas_df,
    calcular_cartera_actual,
    resumir_cartera_por_accion,
    calcular_rendimiento_y_ganancia_por_accion,
    imprimir_resumen_cartera,
    eliminar_acciones,
    insertar_posiciones_abiertas,
)


def main() -> int:

    print("Iniciando ThePyTrader 🚀")

    # Paso 1: Obtención datos origen: ventas y compras
    compras = obtener_acciones_compras_df()
    ventas = obtener_acciones_ventas_df()

    # Paso 2: Procesamiento cartera
    # 2.1 Cálculo posiciones abiertas
    posiciones_abiertas = calcular_cartera_actual(compras, ventas)
    # 2.2 Agrupación por acción
    cartera = resumir_cartera_por_accion(posiciones_abiertas)
    # 2.3 Añadir tickers
    cartera_con_tikcer = anadir_ticker_desde_bd(cartera)

    # Paso 3: Obtención de cotizaciones
    precios, df_con_precios = obtener_ultimos_precios_cartera(cartera_con_tikcer)

    # Paso 4: Cálculo rendimiento
    df_final = calcular_rendimiento_y_ganancia_por_accion(df_con_precios)

    acciones_a_quitar = ["BATS", "Diageo"]
    df_final = eliminar_acciones(df_final, acciones_a_quitar)

    # Paso 5: Impresion de datos 
    imprimir_resumen_cartera(df_final)

    # Paso 6: Inserción BD posiciones abiertas
    insertar_posiciones_abiertas(df_final)

    return 0


if __name__ == "__main__":
    sys.exit(main())  # next section explains the use of sys.exit
