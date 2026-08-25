import sys

from services.metal_manager import procesado_metales_completo
from services.wallet_manager import procesado_cartera_completo, detalle_operaciones_por_accion
from services.price_manager import procesado_tipos_cambio_completo, procesado_historico_cotizaciones_completo
from logger import get_logger

logger = get_logger(__name__)

def mostrar_menu():
    logger.info("\n--- MENÚ PRINCIPAL ---")
    logger.info("1. Procesamiento cartera")
    logger.info("2. Procesamiento precios metales")
    logger.info("3. Detalle de operaciones por acción")
    logger.info("4. Obtener tipos de cambio")
    logger.info("5. Obtener histórico cotizaciones")
    logger.info("0. Salir")


def main() -> int:

    logger.info("Iniciando ThePyTrader")
    
    while True:
        mostrar_menu()
        opcion = input("Selecciona operación: ")

        if opcion == "1":
            procesado_cartera_completo()
        elif opcion == "2":
            procesado_metales_completo()
        elif opcion == "3":
            nombre_accion = input("Introduce el nombre de la acción: ").strip()
            broker = input("Introduce el broker: ").strip()
            detalle_operaciones_por_accion(nombre_accion, broker)
        elif opcion == "4":
            procesado_tipos_cambio_completo()
        elif opcion == "5":
            procesado_historico_cotizaciones_completo()
        elif opcion == "0":
            logger.info("Saliendo del programa")
            break
        else:
            logger.info("Opción no válida, intenta de nuevo.")
        
    return 0


if __name__ == "__main__":
    sys.exit(main())  # next section explains the use of sys.exit
