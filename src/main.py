import sys

from services.metal_manager import procesado_metales_completo
from services.wallet_manager import procesado_cartera_completo
from logger import get_logger

logger = get_logger(__name__)

def mostrar_menu():
    print("\n--- MENÚ PRINCIPAL ---")
    print("1. Procesamiento cartera")
    print("2. Procesamiento precios metales")
    print("0. Salir")


def main() -> int:

    logger.info("Iniciando ThePyTrader")
    
    while True:
        mostrar_menu()
        opcion = input("Selecciona operación: ")

        if opcion == "1":
            procesado_cartera_completo()
        elif opcion == "2":
            procesado_metales_completo()
        elif opcion == "0":
            logger.info("Saliendo del programa")
            break
        else:
            print("Opción no válida, intenta de nuevo.")
        
    return 0


if __name__ == "__main__":
    sys.exit(main())  # next section explains the use of sys.exit
