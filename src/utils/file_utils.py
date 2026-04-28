import pandas as pd
from logger import get_logger

logger = get_logger(__name__)


def csv_to_json(file_path):
    # Cargar el CSV con detección de separador automático; ajustar si es necesario
    try:
        data = pd.read_csv(file_path, sep=';')
    except Exception as e:
        logger.exception("Error al leer el archivo")
        return
    
    # Renombrar las columnas para claridad
    if len(data.columns) == 2:
        data.columns = ['Company', 'URL']
    else:
        logger.error("Formato de archivo no esperado")
        return
    
    # Eliminar posibles espacios en blanco en los nombres de las compañías y las URLs
    data['Company'] = data['Company'].str.strip()
    data['URL'] = data['URL'].str.strip()
    
    # Convertir el DataFrame a un diccionario en el formato deseado
    json_output = data.set_index('Company').to_dict()['URL']
    
    return json_output

