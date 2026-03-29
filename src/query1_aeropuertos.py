"""
Script de consulta Q1: Información de Aeropuertos en HBase.

Este script permite consultar la tabla 'aeropuertos' dado un código IATA.
Soporta proyección de columnas específicas (ej. solo la ciudad o el país).
Cumple con el requerimiento 1 del proyecto.
"""

import happybase
import argparse
import sys

# Configuración de conexión
HBASE_HOST = 'localhost'

def get_connection() -> happybase.Connection:
    """Establece y devuelve la conexión con el servidor HBase local."""
    return happybase.Connection(HBASE_HOST, port=9090)

def query1_aeropuerto(conn: happybase.Connection, iata_code: str, columns: list = None):
    """
    Busca un aeropuerto por su código IATA (RowKey) e imprime sus atributos.
    Si se especifican columnas, recupera únicamente esas (proyección).
    
    Args:
        conn (happybase.Connection): Conexión activa a HBase.
        iata_code (str): Código IATA del aeropuerto (ej. 'ATL').
        columns (list, opcional): Lista de columnas en formato 'cf:col' para proyectar.
    """
    print(f"\n=======================================================")
    print(f"Q1: Detalle del Aeropuerto {iata_code}")
    if columns:
        print(f"Proyectando columnas: {columns}")
    print(f"=======================================================")
    
    table = conn.table('aeropuertos')
    # Codificar las columnas si se proveen
    cols_encoded = [c.encode('utf-8') for c in columns] if columns else None
    row = table.row(iata_code.encode('utf-8'), columns=cols_encoded)
    
    if row:
        for key, value in row.items():
            print(f"  {key.decode()}: {value.decode()}")
    else:
        print("-> Aeropuerto no encontrado o sin datos en las columnas solicitadas.")
    print("=======================================================\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Q1: Consultar información de un aeropuerto.")
    parser.add_argument("iata", help="Código IATA del aeropuerto a buscar (ej. JFK)")
    parser.add_argument("-c", "--columns", nargs="+", help="Columnas a proyectar (ej. info:city info:state)")
    
    args = parser.parse_args()
    
    try:
        connection = get_connection()
        query1_aeropuerto(connection, args.iata, args.columns)
        connection.close()
    except Exception as e:
        print(f"Error fatal conectando a HBase: {e}")
        sys.exit(1)
