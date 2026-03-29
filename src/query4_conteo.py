"""
Script de consulta Q4: Auditoría y conteo de registros en HBase.

Verifica el número de filas almacenadas en cada tabla usando un KeyOnlyFilter
hacia el lado del servidor, lo que previene la transferencia del peso completo
de los datos por la red y optimiza drásticamente el proceso.
Cumple con el requerimiento 4 del proyecto.
"""

import happybase
import sys
import time

HBASE_HOST = 'localhost'

def get_connection() -> happybase.Connection:
    """Devuelve conexión a HBase."""
    return happybase.Connection(HBASE_HOST, port=9090)

def query4_conteos(conn: happybase.Connection):
    """
    Escanea cada tabla de la base de datos limitando lo recuperado a sus claves (RowKey).
    Calcula e imprime el total.
    
    Args:
        conn (happybase.Connection): Conexión activa a HBase.
    """
    print(f"\n=======================================================")
    print(f"Q4 - Auditoría de Conteo de registros en HBase")
    print(f"(usando KeyOnlyFilter() para evitar transferir valores)")
    print(f"=======================================================\n")
    tables = [b'aeropuertos', b'companias', b'rutas', b'vuelos']
    for t in tables:
        try:
            table = conn.table(t)
            scanner = table.scan(filter=b"KeyOnlyFilter()")
            count = sum(1 for _ in scanner)
            print(f"> Tabla '{t.decode()}': {count:,} registros almacenados.".replace(',', '.'))
        except Exception as e:
            print(f"> Error al acceder a tabla '{t.decode()}': {e}")
    print("\n")

if __name__ == "__main__":
    print("Iniciando barrido de tablas. Para la tabla 'vuelos' esto puede tardar unos minutos...")
    try:
        connection = get_connection()
        start_time = time.time()
        query4_conteos(connection)
        end_time = time.time()
        connection.close()
        print(f"Tiempo de ejecución total: {round(end_time - start_time, 4)} segundos")
    except Exception as e:
        print(f"Error fatal conectando a HBase: {e}")
        sys.exit(1)
