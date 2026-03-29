"""
Script de consulta Q2: Vuelos por Fecha en HBase.

Este script escanea la tabla 'vuelos' buscando vuelos de un día o mes completo
(YYYYMM o YYYYMMDD) aprovechando la optimización geométrica del row_prefix.
Opcionalmente, se puede filtrar por el aeropuerto de origen.
Cumple con el requerimiento 2 del proyecto.
"""

import happybase
import argparse
import sys

HBASE_HOST = 'localhost'

def get_connection() -> happybase.Connection:
    """Establece y devuelve la conexión con HBase."""
    return happybase.Connection(HBASE_HOST, port=9090)

def format_hhmm(t_str: str) -> str:
    """Convierte una hora bruta a HH:MM."""
    if not t_str or t_str == 'nan': return '--:--'
    t = t_str.split('.')[0].zfill(4)
    return f"{t[:2]}:{t[2:]}" if len(t) == 4 else t_str

def query2_vuelos(conn: happybase.Connection, fecha_prefix: str, origen: str = None, limit: int = 10):
    """
    Busca e imprime vuelos que coinciden con un prefijo de fecha.
    
    Args:
        conn (happybase.Connection): Conexión activa a HBase.
        fecha_prefix (str): Prefijo ROWKEY de búsqueda (YYYYMMDD o YYYYMM).
        origen (str, opcional): Filtro estricto de origen en el cliente (IATA).
        limit (int): Número máximo de resultados a imprimir en pantalla.
    """
    print(f"\n=======================================================")
    print(f"Q2: Vuelos para la fecha/mes '{fecha_prefix}'")
    if origen:
        print(f"Filtro aplicado -> Origen: {origen}")
    print(f"=======================================================")
    
    table = conn.table('vuelos')
    # Aumentamos el límite del scan si hay que filtrar en cliente por origen
    scan_limit = limit * 20 if origen else limit
    scanner = table.scan(row_prefix=fecha_prefix.encode('utf-8'), limit=scan_limit)
    
    count = 0
    for key, data in scanner:
        origen_vuelo = data.get(b'route:Origin', b'').decode()
        
        # Filtro en python si se solicita
        if origen and origen_vuelo != origen:
            continue
            
        print(f" ------------------------------------")
        print(f"  RowKey: {key.decode()}")
        print(f"    Origen: {origen_vuelo} -> Destino: {data.get(b'route:Dest', b'').decode()}")
        
        h_salida = format_hhmm(data.get(b'time:DepTime', b'').decode())
        h_llegada = format_hhmm(data.get(b'time:ArrTime', b'').decode())
        print(f"    Hora Salida: {h_salida} | Llegada: {h_llegada}")
        print(f"    Vuelo: {data.get(b'info:FlightNum', b'').decode()} | Aeronave: {data.get(b'info:TailNum', b'').decode()}")
        
        dist_millas = data.get(b'route:Distance', b'').decode()
        try:
            dist_km = round(float(dist_millas) * 1.60934, 2)
            dist_str = f"{dist_millas} millas ({dist_km} km)"
        except Exception:
            dist_str = f"{dist_millas} millas"
            
        print(f"    Distancia: {dist_str}")
        
        count += 1
        if count >= limit:
            break
            
    if count == 0:
        print("-> No se encontraron vuelos para esta selección.")
    else:
        print(f"\nMostrando {count} registro(s) (Límite visual: {limit}).")
    print("=======================================================\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Q2: Consultar vuelos por año/mes/día.")
    parser.add_argument("fecha", help="Prefijo de fecha (ej. 200801 para Enero 2008, o 20080115 para un día)")
    parser.add_argument("-o", "--origen", help="Filtrar por código IATA de origen (ej. LAX)")
    parser.add_argument("-l", "--limit", type=int, default=5, help="Número de resultados a mostrar (por defecto: 5)")
    
    args = parser.parse_args()
    
    try:
        connection = get_connection()
        query2_vuelos(connection, args.fecha, args.origen, args.limit)
        connection.close()
    except Exception as e:
        print(f"Error fatal conectando a HBase: {e}")
        sys.exit(1)
