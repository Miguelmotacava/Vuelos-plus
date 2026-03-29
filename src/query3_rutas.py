"""
Script de consulta Q3: Análisis de Rutas en HBase.

Realiza una petición a la tabla 'rutas' pre-agregada buscando estadísticas
históricas para el par Origen-Destino proporcionado, retornando sustratos
analíticos ordenados por la aerolínea con mayor frecuencia.
Cumple con el requerimiento 3 del proyecto.
"""

import happybase
import argparse
import sys
import math
import time

HBASE_HOST = 'localhost'

def haversine(lat1, lon1, lat2, lon2):
    """Calcula la distancia en grandes círculos entre dos puntos de la Tierra."""
    R = 6371.0 # Radio Tierra en km
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    return R * c

def get_connection() -> happybase.Connection:
    """Establece conexión a HBase."""
    return happybase.Connection(HBASE_HOST, port=9090)

def query3_rutas(conn: happybase.Connection, origen: str, destino: str):
    """
    Recupera estadísticas de una ruta agrupadas por aerolínea y ordenadas por relevancia.
    
    Args:
        conn (happybase.Connection): Conexión activa a HBase.
        origen (str): IATA origen.
        destino (str): IATA destino.
    """
    print(f"\n=======================================================")
    print(f"Q3 - Estadísticas Analíticas de Ruta {origen}-{destino}")
    print(f"=======================================================")
    table_rutas = conn.table('rutas')
    table_companias = conn.table('companias')
    table_aero = conn.table('aeropuertos')
    
    rk = f"{origen}_{destino}".encode('utf-8')
    row = table_rutas.row(rk)
    
    if not row:
        print("-> Ruta no encontrada o sin estadísticas en HBase.")
        return
        
    # Obtener distancia desde la lat/lon de los aeropuertos
    row_origen = table_aero.row(origen.encode('utf-8'))
    row_destino = table_aero.row(destino.encode('utf-8'))
    
    dist_km = "N/A"
    try:
        lat1 = float(row_origen.get(b'info:lat').decode())
        lon1 = float(row_origen.get(b'info:long').decode())
        lat2 = float(row_destino.get(b'info:lat').decode())
        lon2 = float(row_destino.get(b'info:long').decode())
        dist_km = round(haversine(lat1, lon1, lat2, lon2), 2)
    except Exception:
        pass
    print(f"\nRuta: {origen} -> {destino}\n")
    print(f"Distancia Estimada (Geográfica): {dist_km} km\n")
        
    airlines_stats = {}
    for key, value in row.items():
        col_name = key.decode().split(':')[1]
        carrier, metric = col_name.split('_', 1)
        val = value.decode()
        if val == 'nan': val = 'N/A'
        
        if carrier not in airlines_stats:
            airlines_stats[carrier] = {}
        airlines_stats[carrier][metric] = val
        
    sorted_airlines = sorted(
        airlines_stats.items(), 
        key=lambda item: float(item[1].get('flights_count', 0)) if item[1].get('flights_count', 'N/A') != 'N/A' else 0,
        reverse=True
    )
    
    # Calcular y mostrar Estadísticas Totales (Promedio Ponderado)
    total_flights = 0
    sum_airtime_peso = 0.0
    sum_depdelay_peso = 0.0
    sum_arrdelay_peso = 0.0
    
    for carrier, stats in sorted_airlines:
        try:
            f_count = float(stats.get('flights_count', 0))
            if f_count > 0:
                total_flights += f_count
                sum_airtime_peso += float(stats.get('avg_airtime', 0)) * f_count
                sum_depdelay_peso += float(stats.get('avg_depdelay', 0)) * f_count
                sum_arrdelay_peso += float(stats.get('avg_arrdelay', 0)) * f_count
        except Exception:
            pass
            
    if total_flights > 0:
        print(f"--- ESTADÍSTICA GLOBAL DE LA RUTA ---")
        print(f"  Total de Aerolíneas Operando: {len(sorted_airlines)}")
        print(f"  Total de Vuelos en la Ruta:   {int(total_flights)}")
        print(f"  Promedio AirTime General:     {round(sum_airtime_peso / total_flights, 2)} mins")
        print(f"  Promedio Retraso Salida:      {round(sum_depdelay_peso / total_flights, 2)} mins")
        print(f"  Promedio Retraso Llegada:     {round(sum_arrdelay_peso / total_flights, 2)} mins")
        print(f"-------------------------------------\n")
        
    print("Desglose por Aerolínea:")
    
    for carrier, stats in sorted_airlines:
        carrier_row = table_companias.row(carrier.encode('utf-8'))
        carrier_name = carrier_row.get(b'info:Description', b'').decode() if carrier_row else carrier
        
        try:
            avg_airtime = round(float(stats.get('avg_airtime')), 2) if stats.get('avg_airtime') != 'N/A' else 'N/A'
            avg_depdelay = round(float(stats.get('avg_depdelay')), 2) if stats.get('avg_depdelay') != 'N/A' else 'N/A'
            avg_arrdelay = round(float(stats.get('avg_arrdelay')), 2) if stats.get('avg_arrdelay') != 'N/A' else 'N/A'
        except Exception:
            avg_airtime = stats.get('avg_airtime')
            avg_depdelay = stats.get('avg_depdelay')
            avg_arrdelay = stats.get('avg_arrdelay')
            
        print(f"  Aerolínea: {carrier_name} ({carrier})")
        print(f"      Frecuencia (Vuelos operados): {int(float(stats.get('flights_count', 0)))}")
        print(f"      Duración Promedio: {avg_airtime} mins")
        print(f"      Retraso Salida:    {avg_depdelay} mins")
        print(f"      Retraso Llegada:   {avg_arrdelay} mins\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Q3: Analizar estadísticas de una ruta (Origen -> Destino).")
    parser.add_argument("origen", help="Código IATA de origen (ej. ATL)")
    parser.add_argument("destino", help="Código IATA de destino (ej. JFK)")
    
    args = parser.parse_args()
    
    try:
        connection = get_connection()
        start_time = time.time()
        query3_rutas(connection, args.origen.upper(), args.destino.upper())
        end_time = time.time()
        connection.close()
        print(f"Tiempo de ejecución: {round(end_time - start_time, 4)} segundos")
    except Exception as e:
        print(f"Error fatal conectando a HBase: {e}")
        sys.exit(1)
