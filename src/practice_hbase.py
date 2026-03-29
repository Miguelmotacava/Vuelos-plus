import pandas as pd
import happybase
import time
import os

HBASE_HOST = 'localhost'

def get_connection():
    # Intenta conectarse a HBase hasta que esté disponible
    for i in range(5):
        try:
            return happybase.Connection(HBASE_HOST, port=9090, timeout=1000000)
        except Exception as e:
            print(f"Esperando a HBase... ({i+1}/5)")
            time.sleep(5)
    raise Exception("No se pudo conectar a HBase en localhost:9090. Asegúrate de tenerlo levantado con Docker.")

def recreate_tables(conn: happybase.Connection):
    """
    Elimina las tablas existentes y las vuelve a crear vacías con sus respectivas 
    familias de columnas (Column Families), preparándolas para la carga de datos.
    
    Args:
        conn (happybase.Connection): Objeto de conexión hacia el cluster HBase.
    """
    tables = [b'aeropuertos', b'companias', b'vuelos', b'rutas']
    existing_tables = conn.tables()
    for t in tables:
        if t in existing_tables:
            print(f"Eliminando tabla existente {t.decode()}...")
            conn.disable_table(t)
            conn.delete_table(t)
            
    print("Creando tablas según el modelo diseñado...")
    conn.create_table('aeropuertos', {'info': dict()})
    conn.create_table('companias', {'info': dict()})
    # Tablas de vuelos tendrán info, route y time
    conn.create_table('vuelos', {'info': dict(), 'route': dict(), 'time': dict()})
    # Rutas tendrá stats pre-agregadas
    conn.create_table('rutas', {'stats': dict()})
    print("Tablas creadas exitosamente.")

def load_aeropuertos(conn: happybase.Connection):
    """
    Carga el listado de aeropuertos desde 'airports.csv' hacia la tabla 'aeropuertos'.
    Utiliza el código IATA como RowKey y agrupa todos sus metadatos en la familia de columnas 'info'.
    
    Args:
        conn (happybase.Connection): Objeto de conexión hacia HBase.
    """
    print("Cargando aeropuertos.csv...")
    df = pd.read_csv('../data/airports.csv', encoding='utf-8').fillna('')
    table = conn.table('aeropuertos')
    with table.batch(batch_size=1000) as b:
        for _, row in df.iterrows():
            iata_str = str(row['iata']).strip()
            if not iata_str:
                continue
            iata = iata_str.encode('utf-8')
            b.put(iata, {
                b'info:airport': str(row['airport']).encode('utf-8'),
                b'info:city': str(row['city']).encode('utf-8'),
                b'info:state': str(row['state']).encode('utf-8'),
                b'info:country': str(row['country']).encode('utf-8'),
                b'info:lat': str(row['lat']).encode('utf-8'),
                b'info:long': str(row['long']).encode('utf-8')
            })
    print(f"Cargados {len(df)} aeropuertos.")

def load_carriers(conn: happybase.Connection):
    """
    Carga el listado de aerolíneas desde 'carriers.csv' hacia la tabla 'companias'.
    La RowKey es el código de 2 letras de la aerolínea, almacenando la descripción
    en la familia de columnas 'info'.
    
    Args:
        conn (happybase.Connection): Objeto de conexión hacia HBase.
    """
    print("Cargando carriers.csv...")
    df = pd.read_csv('../data/carriers.csv', encoding='utf-8').fillna('')
    table = conn.table('companias')
    with table.batch(batch_size=500) as b:
        for _, row in df.iterrows():
            code_str = str(row['Code']).strip()
            if not code_str:
                continue
            code = code_str.encode('utf-8')
            b.put(code, {
                b'info:Description': str(row['Description']).encode('utf-8')
            })
    print(f"Cargadas {len(df)} compañías aéreas.")

def load_vuelos_and_rutas(conn: happybase.Connection, limit: int = None):
    """
    Proceso central ETL que parsea la inmensa tabla de registros de vuelos (2008.csv.bz2).
    Aplica una doble estrategia de modelado NoSQL:
    1. Pre-agrega métricas analíticas por Ruta-Aerolínea y las inserta en la tabla 'rutas' (útil para Q3).
    2. Inserta un registro individual para cada vuelo en la tabla 'vuelos', con una RowKey optimizada
       para búsquedas de rango temporales en Q2 y 3 Column Families (info, route, time).
    
    Args:
        conn (happybase.Connection): Objeto de conexión hacia HBase.
        limit (int, opcional): Si se define, lee solo las primeras N filas del CSV para acelerar tests.
    """
    print("Leyendo 2008.csv.bz2... (esto puede tardar por el tamaño del archivo comprimido)")
    cols = ['Year', 'Month', 'DayofMonth', 'DepTime', 'ArrTime', 'UniqueCarrier', 'FlightNum', 'Origin', 'Dest', 'Distance', 'AirTime', 'DepDelay', 'ArrDelay', 'TailNum']
    
    if limit:
        print(f"** MODO DESARROLLO: Limitando lectura a {limit} registros **")
        df = pd.read_csv('../data/2008.csv.bz2', usecols=cols, encoding='utf-8', nrows=limit)
    else:
        df = pd.read_csv('../data/2008.csv.bz2', usecols=cols, encoding='utf-8')
        
    print(f"Registros de vuelos leídos: {len(df)}")
    
    # 1. Pre-agregación de la tabla RUTAS
    print("Pre-agregando estadísticas de rutas para optimizar queries...")
    routes_df = df.groupby(['Origin', 'Dest', 'UniqueCarrier']).agg(
        avg_airtime=('AirTime', 'mean'),
        avg_depdelay=('DepDelay', 'mean'),
        avg_arrdelay=('ArrDelay', 'mean'),
        flights_count=('FlightNum', 'count')
    ).reset_index()
    
    table_rutas = conn.table('rutas')
    print("Insertando datos en tabla 'rutas'...")
    with table_rutas.batch(batch_size=2000) as b:
        for _, row in routes_df.iterrows():
            rk = f"{row['Origin']}_{row['Dest']}".encode('utf-8')
            carrier = row['UniqueCarrier']
            b.put(rk, {
                f'stats:{carrier}_avg_airtime'.encode('utf-8'): str(row['avg_airtime']).encode('utf-8'),
                f'stats:{carrier}_avg_depdelay'.encode('utf-8'): str(row['avg_depdelay']).encode('utf-8'),
                f'stats:{carrier}_avg_arrdelay'.encode('utf-8'): str(row['avg_arrdelay']).encode('utf-8'),
                f'stats:{carrier}_flights_count'.encode('utf-8'): str(row['flights_count']).encode('utf-8'),
            })
    print(f"Cargadas estadísticas para {len(routes_df)} combinaciones ruta-aerolínea.")

    # 2. Inserción en tabla VUELOS
    print("Cargando tabla 'vuelos' insertando registro por registro...")
    table_vuelos = conn.table('vuelos')
    df = df.fillna('')
    
    count = 0
    start_time = time.time()
    
    with table_vuelos.batch(batch_size=2000) as b:
        for _, row in df.iterrows():
            try:
                mm = str(int(row['Month'])).zfill(2)
                dd = str(int(row['DayofMonth'])).zfill(2)
                # Formato yyyyMMdd_Origin_Dest_Carrier_FlightNum
                rk = f"{row['Year']}{mm}{dd}_{row['Origin']}_{row['Dest']}_{row['UniqueCarrier']}_{row['FlightNum']}".encode('utf-8')
            except Exception:
                rk = f"UNKNOWN_{count}".encode('utf-8')
                
            b.put(rk, {
                b'info:FlightNum': str(row['FlightNum']).encode('utf-8'),
                b'info:TailNum': str(row['TailNum']).encode('utf-8'),
                b'route:Origin': str(row['Origin']).encode('utf-8'),
                b'route:Dest': str(row['Dest']).encode('utf-8'),
                b'route:Distance': str(row['Distance']).encode('utf-8'),
                b'time:DepTime': str(row['DepTime']).encode('utf-8'),
                b'time:ArrTime': str(row['ArrTime']).encode('utf-8')
            })
            count += 1
            if count % 100000 == 0:
                print(f"  -> Insertados {count} vuelos...")
                
    elapsed = round(time.time() - start_time, 2)
    print(f"Finalizada la carga de {count} vuelos en {elapsed}s.")

if __name__ == "__main__":
    print("--- INICIANDO PROCESO ETL PARA HBASE ---")
    conn = get_connection()
    recreate_tables(conn)
    load_aeropuertos(conn)
    load_carriers(conn)
    
    # IMPORTANTE: Si vas a presentar la práctica y quieres insertar TODOS los 7 millones
    # de vuelos, cambia limit=None.
    # Por defecto lo marcamos limit=50000 para que termine rápido en pruebas de desarrollo local.
    load_vuelos_and_rutas(conn, limit=None) 
    
    conn.close()
    print("--- ETL COMPLETADA CON ÉXITO ---")
