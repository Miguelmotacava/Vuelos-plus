"""
Script de consulta Q2: Seguimiento de Vuelos Flexible (HBase CLI).

Permite realizar búsquedas filtrando por Año, Mes, Día, Origen y Destino
usando filtros de servidor (RowFilter + regex) para máxima eficiencia.

Ejemplos de uso:
  python query2_vuelos.py --year 2008 --month 01 --day 15
  python query2_vuelos.py --origin JFK --dest LAX --limit 20
  python query2_vuelos.py --month 05 --origin ORD
"""

import happybase
import argparse
import sys
import time

HBASE_HOST = 'localhost'

def get_connection() -> happybase.Connection:
    return happybase.Connection(HBASE_HOST, port=9090)

def format_hhmm(t_str: str) -> str:
    if not t_str or t_str == 'nan': return '--:--'
    t = t_str.split('.')[0].zfill(4)
    return f"{t[:2]}:{t[2:]}" if len(t) == 4 else t_str

def query2_vuelos(year=None, month=None, day=None, origin=None, dest=None, limit=10):
    try:
        conn = get_connection()
        table = conn.table('vuelos')
        
        # 1. Construcción inteligente del Prefijo (solo si hay Año)
        prefix = ""
        filters = []
        
        if year:
            prefix = str(year)
            if month:
                prefix += str(month).zfill(2)
                if day:
                    prefix += str(day).zfill(2)
        else:
            if month:
                filters.append(f"RowFilter(=, 'regexstring:^.{{4}}{str(month).zfill(2)}')")
            if day:
                filters.append(f"RowFilter(=, 'regexstring:^.{{6}}{str(day).zfill(2)}')")

        # 2. Filtros de Aeropuerto
        if origin:
            filters.append(f"RowFilter(=, 'regexstring:^.{{9}}{origin.upper()}')")
        if dest:
            filters.append(f"RowFilter(=, 'regexstring:^.{{13}}{dest.upper()}')")

        # CABECERA DETALLADA SOLICITADA
        meses_nombres = {
            "01": "Enero", "02": "Febrero", "03": "Marzo", "04": "Abril",
            "05": "Mayo", "06": "Junio", "07": "Julio", "08": "Agosto",
            "09": "Septiembre", "10": "Octubre", "11": "Noviembre", "12": "Diciembre"
        }

        print("\n" + "="*70)
        print("Q2 - Vuelos mostrados para los siguientes filtros aplicados:")
        if year: print(f"  -> Año   : {year}")
        if month: print(f"  -> Mes   : {month} - {meses_nombres.get(str(month).zfill(2), 'Desconocido')}")
        if day: print(f"  -> Día   : {day}")
        if origin: print(f"  -> Origen: {origin.upper()}")
        if dest: print(f"  -> Destino: {dest.upper()}")
        if not any([year, month, day, origin, dest]):
            print("  -> (Sin filtros específicos aplicados)")
        print("="*70 + "\n")

        # 3. Combinación de filtros
        final_filter = None
        if filters:
            wrapped = [f"({f})" for f in filters]
            final_filter = " AND ".join(wrapped)

        scanner = table.scan(
            row_prefix=prefix.encode() if prefix else None,
            filter=final_filter.encode() if final_filter else None,
            limit=limit
        )

        count = 0
        for key, data in scanner:
            rk = key.decode()
            v_orig = data.get(b'route:Origin', b'').decode().strip()
            v_dest = data.get(b'route:Dest', b'').decode().strip()
            v_tail = data.get(b'info:TailNum', b'').decode().strip()
            v_num  = data.get(b'info:FlightNum', b'').decode().strip()
            v_dist = data.get(b'route:Distance', b'0').decode().strip()
            
            try:
                km = round(float(v_dist) * 1.609, 2)
            except:
                km = 0.0

            print(f" [Vuelo: {v_num}] | [Aeronave: {v_tail}]")
            print(f"   Ruta      : {v_orig} -> {v_dest}")
            print(f"   Tiempos   : {format_hhmm(data.get(b'time:DepTime', b'').decode())} (Salida) / {format_hhmm(data.get(b'time:ArrTime', b'').decode())} (Llegada)")
            print(f"   Distancia : {v_dist} millas (~{km} km)")
            print(f"   RowKey    : {rk}")
            print("-" * 60 + "\n")
            
            count += 1

        if count == 0:
            print("No se encontraron resultados para los filtros aplicados.")
        else:
            print(f"Total: {count} registros mostrados.")

        conn.close()
    except Exception as e:
        print(f"Error ejecutando consulta: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Consulta Q2 Flexible en HBase.")
    parser.add_argument("--year", help="Año (ej. 2008)")
    parser.add_argument("--month", help="Mes (01-12)")
    parser.add_argument("--day", help="Dia (01-31)")
    parser.add_argument("--origin", help="IATA Origen (ej. JFK)")
    parser.add_argument("--dest", help="IATA Destino (ej. LAX)")
    parser.add_argument("--limit", type=int, default=10, help="Limite de resultados")

    args = parser.parse_args()
    start_time = time.time()
    query2_vuelos(args.year, args.month, args.day, args.origin, args.dest, args.limit)
    end_time = time.time()
    print(f"Tiempo de ejecución: {round(end_time - start_time, 4)} segundos")
