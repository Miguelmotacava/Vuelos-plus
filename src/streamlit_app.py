import streamlit as st
import happybase
import pandas as pd
import os
import pydeck as pdk
import math

# Ocultar advertencias de Pandas
pd.options.mode.chained_assignment = None

# Resolver ruta del CSV relativa al directorio de este script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, '..', 'data')

# Configuracion de conexion a HBase — debe ir ANTES de las funciones que la usan
HBASE_HOST = 'localhost'

def haversine(lat1, lon1, lat2, lon2):
    """Calcula la distancia en grandes círculos entre dos puntos de la Tierra."""
    R = 6371.0 # Radio Tierra en km
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    return R * c

@st.cache_data
def get_routes_index():
    """
    Escanea solo las RowKeys de la tabla 'rutas' (KeyOnlyFilter) para construir
    un dict {origen_iata: [destino_iata, ...]} con todas las rutas almacenadas.
    Se cachea en sesion para no repetir el scan en cada interaccion.
    """
    try:
        conn = happybase.Connection(HBASE_HOST, port=9090)
        table = conn.table('rutas')
        scanner = table.scan(filter=b"KeyOnlyFilter()")
        routes = {}
        for key, _ in scanner:
            parts = key.decode().split('_', 1)
            if len(parts) == 2:
                orig, dest = parts
                routes.setdefault(orig, []).append(dest)
        conn.close()
        return {k: sorted(v) for k, v in sorted(routes.items())}
    except Exception:
        return {}

@st.cache_data
def get_iata_codes() -> list:
    """Extrae listado formateado (IATA - Nombre (Ciudad)) de los aeropuertos para los selectores."""
    try:
        airports_path = os.path.join(DATA_DIR, 'airports.csv')
        df = pd.read_csv(airports_path, usecols=['iata', 'airport', 'city', 'state'])
        df = df.dropna(subset=['iata'])
        df['iata'] = df['iata'].astype(str).str.strip()
        df['airport'] = df['airport'].fillna('Desconocido')
        df['city'] = df['city'].fillna('')
        df['state'] = df['state'].fillna('')
        df['display'] = df['iata'] + " - " + df['airport'] + " (" + df['city'] + ", " + df['state'] + ")"
        codes = df[df['iata'] != '']['display'].tolist()
        return sorted(codes)
    except Exception as e:
        st.warning(f"No se pudo cargar airports.csv: {e}")
        return ["ATL - Hartsfield Jackson (Atlanta, GA)", "JFK - John F Kennedy Intl (New York, NY)"]

@st.cache_data
def get_airport_coords() -> dict:
    """Extrae las coordenadas geoespaciales de cada aeropuerto para usar PyDeck."""
    try:
        airports_path = os.path.join(DATA_DIR, 'airports.csv')
        df = pd.read_csv(airports_path, usecols=['iata', 'lat', 'long'])
        df = df.dropna(subset=['iata'])
        df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
        df['long'] = pd.to_numeric(df['long'], errors='coerce')
        df = df.dropna()
        return df.set_index('iata')[['lat', 'long']].to_dict('index')
    except Exception:
        return {}

iata_list    = get_iata_codes()
routes_index = get_routes_index()

st.set_page_config(page_title="HBase Flights Dashboard", layout="wide")

st.title("Panel de Vuelos HBase")
st.markdown("Plataforma interactiva para el analisis de la base de datos de Viajes+.")

def test_connection():
    try:
        conn = happybase.Connection(HBASE_HOST, port=9090)
        conn.tables()
        conn.close()
    except Exception as e:
        st.error(f"Error conectando a HBase: {e}")
        st.info("Asegurate de tener el contenedor Docker encendido y mapeado al puerto 9090.")
        st.stop()

test_connection()

st.sidebar.title("Navegacion")
opciones = [
    "1. Detalles del Aeropuerto",
    "2. Seguimiento de Vuelos",
    "3. Analisis de Rutas",
    "4. Auditoria de Datos",
]
seleccion = st.sidebar.radio("Selecciona una consulta:", opciones)

# ==============================================================
# Q1 - AEROPUERTOS
# ==============================================================
if seleccion == opciones[0]:
    st.header("Q1 - Detalles del Aeropuerto")
    st.markdown("Obtén los detalles geograficos de cualquier aeropuerto. Puedes buscar por nombre, ciudad o codigo IATA y proyectar solo los atributos que necesites.")

    iata_selection = st.selectbox(
        "Aeropuerto (escribe el nombre o codigo IATA para buscar):",
        options=iata_list, index=None,
        placeholder="Ej. JFK, Los Angeles, LAX..."
    )

    COLUMNAS_DISPONIBLES = {
        "Nombre del Aeropuerto (airport)": b"info:airport",
        "Ciudad (city)":                   b"info:city",
        "Estado / Provincia (state)":      b"info:state",
        "Pais (country)":                  b"info:country",
        "Latitud (lat)":                   b"info:lat",
        "Longitud (long)":                 b"info:long",
    }

    with st.expander("Proyeccion de atributos (opcional — por defecto se devuelven todos)"):
        cols_seleccionadas = st.multiselect(
            "Selecciona los atributos que quieres mostrar:",
            options=list(COLUMNAS_DISPONIBLES.keys()),
            default=list(COLUMNAS_DISPONIBLES.keys()),
        )

    if st.button("Buscar Aeropuerto") and iata_selection:
        iata = iata_selection.split(" - ")[0].strip()
        columns_filter = [COLUMNAS_DISPONIBLES[c] for c in cols_seleccionadas] if cols_seleccionadas else None

        try:
            conn = happybase.Connection(HBASE_HOST, port=9090)
            table = conn.table('aeropuertos')
            row = table.row(iata.encode('utf-8'), columns=columns_filter)
            conn.close()

            if row:
                data = {k.decode().replace('info:', ''): v.decode() for k, v in row.items()}
                st.success(f"**Aeropuerto encontrado:** {data.get('airport', iata)}")

                items = list(data.items())
                mid = (len(items) + 1) // 2
                col1, col2 = st.columns(2)
                LABELS = {
                    'airport': 'Nombre', 'city': 'Ciudad', 'state': 'Estado / Provincia',
                    'country': 'Pais', 'lat': 'Latitud (grados decimales)', 'long': 'Longitud (grados decimales)'
                }
                with col1:
                    for k, v in items[:mid]:
                        st.write(f"**{LABELS.get(k, k)}:**", v.replace('USA', 'EE. UU.') if k == 'country' else v)
                with col2:
                    for k, v in items[mid:]:
                        st.write(f"**{LABELS.get(k, k)}:**", v)

                if columns_filter:
                    filtros_str = ", ".join(c.decode() for c in columns_filter)
                    st.caption(f"Proyeccion HBase aplicada: `columns=[{filtros_str}]`")
            else:
                st.warning("Aeropuerto no encontrado en la base de datos.")
        except Exception as e:
            st.error(f"Error de conexion con HBase (Posible sobrecarga): {e}")


# ==============================================================
# Q2 - SEGUIMIENTO DE VUELOS
# ==============================================================
elif seleccion == opciones[1]:
    st.header("Q2 - Seguimiento de Vuelos")
    st.markdown(
        "Busqueda flexible de vuelos por fecha y aeropuerto. "
        "Permite filtrar por cualquier combinacion de parametros utilizando tecnicas de filtrado en servidor."
    )

    col1, col2, col3 = st.columns(3)
    with col1:
        year_sel = st.selectbox("Año:", ["Todos", "2008"], index=1)
    with col2:
        meses_q2 = ["Todos"] + [str(i).zfill(2) for i in range(1, 13)]
        month_sel = st.selectbox("Mes:", meses_q2)
    with col3:
        dias_q2 = ["Todos"] + [str(i).zfill(2) for i in range(1, 32)]
        day_sel = st.selectbox("Dia:", dias_q2)

    col_orig, col_dest = st.columns(2)
    with col_orig:
        origen_q2 = st.selectbox(
            "Origen:",
            options=[None] + iata_list,
            index=0,
            format_func=lambda x: "Todos" if x is None else x,
            placeholder="Salida..."
        )
    with col_dest:
        destino_q2 = st.selectbox(
            "Destino:",
            options=[None] + iata_list,
            index=0,
            format_func=lambda x: "Todos" if x is None else x,
            placeholder="Llegada..."
        )

    limite = st.number_input(
        "Numero de resultados a mostrar:",
        min_value=1, max_value=1000, value=10
    )

    if st.button("Ejecutar Consulta"):
        # Construccion dinamica del prefijo y filtros
        prefix = ""
        filters = []  # type: list[str]
        
        # El prefijo solo es util si el Año esta definido
        if year_sel != "Todos":
            prefix = year_sel
            if month_sel != "Todos":
                prefix += month_sel
                if day_sel != "Todos":
                    prefix += day_sel
        else:
            # Si no hay año, usamos regex para mes/dia si estan presentes
            if month_sel != "Todos":
                filters.append(f"RowFilter(=, 'regexstring:^.{{4}}{month_sel}')")
            if day_sel != "Todos":
                filters.append(f"RowFilter(=, 'regexstring:^.{{6}}{day_sel}')")

        # Filtro de origen (posicion exacta 9 en la RowKey)
        if origen_q2:
            iata_orig = str(origen_q2.split(" - ")[0].strip())
            filters.append(f"RowFilter(=, 'regexstring:^.{{9}}{iata_orig}')")

        # Filtro de destino (posicion exacta 13 en la RowKey)
        if destino_q2:
            iata_dest = str(destino_q2.split(" - ")[0].strip())
            filters.append(f"RowFilter(=, 'regexstring:^.{{13}}{iata_dest}')")

        final_filter = " AND ".join(filters) if filters else None

        with st.spinner("Procesando consulta en HBase..."):
            try:
                conn = happybase.Connection(HBASE_HOST, port=9090)
                table = conn.table('vuelos')
                table_companias = conn.table('companias')
                
                # Agrupamos filtros con parentesis para mayor estabilidad si hay multiples
                final_filter_str = None
                if filters:
                    wrapped_filters = [f"({f})" for f in filters]
                    final_filter_str = " AND ".join(wrapped_filters)

                # Scan con filtros de servidor
                scanner = table.scan(
                    row_prefix=prefix.encode() if prefix else None,
                    filter=final_filter_str.encode() if final_filter_str else None,
                    limit=limite
                )
                
                airport_coords = get_airport_coords()
                vuelos_lista = []
                geodata = []
                carrier_names = {}

                for key, data in scanner:
                    try:
                        row_key = key.decode()
                        orig = data.get(b'route:Origin', b'').decode().strip()
                        dest = data.get(b'route:Dest', b'').decode().strip()
                        tail_num = data.get(b'info:TailNum', b'').decode().strip()
                        dist_mi = data.get(b'route:Distance', b'0').decode().strip()
                        flight_num = data.get(b'info:FlightNum', b'').decode().strip()
                        
                        try:
                            km = round(float(dist_mi) * 1.609, 2)
                        except:
                            km = 0.0

                        if orig and dest:
                            vuelos_lista.append({
                                "Vuelo": flight_num,
                                "Aeronave": tail_num,
                                "Origen": orig,
                                "Destino": dest,
                                "Distancia (Millas)": dist_mi,
                                "Distancia (Km)": km
                            })

                            # Preparacion de datos para el mapa
                            c_o = airport_coords.get(orig)
                            c_d = airport_coords.get(dest)
                            if c_o and c_d:
                                geodata.append({
                                    "s_lat": c_o['lat'], "s_lon": c_o['long'],
                                    "e_lat": c_d['lat'], "e_lon": c_d['long'],
                                    "orig": orig, "dest": dest,
                                    "info": f"Vuelo: {flight_num}<br>Aeronave: {tail_num}<br>Distancia: {km} Km"
                                })
                    except Exception as row_error:
                        # Si una fila falla, mostramos aviso tecnico ligero y continuamos con el resto
                        st.caption(f"Aviso: Se omitio una fila por error en datos: {row_error}")
                        continue

                conn.close()

                if vuelos_lista:
                    st.success(f"Consulta finalizada. Se encontraron {len(vuelos_lista)} registros.")
                    st.dataframe(pd.DataFrame(vuelos_lista), use_container_width=True)
                    
                    if geodata:
                        st.subheader("Visualizacion de Rutas")
                        df_geo = pd.DataFrame(geodata)
                        
                        # Capa de destinos para un "click" preciso en el punto final
                        df_dest = df_geo[['e_lat', 'e_lon', 'orig', 'dest', 'info']].copy()
                        df_dest.rename(columns={'e_lat': 'lat', 'e_lon': 'lon'}, inplace=True)

                        # Usamos CartoDB Voyager (no requiere token de Mapbox y es de estilo fisico/vial)
                        MAP_STYLE = 'https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json'
                        
                        st.pydeck_chart(pdk.Deck(
                            map_style=MAP_STYLE,
                            initial_view_state=pdk.ViewState(
                                latitude=df_geo['s_lat'].mean() if not df_geo.empty else 37.0,
                                longitude=df_geo['s_lon'].mean() if not df_geo.empty else -95.0,
                                zoom=3,
                                pitch=45
                            ),
                            layers=[
                                pdk.Layer(
                                    'ArcLayer',
                                    data=df_geo,
                                    get_source_position='[s_lon, s_lat]',
                                    get_target_position='[e_lon, e_lat]',
                                    get_source_color='[180, 50, 0, 160]',
                                    get_target_color='[0, 150, 50, 160]',
                                    get_width=3,
                                    pickable=True
                                ),
                                pdk.Layer(
                                    'ScatterplotLayer',
                                    data=df_dest,
                                    get_position='[lon, lat]',
                                    get_color='[0, 100, 255, 200]',
                                    get_radius=40000,
                                    pickable=True
                                )
                            ],
                            tooltip={
                                "html": """
                                    <div style="font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif; padding: 12px; background: white; border-radius: 8px; border: 1px solid #ddd; box-shadow: 0 4px 6px rgba(0,0,0,0.1); color: #333; line-height: 1.5;">
                                        <b style="color: #c62828; font-size: 1.1em; display: block; border-bottom: 1px solid #eee; margin-bottom: 8px; padding-bottom: 4px;">Especificaciones de la Ruta</b>
                                        <div style="margin-top: 5px;">
                                            <b>Origen:</b> {orig}<br/>
                                            <b>Destino:</b> {dest}<br/>
                                            {info}
                                        </div>
                                    </div>
                                """,
                                "style": {"backgroundColor": "transparent", "color": "white"}
                            }
                        ))
                else:
                    st.warning("No se encontraron resultados para los criterios seleccionados.")
            except Exception as e:
                st.error(f"Error técnico en la conexión con HBase: {e}")

# ==============================================================
# Q3 - RUTAS
# ==============================================================
elif seleccion == opciones[2]:
    st.header("Q3 - Analisis Estadistico de Rutas")
    st.markdown(
        "Selecciona primero el aeropuerto de **origen**: el selector de destino se filtrara "
        "automaticamente para mostrar solo los destinos con datos historicos desde ese origen."
    )

    # --- Selectores en dos columnas, siempre visibles ---
    iata_to_label = {item.split(" - ")[0].strip(): item for item in iata_list}
    origenes_disponibles = sorted(routes_index.keys()) if routes_index else []

    col1, col2 = st.columns(2)
    with col1:
        origen_iata = st.selectbox(
            "Aeropuerto de Origen:",
            options=origenes_disponibles,
            index=None,
            format_func=lambda code: iata_to_label.get(code, code),
            placeholder="Selecciona o escribe el codigo IATA...",
            help=f"{len(origenes_disponibles)} aeropuertos con rutas almacenadas."
        )
    with col2:
        if origen_iata:
            destinos_disponibles = routes_index.get(origen_iata, [])
            label_destino = f"Aeropuerto de Destino ({len(destinos_disponibles)} opciones desde {origen_iata}):"
        else:
            destinos_disponibles = sorted({d for dests in routes_index.values() for d in dests})
            label_destino = "Aeropuerto de Destino (selecciona primero un origen para filtrar):"

        destino_iata = st.selectbox(
            label_destino,
            options=destinos_disponibles,
            index=None,
            format_func=lambda code: iata_to_label.get(code, code),
            placeholder="Selecciona o escribe el codigo IATA..."
        )

    # --- Boton de analisis ---
    analizar = st.button("Analizar Ruta", disabled=(origen_iata is None or destino_iata is None))

    if analizar and origen_iata and destino_iata:
        rk = f"{origen_iata}_{destino_iata}".encode('utf-8')

        try:
            conn = happybase.Connection(HBASE_HOST, port=9090)
            table_rutas     = conn.table('rutas')
            table_companias = conn.table('companias')
            row = table_rutas.row(rk)

            if not row:
                conn.close()
                st.warning(
                    f"No hay estadisticas historicas para la ruta `{origen_iata}` -> `{destino_iata}`. "
                    f"(RowKey buscada: `{rk.decode()}`)"
                )
            else:
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

                resultados = []
                total_flights = 0.0
                sum_airtime_peso = 0.0
                sum_depdelay_peso = 0.0
                sum_arrdelay_peso = 0.0
                
                for carrier, stats in sorted_airlines:
                    # Acumular para los totales combinados
                    try:
                        f_count = float(stats.get('flights_count', 0))
                        if f_count > 0:
                            total_flights += f_count
                            sum_airtime_peso += float(stats.get('avg_airtime', 0)) * f_count
                            sum_depdelay_peso += float(stats.get('avg_depdelay', 0)) * f_count
                            sum_arrdelay_peso += float(stats.get('avg_arrdelay', 0)) * f_count
                    except Exception:
                        pass
                        
                    carrier_row  = table_companias.row(carrier.encode('utf-8'))
                    carrier_name = carrier_row.get(b'info:Description', b'').decode() if carrier_row else carrier
                    try:
                        duracion_promedio = round(float(stats.get('avg_airtime')), 2) if stats.get('avg_airtime') != 'N/A' else 'N/A'
                        retraso_salida    = round(float(stats.get('avg_depdelay')), 2) if stats.get('avg_depdelay') != 'N/A' else 'N/A'
                        retraso_llegada   = round(float(stats.get('avg_arrdelay')), 2) if stats.get('avg_arrdelay') != 'N/A' else 'N/A'
                    except Exception:
                        duracion_promedio = stats.get('avg_airtime')
                        retraso_salida    = stats.get('avg_depdelay')
                        retraso_llegada   = stats.get('avg_arrdelay')

                    resultados.append({
                        "Aerolinea":                   carrier_name,
                        "Codigo (Aerolinea)":          carrier,
                        "Frecuencia (Num. de Vuelos)": stats.get('flights_count', '0'),
                        "Duracion Promedio (min)":     duracion_promedio,
                        "Retraso de Salida (min)":     retraso_salida,
                        "Retraso de Llegada (min)":    retraso_llegada,
                    })

                conn.close()
                df_rutas = pd.DataFrame(resultados)
                df_rutas['Frecuencia (Num. de Vuelos)'] = pd.to_numeric(
                    df_rutas['Frecuencia (Num. de Vuelos)'], errors='coerce'
                )

                st.caption(f"Consulta HBase: `table_rutas.row(b'{rk.decode()}')`  + tabla `companias` para el nombre de cada aerolinea.")

                # --- Calculo de distancia entre los dos aeropuertos ---
                try:
                    c_orig = get_airport_coords().get(origen_iata)
                    c_dest = get_airport_coords().get(destino_iata)
                    dist_km = round(haversine(c_orig['lat'], c_orig['long'], c_dest['lat'], c_dest['long']), 2)
                except Exception:
                    dist_km = "N/A"

                st.subheader("Estadísticas Globales de la Ruta")
                if total_flights > 0:
                    c1, c2, c3, c4, c5 = st.columns(5)
                    c1.metric("Distancia", f"{dist_km} km" if dist_km != "N/A" else "N/A")
                    c2.metric("Vuelos Totales", f"{int(total_flights):,}".replace(',', '.'))
                    c3.metric("Duración Media", f"{round(sum_airtime_peso/total_flights, 2)} min")
                    c4.metric("Retraso Salida", f"{round(sum_depdelay_peso/total_flights, 2)} min")
                    c5.metric("Retraso Llegada", f"{round(sum_arrdelay_peso/total_flights, 2)} min")
                else:
                    st.warning("No hay registros suficientes para calcular estadisticas globales.")

                # --- Metricas de la aerolinea principal (la mas frecuente) ---
                top = df_rutas.iloc[0] if not df_rutas.empty else None
                if top is not None:
                    st.subheader(f"Aerolinea principal: {top['Aerolinea']} ({top['Codigo (Aerolinea)']})",
                                 help="La aerolinea con mas vuelos historicos en esta ruta.")
                    m1, m2, m3, m4 = st.columns(4)
                    m1.metric("Vuelos operados",     f"{int(top['Frecuencia (Num. de Vuelos)']):,}".replace(',', '.'))
                    m2.metric("Duracion media",      f"{top['Duracion Promedio (min)']} min")
                    m3.metric("Retraso salida",      f"{top['Retraso de Salida (min)']} min")
                    m4.metric("Retraso llegada",     f"{top['Retraso de Llegada (min)']} min")

                st.subheader("a) Duracion promedio del vuelo por aerolinea")
                st.dataframe(
                    df_rutas[["Aerolinea", "Codigo (Aerolinea)", "Frecuencia (Num. de Vuelos)", "Duracion Promedio (min)"]],
                    use_container_width=True
                )

                st.subheader("b) Retrasos medios por aerolinea")
                st.dataframe(
                    df_rutas[["Aerolinea", "Codigo (Aerolinea)", "Retraso de Salida (min)", "Retraso de Llegada (min)"]],
                    use_container_width=True
                )

                st.subheader("c) Tabla completa ordenada por frecuencia")
                st.dataframe(df_rutas, use_container_width=True)

        except Exception as e:
            st.error(f"Error de conexion con HBase (Posible sobrecarga): {e}")

# ==============================================================
# Q4 - CONTEO DE REGISTROS
# ==============================================================
elif seleccion == opciones[3]:
    st.header("Q4 - Auditoria de Datos HBase")
    st.markdown(
        "Conteo de registros (filas) en cada tabla usando un `KeyOnlyFilter` para minimizar "
        "la transferencia de datos por la red."
    )

    ESPERADOS = {
        "aeropuertos": "~3 376 aeropuertos (airports.csv)",
        "companias":   "~1 491 companias (carriers.csv)",
        "rutas":       "variable — combinaciones Origin-Dest unicas del 2008.csv",
        "vuelos":      "~7 millones de filas (todos los registros de 2008.csv)",
    }

    if st.button("Ejecutar Conteo de HBase"):
        with st.spinner("Realizando barrido en HBase... (puede tardar varios minutos para la tabla vuelos)"):
            tables = [b'aeropuertos', b'companias', b'rutas', b'vuelos']
            conteos = []

            try:
                conn = happybase.Connection(HBASE_HOST, port=9090)
                progress = st.progress(0)
                for i, t in enumerate(tables):
                    table   = conn.table(t)
                    scanner = table.scan(filter=b"KeyOnlyFilter()")
                    count   = sum(1 for _ in scanner)
                    conteos.append({
                        "Tabla":                      t.decode(),
                        "Registros alojados (filas)": count,
                        "Referencia esperada":        ESPERADOS.get(t.decode(), "—"),
                    })
                    progress.progress((i + 1) / len(tables))
                conn.close()

                df_conteos = pd.DataFrame(conteos)

                col1, col2 = st.columns([2, 1])
                with col1:
                    st.dataframe(df_conteos, use_container_width=True)
                with col2:
                    total = df_conteos['Registros alojados (filas)'].sum()
                    st.metric(
                        label="Total Volumen HBase",
                        value=f"{total:,} filas".replace(',', '.')
                    )
                    st.caption(
                        "Consulta HBase usada:\n"
                        "```python\ntable.scan(filter=b'KeyOnlyFilter()')\n```"
                    )
            except Exception as e:
                st.error(f"Error de conexion con HBase (Posible sobrecarga): {e}")