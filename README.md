# Vuelos +
### Un análisis de los vuelos del primer cuatrimestre de 2008 en EE.UU

Este proyecto consiste en una canalización de datos interactiva (*ETL*, consultas por terminal y visualización) diseñada sobre una base de datos NoSQL columnar (**Apache HBase**). 
Su objetivo es almacenar, procesar y consultar de forma ultra-rápida y estructurada millones de registros de vuelos de aerolíneas usando el dataset histórico *Airline On-Time Performance*.

---

## Requisitos e Instalación

1. **Python 3.10+**.
2. **HBase Server** ejecutándose localmente (por ejemplo, vía contenedor Docker) y exponiendo el puerto `9090` (Thrift).
3. **Entorno Virtual**: Activa tu entorno virtual (`env_hbase` en Windows).
   ```powershell
   ..\env_hbase\Scripts\activate.bat
   ```
4. **Instalar dependencias**:
   ```powershell
   pip install pandas happybase streamlit pydeck
   ```

---

## Arquitectura ETL (`practice_hbase.py`)

Para inicializar la base de datos y realizar la carga inicial, ejecuta el módulo principal de práctica:

```powershell
python src\practice_hbase.py
```

**Estrategia de Modelado NoSQL utilizada:**
- **Pre-agregación**: En lugar de calcular el retraso o duración histórica de rutas *on-the-fly* (proceso costoso en HBase), se pre-agregan los datos calculando los promedios durante la inserción y subiéndolos como una tabla estática `rutas`.
- **Diseño de RowKey para Rangos Temporales**: En la inmensa tabla `vuelos`, la RowKey adopta el modelo `YYYYMMDD_Origen_Destino_Carrier_FlightNum`, lo que permite consultas instantáneas (Row Prefix Scans) tanto de días (`20080101`) como de meses enteros (`200801`).

---

## Consultas Interactivas por Consola (CLI)

Se han preparado 4 scripts dedicados para cubrir las 4 consultas clave requeridas por la documentación del ejercicio, todos con ayuda nativa (usa `-h` para ver las opciones disponibles en cada script).

### Q1: Búsqueda de Aeropuerto (Proyección de Columnas)
Permite observar todos los campos de un aeropuerto por defecto, o definir qué familias de columnas en particular se quieren ver (ahorrando tiempo de red).

```powershell
python src\query1_aeropuertos.py ATL -c info:city info:state
```
**Output:**
```text
=======================================================
Q1: Detalle del Aeropuerto ATL
Proyectando columnas: ['info:city', 'info:state']
=======================================================
  info:city: Atlanta
  info:state: GA
=======================================================
```

### Q2: Búsqueda de Vuelos por Fecha (Filtrado Row-Prefix)
Consulta los vuelos de una fecha (ej. todo el día 1 de Enero de 2008 o todo el mes enviando sólo `200801`). Soporta límite visual (`-l`) y filtro local por IATA origen (`-o`).

```powershell
python src\query2_vuelos.py 20080101 -o ATL -l 2
```
**Output:**
```text
=======================================================
Q2: Vuelos para la fecha/mes '20080101'
Filtro aplicado -> Origen: ATL
=======================================================
 ------------------------------------
  RowKey: 20080101_ATL_MCO_DL_1234
    Origen: ATL -> Destino: MCO
    Hora Salida: 08:30 | Llegada: 10:15
    Vuelo: 1234 | Aeronave: N12345
    Distancia: 400.0 millas
 ------------------------------------
  RowKey: 20080101_ATL_PHL_FL_888
    Origen: ATL -> Destino: PHL
    Hora Salida: 14:10 | Llegada: 16:20
    Vuelo: 888 | Aeronave: N90989
    Distancia: 666.0 millas

Mostrando 2 registro(s) (Límite visual: 2).
=======================================================
```

### Q3: Inteligencia y Estadísticas de Ruta
Obtiene la tabla pre-agregada analítica del ETL ordenando a la aerolínea con mayor volumen de vuelos de primero.

```powershell
python src\query3_rutas.py ATL JFK
```
**Output:**
```text
=======================================================
Q3: Estadísticas Analíticas de Ruta ATL-JFK
=======================================================
  Aerolínea: Delta Air Lines Inc. (DL)
      Frecuencia (Vuelos operados): 120
      Duración Promedio: 140.5 mins
      Retraso Salida: 15.2 mins
      Retraso Llegada: 12.1 mins

  Aerolínea: Comair Inc. (OH)
      Frecuencia (Vuelos operados): 40
      Duración Promedio: 144.3 mins
      Retraso Salida: 24.1 mins
      ...
```

### Q4: Auditoría Inteligente de DB
Este script realiza un conteo asombrosamente rápido incluso con millones de datos. Inyecta subrutinas nativas de Java dentro de HBase (`KeyOnlyFilter()`) de manera que **los datos del vuelo no viajan por la red**, devolviendo exclusivamente una señal al servidor intermedio disminuyendo los tiempos en gran medida.

```powershell
python src\query4_conteo.py
```
**Output:**
```text
Iniciando barrido de tablas. Para la tabla 'vuelos' esto puede tardar unos minutos...

=======================================================
Q4: Auditoría de Conteo de registros en HBase
    (usando KeyOnlyFilter() para evitar transferir valores)
=======================================================
> Tabla 'aeropuertos': 3.376 registros almacenados.
> Tabla 'companias': 1.490 registros almacenados.
> Tabla 'rutas': 4.981 registros almacenados.
> Tabla 'vuelos': 2.389.212 registros almacenados.
=======================================================
```

---

## Extensión Visual: Dashboard Interactivo en Streamlit

Para agregar una dimensión moderna y accesible al proyecto analítico, **la consola no es la única ventana**.
Puedes iniciar una interfaz gráfica potente mediante **Streamlit**, desarrollada adicional a los requisitos para enriquecer tu presentación:

```powershell
streamlit run src\streamlit_app.py
```

### Características Exclusivas del Panel Visual
1. **Filtros Dinámicos**: En la Pestaña Vuelos (Q2) puedes buscar por origen usando códigos IATA o por *Nombre Textual del Aeropuerto* con un combo-box.
2. **Control Interactivo**: En la Pestaña Rutas (Q3), el selector de destino está condicionado y encriptado dinámicamente; si seleccionas un aeropuerto que no tiene destinos operados registrados en la base de datos, las opciones de "destinos vacíos" se bloquean para evitar peticiones infructuosas.
3. **Conversión y Formateo Automático**: Genera duraciones precisas extraídas a partir de horarios crudos y convierte distancias métricas de Millas a Kms al instante.
4. **Capas Geográficas 3D (PyDeck)**: Adicionalmente, el panel carga las coordenadas espaciales `Lat` y `Long` en HBase, e imperceptivamente traza espectaculares **arcos curvos tridimensionales** para ilustrar masivamente todos los vuelos interceptados en Q2 sobre un mapamundi.
