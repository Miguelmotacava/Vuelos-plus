# Vuelos +
### Un análisis de los vuelos del primer cuatrimestre de 2008 en EE.UU

Este proyecto consiste en una canalización de datos interactiva (*ETL*, consultas por terminal y visualización) diseñada sobre una base de datos NoSQL columnar (**Apache HBase**). 
Su objetivo es almacenar, procesar y consultar de forma ultra-rápida y estructurada millones de registros de vuelos de aerolíneas usando el dataset histórico *Airline On-Time Performance*.

---

## Estructura del Proyecto

El repositorio está organizado de la siguiente manera:

- **`src/`**: Contiene el núcleo de la lógica en Python (ETL, scripts de consulta CLI y la aplicación Streamlit).
- **`data/`**: Directorio para los archivos CSV (Airports, Carriers y Vuelos). *Nota: Los archivos pesados están omitidos en Git.*
- **`informe/`**: Contiene el reporte final de la práctica en formato PDF, deonde se comentan el modelo adoptado en HBase, las tablas creadas, las queries creadas y sus resultados, etc, ...
- **`docker-compose.yml`**: Configuración para levantar el ecosistema de HBase, Thrift y Zookeeper mediante contenedores.
- **`requirements.txt`**: Listado de dependencias de Python necesarias para ejecutar el proyecto.

---

## Requisitos e Instalación

1. **Python 3.10+**.
2. **HBase Server** ejecutándose localmente (por ejemplo, vía contenedor Docker) y exponiendo el puerto `9090` (Thrift).
3. **Entorno Virtual**: Activa tu entorno virtual (`env_hbase` en Windows).
   ```powershell
   ..\env_hbase\Scripts\activate
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
python query1_aeropuertos.py ATL
```
**Output:**
```text
=======================================================
Q1 - Detalle del Aeropuerto ATL
=======================================================
  Aeropuerto -> William B Hartsfield-Atlanta Intl
  Ciudad      -> Atlanta
  País        -> USA
  Latitud     -> 33.64044444
  Longitud    -> -84.42694444
  Estado      -> GA
=======================================================

Tiempo de ejecución: 0.0477 segundos
```

### Q2: Búsqueda de Vuelos por Fecha (Filtrado Row-Prefix)
Consulta los vuelos de una fecha (ej. todo el día 1 de Enero de 2008 o todo el mes enviando sólo `200801`). Soporta límite visual (`-l`) y filtro local por IATA origen (`-o`).

```powershell
python query2_vuelos.py --year 2008 --month 01 --day 15 --origin ATL
```
**Output:**
```text
======================================================================
Q2 - Vuelos mostrados para los siguientes filtros aplicados:
  -> Año   : 2008
  -> Mes   : 01 - Enero
  -> Día   : 15
  -> Origen: ATL
======================================================================

 [Vuelo: 4795] | [Aeronave: N881AS]
   Ruta      : ATL -> ABE
   Tiempos   : 14:53 (Salida) / 16:47 (Llegada)
   Distancia : 692 millas (~1113.43 km)
   RowKey    : 20080115_ATL_ABE_EV_4795
------------------------------------------------------------

 [Vuelo: 5184] | [Aeronave: N420CA]
   Ruta      : ATL -> ABE
   Tiempos   : 21:17 (Salida) / 23:19 (Llegada)
   Distancia : 692 millas (~1113.43 km)
   RowKey    : 20080115_ATL_ABE_OH_5184
------------------------------------------------------------

 [Vuelo: 1433] | [Aeronave: N915DL]
   Ruta      : ATL -> ABQ
   Tiempos   : 11:18 (Salida) / 12:46 (Llegada)
   Distancia : 1269 millas (~2041.82 km)
   RowKey    : 20080115_ATL_ABQ_DL_1433
------------------------------------------------------------

 [Vuelo: 1540] | [Aeronave: N930DL]
   Ruta      : ATL -> ABQ
   Tiempos   : 21:36 (Salida) / 23:12 (Llegada)
   Distancia : 1269 millas (~2041.82 km)
   RowKey    : 20080115_ATL_ABQ_DL_1540
------------------------------------------------------------

 [Vuelo: 567] | [Aeronave: N980DL]
   Ruta      : ATL -> ABQ
   Tiempos   : 17:36 (Salida) / 19:08 (Llegada)
   Distancia : 1269 millas (~2041.82 km)
   RowKey    : 20080115_ATL_ABQ_DL_567
------------------------------------------------------------

 [Vuelo: 4361] | [Aeronave: N849AS]
   Ruta      : ATL -> ABY
   Tiempos   : 22:16 (Salida) / 22:56 (Llegada)
   Distancia : 146 millas (~234.91 km)
   RowKey    : 20080115_ATL_ABY_EV_4361
------------------------------------------------------------

 [Vuelo: 4468] | [Aeronave: N936EV]
   Ruta      : ATL -> ABY
   Tiempos   : 09:19 (Salida) / 10:13 (Llegada)
   Distancia : 146 millas (~234.91 km)
   RowKey    : 20080115_ATL_ABY_EV_4468
------------------------------------------------------------

 [Vuelo: 4689] | [Aeronave: N848AS]
   Ruta      : ATL -> ABY
   Tiempos   : 16:01 (Salida) / 16:47 (Llegada)
   Distancia : 146 millas (~234.91 km)
   RowKey    : 20080115_ATL_ABY_EV_4689
------------------------------------------------------------

 [Vuelo: 4792] | [Aeronave: N839AS]
   Ruta      : ATL -> ACY
   Tiempos   : 20:47 (Salida) / 22:32 (Llegada)
   Distancia : 678 millas (~1090.9 km)
   RowKey    : 20080115_ATL_ACY_EV_4792
------------------------------------------------------------

 [Vuelo: 4362] | [Aeronave: N929EV]
   Ruta      : ATL -> AEX
   Tiempos   : 10:24 (Salida) / 11:08 (Llegada)
   Distancia : 500 millas (~804.5 km)
   RowKey    : 20080115_ATL_AEX_EV_4362
------------------------------------------------------------

Total: 10 registros mostrados.
Tiempo de ejecución: 0.0385 segundos
```

### Q3: Inteligencia y Estadísticas de Ruta
Obtiene la tabla pre-agregada analítica del ETL ordenando a la aerolínea con mayor volumen de vuelos de primero.

```powershell
python query3_rutas.py ATL JFK
```
**Output:**
```text
=======================================================
Q3 - Estadísticas Analíticas de Ruta ATL-JFK
=======================================================

Ruta: ATL -> JFK

Distancia Estimada (Geográfica): 1221.73 km

--- ESTADÍSTICA GLOBAL DE LA RUTA ---
  Total de Aerolíneas Operando: 2
  Total de Vuelos en la Ruta:   626
  Promedio AirTime General:     104.36 mins
  Promedio Retraso Salida:      14.05 mins
  Promedio Retraso Llegada:     11.87 mins
-------------------------------------

Desglose por Aerolínea:
  Aerolínea: Delta Air Lines Inc. (DL)
      Frecuencia (Vuelos operados): 507
      Duración Promedio: 104.7 mins
      Retraso Salida:    11.17 mins
      Retraso Llegada:   9.12 mins

  Aerolínea: Comair Inc. (OH)
      Frecuencia (Vuelos operados): 119
      Duración Promedio: 102.95 mins
      Retraso Salida:    26.29 mins
      Retraso Llegada:   23.61 mins

Tiempo de ejecución: 0.0288 segundos
```

### Q4: Auditoría Inteligente de DB
Este script realiza un conteo asombrosamente rápido incluso con millones de datos. Inyecta subrutinas nativas de Java dentro de HBase (`KeyOnlyFilter()`) de manera que **los datos del vuelo no viajan por la red**, devolviendo exclusivamente una señal al servidor intermedio disminuyendo los tiempos en gran medida.

```powershell
python query4_conteo.py
```
**Output:**
```text
Iniciando barrido de tablas. Para la tabla 'vuelos' esto puede tardar unos minutos...

=======================================================
Q4 - Auditoría de Conteo de registros en HBase
(usando KeyOnlyFilter() para evitar transferir valores)
=======================================================

> Tabla 'aeropuertos': 3.376 registros almacenados.
> Tabla 'companias': 1.490 registros almacenados.
> Tabla 'rutas': 4.981 registros almacenados.
> Tabla 'vuelos': 2.389.212 registros almacenados.


Tiempo de ejecución total: 48.705 segundos
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

<<<<<<< HEAD
=======
---

## Referencias y Enlaces Útiles

Para la reproducción de este proyecto y consulta técnica, se pueden utilizar los siguientes recursos:

- **Datasets Originales (Harvard Dataverse):**
    - [Dataset de Aeropuertos](https://dataverse.harvard.edu/file.xhtml?persistentId=doi:10.7910/DVN/HG7NV7/XTPZZY&version=1.0)
    - [Dataset de Compañías](https://dataverse.harvard.edu/file.xhtml?persistentId=doi:10.7910/DVN/HG7NV7/3NOQ6Q&version=1.0)
    - [Dataset de Rutas](https://dataverse.harvard.edu/file.xhtml?persistentId=doi:10.7910/DVN/HG7NV7/EIR0RA&version=1.0)
- **Documentación Técnica:**
    - [HappyBase Documentation](https://happybase.readthedocs.io/en/latest/)
    - [HBase Reference Guide](https://hbase.apache.org/book.html)

---

>>>>>>> 3ba8f8b (Finalizing HBase project: Updated README with dataset references, added execution timers to all queries, and humanized CLI outputs.)
A continuación se muestran las capturas de cómo se vería el streamlit ejecutado:

**Q1 - Detalles de los aeropuertos**

https://github.com/user-attachments/assets/6635e5e5-6761-4a80-a53b-869b8a3b2c79

<<<<<<< HEAD


**Q2 - Q2 - Seguimiento de Vuelos**

https://github.com/user-attachments/assets/80cc8d34-df56-4bf0-970e-71098e7fd67b



**Q3 - Q3 - Analisis Estadistico de Rutas**

https://github.com/user-attachments/assets/51f8c6c2-df8a-4d67-9137-3c43c2fb54f0



**Q4 - Q4 - Auditoria de Datos HBase**
=======
**Q2 - Seguimiento de Vuelos**

https://github.com/user-attachments/assets/80cc8d34-df56-4bf0-970e-71098e7fd67b

**Q3 - Analisis Estadistico de Rutas**

https://github.com/user-attachments/assets/51f8c6c2-df8a-4d67-9137-3c43c2fb54f0

**Q4 - Auditoria de Datos HBase**
>>>>>>> 3ba8f8b (Finalizing HBase project: Updated README with dataset references, added execution timers to all queries, and humanized CLI outputs.)

https://github.com/user-attachments/assets/ef739b44-7a2d-4426-8136-09d28a9a2785



