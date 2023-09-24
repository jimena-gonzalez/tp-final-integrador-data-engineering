#  Proyecto integrador curso Data Engineering UTNBA"
- Alumna: **Jimena B. Gonzalez**

### 1ra Entrega
##### Objetivo
En esta primera entrega se utiliza una **_API Publica_** con el fin de poner en practica la implementacion de **_técnicas de extracción de datos (de una fuente de datos)_** por medio del lenguaje de programación **_Python_**.
En el mismo se pone en practica tanto una **_extracción full_** como una **_extracción incremental_**.

##### Consideraciones propias
En esta ocasión se elige realizar una extracción, seleccionando como fuente de datos a una API. Ambos endpoints seleccionados pueden devolver datos temporales y actualizados, y disponibilizan parametros para filtrar por tiempo. Al momento de seleccionar la fuente de datos utilizada consideré justamente que la misma provea la cantidad de endpoints y el tipo de parametros necesarios para poder poner en practica los puntos requeridos por el enunciado del TP.

En el script se realizan ambos tipos de extracciones:
- **Extracción full:** Se realiza una primera extraccion full con el fin de consultar de todos los datos para un rango de tiempo dado (historico de terremotos).

- **Extracción incremental**: Luego se realiza una extraccion incremental en busqueda de obtener los eventos que fueron actualizados en las últimas 6 horas. Este paso se podría dejar croneado (en algun proceso batch, para la extraccion de los datos actualizados).

Para poner en practica ambos tipos de extracciones, se generó una función main() que invoca tanto a una extraccion full como a una extraccion incremental para ambos endpoints seleccionados. Como ya mencioné, es importante destacar que para sacar un mayor provecho de la extraccion incremental, la misma deberia encontrarse croneada para ser realizada cada "x" tiempo (en el ejemplo, cada 6 horas), a diferencia de la extraccion full que en el ejemplo tiene el objetivo de traer un historico de datos una unica vez.

### 2da Entrega
Notas de version:
- Script `JimenaGonzalez_Almacenamiento.py` ==> Se almacena los outputs (DataFrames), de la primera entrega, en formato **parquet**, agrupandolos en un esquema en cascada por fecha y hora. El objetivo de dividirlo por fecha y hora es para una mejor organización historica de los eventos, pensandolo a modo de "biblioteca". <br>*Aclaración*: No se realiza filtro de campos a almacenar. Se agregan campos de fecha con formato normalizado. 
- Script `JimenaGonzalez_Tablas.py` ==> Se crea una tabla en una base de datos de tipo **OLAP** (en este caso, en una BDD cloud **PostgreSQL**), a través de la librería **SQLAlchemy**. <br>*Aclaración*: En esta ocasión solo se crea una tabla y ciertos campos. No se la puebla. Aún no tiene conexion con respecto a lo realizado en el `script JimenaGonzalez_Almacenamiento.py`

Proximamente:
- Se espera poder adicionar las recomendaciones adicionales obtenidas de la devolución de la primera entrega (cruce con otra fuente para erniquecer los datos). Con esto seguramente se genere una nueva tabla o se adicionen campos a la actualmente creada. 

### 3ra Entrega
- Se implementan técnicas de procesamiento de datos (principalmente de limpieza y enriquecimiento de datos), utilizando Pandas,  para:
   - limpiarlos
   - estandarizarlos
   - enriquecerlos
   - obtencion de información relevante.
- Se cargan los resultados del procesamiento en una base de datos OLAP.

Notas de version:
- Se corrige estructura de directorios en "datalake", respecto a la entrega anterior. Nueva estructura:
   - datalake
      - landing
         - earthquake
            - Cantidad
            - Registros
               - Historial
               - Latest
- Se quita drop obligatorio del Script `JimenaGonzalez_Tablas.py`

- Se aplican las siguientes tareas de procesamiento (previo al almacenamiento en la DB) dispuestas en el Script `JimenaGonzalez_Transformaciones.py`:
   - Formatear columnas de tipo fecha: esta transformación se realizó en el landing, al momento de recibir los datos y almacenarlos en formato parquet.
   - Eliminación de nulos: Se eliminan los registros para los cuales el valor del campo a utilizar como agrupador (place) es nulo.
   - Agregaciones por medio de GROUP BY y funciones como MAX, MIN, AVG: Se calcula la magnitud minima, maxima y promedio por agrupada por locacion/place. ==> OK
   - Formateo de columnas: A modo de ejemplo se truncan valores del campo calculado magnitud_promedio a dos decimales.
   - Crear nuevas columnas a partir de alguna lógica: Se crea la columna impacto_maximo con el impacto maximo por locacion/place, en funcion del valor del campo magnitud_maxima.

- En este release, mediante la ejecucion del script `JimenaGonzalez_Transformaciones.py` se desencadena la ejecución de los scripts de las entregas previas (curl a API, creacion de tabla en la base de datos, etc.)

### Entrega Final

El proceso **Extracción --> Almacenamiento --> Procesamiento de datos** se desencadena al ejecutar el script `JimenaGonzalez_Transformar_y_CargarDB.py`. De todas formas, puede ser ejecutados ambos script por separado.

Notas de version:
- Ante el requisito de entrega de solo dos scripts, se realizan las siguientes modificaciones respecto entregas anteriores:
   - Se renombra el script `JimenaGonzalez_Almacenamiento.py` a `JimenaGonzalez_Extraer_y_Almacenar.py` ==> El mismo realiza la extracción de datos de la API y su almacenamiento en formato Parquet.
   - Se crea el script `JimenaGonzalez_Transformar_y_CargarDB` en reemplazo de  `JimenaGonzalez_Transformaciones.py` y `JimenaGonzalez_Tablas.py`. Este ultimo se reemplaza por requisito de la entrega, si bien se podria dejar por separado (quitando algunas funciones custom), como módulo estandar para operaciones sobre base de datos. ==> El mismo lee los datos almacenados del paso anterior, aplica transformaciones y realizar la carga en la base de datos OLAP.
- Se soluciona bug previo en carga de tabla.
- Se modularizan algunas funciones de operacion sobre la BD y renombran otras pre-existentes (por ejemplo, almacenar_particionado_por_fecha a almacenar_particionado).

### Próximos pasos
Proximamente (a posteriori de la finalización del curso), se realizarán ajustes y mejoras para sacar mayor provecho de los datos provistos por la API en cuestión.

## Dependencias (modulos requeridos)
 - Modulo Requests: Permite realizar peticiones http en Python. Requiere instalarlo mediante el siguiente comando: 
`pip install requests`
- Modulo Pandas:  Es util especialmente para el manejo y análisis de estructuras de datos. Requiere ser instalada mediante el siguiente comando: 
`pip install pandas`
- Modulo fastparquet: Se trata de una interface de Python para el uso del formato de archivos parquet (formato de archivo binario). Se puede instalar mediante el siguiente comando:  `pip install -q fastparquet`
- Modulo sqlalchemy: Es un ORM de Python que faciita la utilizacion y manipulacion de base de datos relacionales (SQL). Se puede instalar mediante el siguiente comando:  `pip install sqlalchemy==1.4.49t`
- Modulo psycopg[binary]: Adaptador para la utilización de una Base de Datos PostgreSQL en Python. Se puede instalar mediante el siguiente comando:  `pip install psycopg2-binary`

- Otros:
   - Módulo `datetime`: Es util especialmente para trabajar con fechas, cálculo y formato de las mismas.
   - Módulo `os`: Es util para el manejo y operacion de archivos y directorios en el filesystem.
   - Módulo `configparser`: Es util para trabajar con archivos de configuración (ej. para trabajar con un `config.ini`).

## Descripcion y documentacion API ##

La API elegida para realizar este trabajo es `https://earthquake.usgs.gov/fdsnws/event/1/` <br> <br> 
La misma permite consultar una serie de datos acerca de terremotos (earthquakes) en el territorio Estadounidense. <br> <br> 
La eleccion de esta API entre otras surgió al encontrar que la misma provee la cantidad de endpoints y el tipo de parametros necesarios para poner en practica los puntos requeridos en el enunciado del primer TP.

### Frecuencia de actualización de la API
Los datos se actualizan cada un minuto. 

### Autenticación
Esta API no requiere autenticarse para ser utilizada.

### Endpoints a utilizar:
En este TP se utilizarán solo los siguientes endpoints provistos en esta API:
##### Endpoint 1: "query"
El siguiente endpoint permite obtener (mediante una peticion `GET`) un conjunto de datos. `https://earthquake.usgs.gov/fdsnws/event/1/query`

Parametros:
Algunos parametros que pueden especificarse son:
|  Parametro | Descripcion | 
| ------- | ------- |
| **`?format={stringFormatoSalida}`** | Indica el formato de salida. Los valores posibles para _stringFormatoSalida_ son: `csv`; `geojson`; `text`; `xml`; etc. | 
| **`?endtime={stringFecha}`** | Permite obtener eventos hasta una cierta fecha limite. Utiliza el formato de fecha/tiempo ISO8601; se asume como zona horaria UTC salvo que se especifique otra. | 
| **`?starttime={stringFecha}`** | Permite obtener eventos desde una cierta fecha limite (hasta hace 30 días atrás). Utiliza el formato de fecha/tiempo ISO8601; se asume como zona horaria UTC salvo que se especifique otra. | 
| **`?updatedafter={stringFecha}`** | Permite obtener eventos que fueron actualizados luego de una cierta fecha limite. Utiliza el formato de fecha/tiempo ISO8601; se asume como zona horaria UTC salvo que se especifique otra. | 
| **`?latitude={gradosEnDecimal}`** | Filtra la ubicacion según una latitud determinada para realizar una busqueda circular en un radio dado. Rango de valores posibles: [-90,90]. | 
| **`?longitude={gradosEnDecimal}`** | Filtra la ubicacion según una longitud determinada para realizar una busqueda circular en un radio dado. Rango de valores posibles: [-180,180]. | 
| **`?maxradius={gradosEnDecimal}`** | Limita la busqueda a eventos con un grado de alerta determinado. Rango de valores posibles: `alertlevel=green`; `alertlevel=yellow`; `alertlevel=orange`; `alertlevel=red`. | 

Ejemplos de uso:
- https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime=2014-01-01&endtime=2014-01-02

##### Endpoint 2: "count"
El siguiente endpoint permite obtener (mediante una peticion `GET`) la cantidad de eventos o datos encontrados para una consulta determinada. `https://earthquake.usgs.gov/fdsnws/event/1/count`

Parametros:
Los parametros que pueden especificarse son los mismos que para `query`.
Su salida puede obtenerse solo en los siguientes formatos: `text` (texto plano - por defecto), `geojson`, `xml`.

## Bibliografia de Consulta ##
 - Sitio de donde se conoció la API: 
    - [Postman - Public REST APIs - Earthquakes
](https://www.postman.com/cs-demo/workspace/public-rest-apis/request/8854915-291c0d5d-9146-4591-b760-65e6db459f96)    

 - Documentación oficial de la API: [API Documentation - Earthquake Catalog
](https://earthquake.usgs.gov/fdsnws/event/1/)

 - Documentación GET modulo Request en Python: [Python requests: GET Request Explained](https://datagy.io/python-requests-get-request/)

 - Material de consulta:
   - _Curso Data Engineering UTNBA_: Notebook "CEL - Extracción - APIs.ipynb"
   - _Curso Data Engineering UTNBA_: Notebook "CEL_Data_Eng_Almacenamiento.ipynb"
   - _Curso Data Engineering UTNBA_: Notebook "CEL - Pipeline.ipynb"  
   - _Curso Data Engineering UTNBA_: Notebook "CEL_Data_Eng_Procesamiento_Ej1.ipynb"  

 - Clase Modulo 1 - Unidad 2 - Extraccion APIs - Jueves 27/07/2023 del _Curso Data Engineering UTNBA_

- [Pandas Groupby and Aggregate for Multiple Columns](https://datagy.io/pandas-groupby-multiple-columns/)
- [Create a new column in Pandas DataFrame based on the existing columns](https://www.geeksforgeeks.org/create-a-new-column-in-pandas-dataframe-based-on-the-existing-columns/)

- Contrastar/Verificar timestamp devuelto por la API y transformaciones de formato realizadas en el script: [Epoch Converter](https://www.epochconverter.com/)

 - Tabulación categorizacion magnitudes: [Earthquake magnitude](https://www.britannica.com/science/earthquake-geology/Earthquake-magnitude)
