## Script "JimenaGonzalez_Extraer_y_Almacenar.py" - "Trabajo final integrador" ##
__author__ = "Jimena B. Gonzalez"
__version__ = "1.0.1"

import requests as req
import pandas as pds
from datetime import datetime, timedelta, date
import os

### Funciones Genericas / Utilitarias ### 

def create_table(data):
    """
    A partir de datos en formato JSON, construye un DataFrame de Pandas.

    Parametros (args):
        :param data (JSON str) ==> Conjunto de datos en formato JSON.

    Output (returns):
        :return df (Pandas Dataframe) ==> Un DataFrame de Pandas con los datos estructurados.
    
    Raises:
        NotImplementedError ==>  Si se detectan parametros con un formato no soportado por Pandas para realizar las operaciones necesarias.
        Exception ==> Si se detectan otros tipos de errores en alguna de las operaciones realizadas.  
    """
    try:

        df = pds.json_normalize(data)

    except NotImplementedError as ex:
        raise NotImplementedError("ERROR! Formato de entrada no soportado por Pandas.")

    except Exception as ex:
        print("ERROR! Formato de entrada inesperado u erroneo. Asegurese de que se trate de un Json.")
        raise ex 
        
    return df

def extract_response_data_field(reponse, fieldToExtract=None):
    """
    Realiza una extraccion de un campo especifico de un json de entrada.

    Parametros (args):
        :param reponse          (JSON str) ==> Los datos obtenidos de la API en formato JSON.
        :param fieldToExtract   (str) ==> Campo que contienen los eventos y/o datos específicos de interés.

    Output (returns):
        :return df (Pandas Dataframe) ==> Los datos del campo deseado, en formato de Tabla (DataFrame de Pandas).
    
    Raises:
        Exception: Si se detectan errores en alguna de las operaciones realizadas.  
    """
    try:
        if(fieldToExtract is None):
            df = create_table(reponse)
        else:
            df = create_table(reponse[fieldToExtract]) 
    except Exception as ex:
        print("ERROR! No se pudo extraer los datos deseados.")
        raise ex
    
    return df

def normalize_date(df, fecha=None, dateFiledName='timestamp_measured', unitEpoch='s'): 
    """
    Realiza una normalización del campo timestamp en un dataframe dado.

    Parametros (args):
        :param df (Pandas Dataframe) ==>  Tabla/DataFrame de Pandas donde se encuentra el campo a normalizar
        :param fecha  (Datetime) ==> Fecha a utilizar en caso de que el 'df' no cuente con el campo debido. Por defecto, None.
        :param dateFiledName  (JSON str) ==> Nombre del campo de fecha a normalizar. Pir defecto, 'timestamp_measured'.
        :param unitEpoch (str) ==> Campo que contiene unidad en que se encuentra el time en epoch (ej. 's'=segundos, 'ms'=milisegundos). Por defecto 's'.

    Output (returns):
        :return df (Pandas Dataframe) ==> El dataframe origen, con el campo 'timestamp_measured' normalizado.
    """
    if(isDataframe(df)):
        if(fecha is None):

            df['timestamp_measured'] = df[dateFiledName]
            df['timestamp_measured'] = pds.to_datetime(df.timestamp_measured, unit=unitEpoch)

            df["fecha"] = df.timestamp_measured.dt.date
            df["hora"] = df.timestamp_measured.dt.hour

        else:

            df['timestamp_measured'] = fecha
            df["fecha"] = df.timestamp_measured.dt.date
            df["hora"] = df.timestamp_measured.dt.hour

    else:
        print("No se pueden normalizar los datos.")
        return None
    
    return df

def almacenar_particionado(df, path, fecha=None, dateFiledName='timestamp_measured',unitEpoch='s', tipoParticion="fyh"): 
    """
    A partir de un DataFrame de Pandas, lo almacena en formato parquet, en una estructura de directorios segmentada por fecha y hora de la medición y/o calculo.

    Parametros (args):
        :param df (Pandas.Dataframe) ==> Dataframe que contiene las columnas a almacenar
        :path (str) ==> Path del directorio donde almacenar los archivos parquet
        :param fecha (Datetime) ==> Hora a colocar en caso de no contar con la misma como dato.
        :param dateFiledName  (JSON str) ==> Nombre del campo de fecha a normalizar. Pir defecto, 'timestamp_measured'.
        :param unitEpoch (str) ==> Campo que contiene unidad en que se encuentra el time en epoch (ej. 's'=segundos, 'ms'=milisegundos). Por defecto 's'.
        :param tipoParticion (str) ==> Indica el tipo de particionado a realizar. Por defecto fecha y hora (fyh). Los valores posibles son: fyh = fecha y hora; f = fecha; h = hora

    Output (returns):
        :return None ==> Sin retorno
    """
    df=normalize_date(df, fecha, dateFiledName, unitEpoch)

    if tipoParticion == "fyh":
        partition_cols = ["fecha", "hora"]
    elif tipoParticion == "f":
        partition_cols = ["fecha"]
    elif tipoParticion == "h":
        partition_cols = ["hora"]
    else:
        partition_cols = ["fecha", "hora"]
   
    if df is not None: 
        df_to_parquet(df, f"{path}", partition_cols)

    return None

def isDataframe(param):
    """
    Evalua si el argumento recibido se encuentra o no en formato DatFrame de Pandas.

    Parametros (args):
        :param param (Pandas.DataFrame): El DataFrame que se desea evaluar u otro elemento
    
    Output (returns):
        :return True ==> Si 'param' SI es un DataFrame de Pandas
        :return False ==> Si 'param' NO es un DataFrame de Pandas
    """
    # Verificar que df sea un DataFrame
    if not isinstance(param, pds.DataFrame):
        return False
    return True

def is_Str_Or_StrList(param):
    """
    Evalua si el argumento recibido es un string o una lista de strings.

    Parametros (args):
        :param param (str o str list): El elemento a evaluar
    
    Output (returns):
        :return True (Boolean) ==> Si 'param' SI es un String o Lista de Strings
        :return False (Boolean)  ==> Si 'param' NO es un String o Lista de Strings
    """
    # Verificar que df sea un DataFrame
    if param is not None and not isinstance(param, (str, list)):
        return False
    return True

def crear_directorio(path):
    """
    Crear un directorio en un path especifico, en caso que no exista.

    Parametros (args):
        :param path (str): El path con el nombre del directorio a crear
    
    Output (returns):
        :return None ==> Sin retorno
    """
    # Crear directorio si no existe
    directory = os.path.dirname(path)

    if directory and not os.path.exists(directory):
        os.makedirs(directory)

def df_to_parquet(df, path, partition_col=None):
    """
    Guardar un DataFrame en formato Parquet en en path especificado.

    Parametros (args):
        :param df (Pandas.DataFrame) ==>  El DataFrame que se desea guardar.
        :param path (str) ==>  La ruta donde se guardará el archivo Parquet.
        :param partition_col (str or list, optional) ==>  Columna(s) por la cual particionar los datos en el formato Parquet.
    
    Output (returns):
        :return None ==> Sin retorno. La salida son los archivo almacenados en "path".

    Raises:
        TypeError ==>  Si el argumento df no es un DataFrame de pandas.
        ValueError ==>  Si partition_col no es None ni una cadena o lista de cadenas.
    """

    ##Control de tipos
    if isDataframe(df) is False:
        raise TypeError("El argumento 'df' debe ser un DataFrame de pandas.")

    if is_Str_Or_StrList(partition_col) is False:
        raise ValueError("El argumento 'partition_col' debe ser un string o una lista de strings.")

    crear_directorio(path)

    try:
        # Guardar el DataFrame en formato Parquet
        df.to_parquet(path, partition_cols=partition_col)
        print("DataFrame guardado exitosamente en formato Parquet.")
    except Exception as e:
        print(f"Error al guardar el DataFrame en formato Parquet: {str(e)}")

def print_parquet(path):
    """
    Imprime un archivo parquet en caso de que exista.

    Parametros (args):
        :param path (str) ==>  El path con el nombre del archivo a leer
    
    Output (returns):
        :return None ==> Sin retorno. La salida es la impresion por pantalla del contenido del archivo "parquet".
    """
    print(read_parquet(path))

def read_parquet(path):
    """
    Lee un archivo parquet en caso de que exista.

    Parametros (args):
        :param path (str) ==>  El path con el nombre del archivo a leer
    
    Output (returns):
        :return df (Pandas Dataframe) ==> Los datos leidos, en formato de Tabla (DataFrame de Pandas).
    """
    if os.path.exists(path):
        return pds.read_parquet(path)
    else:
        print("\nEl archivo o path deseado es inexistente.")

### Métodos de Consulta ### 

def get_response_data(url_base, endpoint, params=None):
    """
    Realiza una solicitud GET a una API REST para obtener datos.

    Parametros (args):
        :param url_base (str) ==> URL base de la API.
        :param endpoint (str) ==> Endpoint (ruta) de la API para obtener datos específicos.
        :param params   (Python Dict) ==> Parámetros de la solicitud GET.

    Output (returns):
        :return response_data (JSON str) ==> Datos obtenidos de la API REST, en formato JSON.
    """

    try:

        ## Se constituye la url del target
        if((url_base is not None) and (url_base[-1]!='/')):
            url_endpoint = f"{url_base}/{endpoint}"
        else: 
           url_endpoint = f"{url_base}{endpoint}"

        ## Se realiza la peticion GET a la API
        response = req.get(url_endpoint, params=params) 
        # response_url = print(response.url)
        # response_status_code = response.status_code
        
        ## Si se detecta un error (ej. status_code!=200) en la HTTP Response, se lanza una excepcion.
        response.raise_for_status()

        try:
            response_data = response.json() 
 
        except ValueError as ex:
            print(f"ERROR! Formato de respuesta inesperado. Asegurese de que se trate de un Dict o Json.\nDetalle del error: {ex}")
            return None
        
        return response_data

    except req.exceptions.RequestException as ex:

        print(f"ERROR al realizar la peticion.\nCódigo de error: {ex}")
    
        return None
    
    except Exception as ex:
         
        print(f"ERROR!: {ex}")

        return None

### Métodos de extraccion especifica ### 

def extraccion_full(url_base, endpoint, params, fieldToExtract=None): 
    """
    Realiza una solicitud GET a una API REST para obtener datos todos los datos sin inportar la fecha de actualización.

    Parametros (args):
        :param url_base (str) ==> URL base de la API.
        :param endpoint (str) ==> Endpoint (ruta) de la API para obtener datos específicos.
        :param params   (Python Dict) ==> Parámetros de la solicitud GET.
        :param fieldToExtract (str) ==> Campo que contienen los eventos y/o datos específicos de interés.

    Output (returns):
        :return df_result (Pandas Dataframe) ==> Los datos del campo deseado, en formato de Tabla (DataFrame de Pandas).

    Raises:
        Exception ==> Si se detectan errores en alguna de las operaciones realizadas.   
    """
    try:
        result = get_response_data(url_base, endpoint, params)
        df_result = extract_response_data_field(result, fieldToExtract)
    
    except Exception as ex:
        print(f"ERROR!: {ex}")
        raise ex

    return df_result ##EXTRACCION FULL EXITOSA => print(df_result.head())

def extraccion_incremental(url_base, endpoint, parameters=None, fieldToExtract=None, deltaHoras=1):
    """
    Realiza una extraccion incremental/parcial a partir de un GET a una API dada para obtener datos sólo actualizados.
    Se basa en la fecha de actualizacion de los eventos a extraer. 

    Parametros (args):
        :param url_base (str) ==> URL base de la API.
        :param endpoint (str) ==> Endpoint (ruta) de la API para obtener datos específicos.
        :param params (Dict) ==> Parámetros de la solicitud GET.
        :param fieldToExtract (str) ==> Nombre del campo donde se encuentran los datos de interés.
        :param deltaHoras (int) ==> Numero que represente la Diferencia de tiempo respecto a hora.

    Output (returns):
        :return df_result (Pandas Dataframe) ==> Datos obtenidos de la API (y/o del campo fieldToExtract de response de la misma), en formato Dataframe. Los eventos será aquellos actualizados en las últimas deltaHoras horas.

    Raises:
        Exception ==> Si se detectan errores en alguna de las operaciones realizadas.  

    Comentarios: La función podría ser mejorada para recibir un listado de campos fechas a utilizar en la extraccion.
    """

    ## Se calcula un timestamp de hace XXhs para realizar una extraccion
    start_date = datetime.utcnow() - timedelta(hours=deltaHoras) 

    updatedafter = start_date.strftime("%Y-%m-%dT%H:00:00Z")

    dynamic_parameters = { "updatedafter": f"{updatedafter}"  }

    try:
        params = {**parameters , **dynamic_parameters}

        result = get_response_data(url_base, endpoint, params)

        df_result = extract_response_data_field(result, fieldToExtract)
    
    except Exception as ex:
        print(f"ERROR!: {ex}")
        raise ex

    return df_result ## EXTRACION INCREMENTAL OK => print(df_result.head())


### Funcion principal del Script ### 

def main():
    """
    Funcion principal del Script para modularizar el código.
    En la misma se envian la mayor parte de parametros propios de la API en cuestion.

    Parametros (args):
        :param (None)

    Output (returns):
        :return (None) 
    """

    aux_nro_consulta = 0; 
    url_base = "https://earthquake.usgs.gov/fdsnws/event/1/"

    endpoints = [ 
        {"endpoint": "query", "params": {'format': 'geojson', 'latitude':34,'longitude':-118, 'maxradius': 5}, "fieldToExtract": "features"}, 
        {"endpoint": "count", "params": {'format': 'geojson', 'latitude':34,'longitude':-118, 'maxradius': 5}, "fieldToExtract": None }
    ]

    for ep in endpoints:
        aux_nro_consulta += 1
        print(f'\n###### Consulta Nro. {aux_nro_consulta} ######\n')
        try:

            df_extraccion_full=extraccion_full(url_base, ep["endpoint"], ep["params"], ep["fieldToExtract"])

            df_extraccion_incremental=extraccion_incremental(url_base, ep["endpoint"], ep["params"], ep["fieldToExtract"], 6)

            if(ep["endpoint"]=="query"):
                
                almacenar_particionado(df_extraccion_full, "Output/datalake/landing/earthquake/Registros/Historial/terremotos-historial.parquet",dateFiledName='properties.time',unitEpoch='ms', tipoParticion='f')
                print_parquet("Output/datalake/landing/earthquake/Registros/Historial/terremotos-historial.parquet")

                almacenar_particionado(df_extraccion_incremental, "Output/datalake/landing/earthquake/Registros/Latest/ultimos-terremotos.parquet",dateFiledName='properties.time',unitEpoch='ms', tipoParticion='f')
                print_parquet("Output/datalake/landing/earthquake/Registros/Latest/ultimos-terremotos.parquet")

            if(ep["endpoint"]=="count"):     
                
                almacenar_particionado(df_extraccion_full, "Output/datalake/landing/earthquake/Cantidad/cant-ult-30dias.parquet", fecha=datetime.utcnow(), tipoParticion='f')
                print_parquet("Output/datalake/landing/earthquake/Cantidad/cant-ult-30dias.parquet")
                
                almacenar_particionado(df_extraccion_incremental, "Output/datalake/landing/earthquake/Cantidad/cant-actualizados-ult-6hs.parquet", fecha=datetime.utcnow(), tipoParticion='f')
                print_parquet("Output/datalake/landing/earthquake/Cantidad/cant-actualizados-ult-6hs.parquet")

        except Exception as ex:
            print(f"ERROR! No se pudo concluir la operacion en base al endpoint solicitado: {str(ex)}")

main()