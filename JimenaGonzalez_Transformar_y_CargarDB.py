## Script "JimenaGonzalez_Transformar_y_CargarDB.py" - "Trabajo final integrador" ##
__author__ = "Jimena B. Gonzalez"
__version__ = "1.0.0"

import pandas as pds
import os
import sqlalchemy as sa
from configparser import ConfigParser
import JimenaGonzalez_Extraer_y_Almacenar as JG_Alm

### Utilitarios Operaciones BD ###  
def connect_to_database(motor="postgres", config_file_path="config.ini", config_file_path_section="postgres"):
    """
    Establece una conexión a una base de datos utilizando la configuración especificada en un archivo dado.

    Parametros (args):
        :param motor (str, optional) ==> Nombre del motor correspondiente a la base de datos a donde nos coenectaremos. Por defecto, "postgres".
        :param config_file_path (str, optional) ==> Ruta del archivo de configuración INI.  Por defecto, "config.ini".
        :param config_file_path_section (str, optional) ==> Nombre de la sección en el archivo que contiene los datos de conexión.

    Output (returns):
        :return conexion (sqlalchemy.engine.Engine) ==> La conexión a la base de datos segun motor elegido. Por defecto, "postgres".
    """
    conexion = None
    if motor=="postgres":
        conexion=connect_to_postgres(config_file_path, config_file_path_section)
    else:
        print("No es posible conectarse a la base de datos solicitada.")
    return conexion

def get_connection_data(config_file_path="config.ini", section="postgres"):
    """
    Permite obtener los datos de conexion desde un archivo con formatpo INI.

    Parametros (args):
        :param config_file_path (str, optional) ==> Ruta del archivo de configuración INI. Por defecto, "config.ini".
        :param section (str, optional) ==>  Nombre de la sección en el archivo INI que contiene los datos de conexión. Por defecto, "postgres".

    Output (returns):
        :return conn (sqlalchemy.engine.Engine) ==> Datos para armar el string de conexión a la base de datos.

    Raises:
        Exception ==> Si se detectan errores en alguna de las operaciones realizadas.  
    """
    # Leer la configuración desde el archivo INI
    config = ConfigParser()

    try:
        config.read(config_file_path)
        conn_data = config[section]

    except Exception as ex:
        print(f"ERROR! No se pudo concluir la operacion solicitada: {str(ex)}")
        raise ex
    
    return conn_data

def connect_to_postgres(config_file_path="config.ini", section="postgres"):
    """
    Establece una conexión a una base de datos postgres, utilizando la configuración especificada en un archivo INI.

    Parametros (args):
        :param config_file_path (str, optional) ==> Ruta del archivo de configuración INI. Por defecto, "config.ini".
        :param section (str, optional) ==>  Nombre de la sección en el archivo INI que contiene los datos de conexión. Por defecto, "postgres".

    Output (returns):
        :return conn (sqlalchemy.engine.Engine) ==> La conexión a la base de datos.

    Raises:
        FileNotFoundError ==>  Si el archivo de configuración es inexistente o no puede ser ubicado.
        Exception ==> Si se detectan otros tipos de errores en alguna de las operaciones realizadas.  
    """
    # Comprobar si el archivo de configuración existe
    if not os.path.exists(config_file_path):
        raise FileNotFoundError(f"El archivo de configuración '{config_file_path}' no existe.")

    try:
        # Obtener los parámetros de conexión
        conn_data=get_connection_data(config_file_path, section)
        host = conn_data.get("host")
        port = conn_data.get("port")
        db = conn_data.get("db")
        user = conn_data.get("user")
        pwd = conn_data.get("pwd")

        string_de_conexion = f"postgresql://{user}:{pwd}@{host}:{port}/{db}"

        # Establecer la conexión a la base de datos PostgreSQL
        conn = sa.create_engine(string_de_conexion)
    except Exception as ex:
        print(f"ERROR! No se pudo generar la conexion: {str(ex)}")
        raise ex
    
    return conn

def execute_query(engine, sql_query):
    """
    Ejecuta un query dada sobre una DB determinada.

    Parametros (args):
        :param engine (sqlalchemy.engine.Engine) ==> Conexion a utilizar para conectarnos a la base de datos dada.
        :param sql_query (str) ==> Query a ejecutar.

    Output (returns):
        :return None ==> Sin retorno
    
    Raises:
        Exception ==> Si se detectan errores en alguna de las operaciones realizadas.  
    """

    try:

        engine.execute(sql_query)
        
    except Exception as ex:
        print(f"ERROR! No se pudo ejecutar la query: {str(ex)}")
        raise ex

def close_connection_to_database(motor="postgres", ref_to_con=None):
    """
    Cierra una conexión a una base de datos.

    Parametros (args):
        :param motor (str, optional) ==> Nombre del motor correspondiente a la base de datos a donde nos coenectaremos. Por defecto, "postgres".
        :param ref_to_con (sqlalchemy.engine.Engine) ==> La conexión a la base de datos.

    Output (returns):
        :return None ==> Sin retorno
    """
    if motor=="postgres":
        close_connection_to_postgres(ref_to_con)
    else:
        print("No es posible cerrar la conexion a la base de datos solicitada.")

def close_connection_to_postgres(ref_to_con):
    """
    Cierra una conexión a una base de datos en caso de que la misma se encuentre abierta.

    Parametros (args):
        :param ref_to_con (sqlalchemy.engine.Engine) ==> La conexión a la base de datos.

    Output (returns):
        :return None ==> Sin retorno
    """
    if ref_to_con is not None:
        ref_to_con.dispose()
        print('Se cerró la conexion a la base de datos postgres.')
    
def delete_database_table(motor="postgres",nombreTabla="Tabla_1"):
    """
    Elimina una tabla con el nombre indicado, sobre una DB dada.

    Parametros (args):
        :param motor (str, optional) ==> Nombre del motor correspondiente a la base de datos a donde nos coenectaremos. Por defecto, "postgres".
        :param nombreTabla (str, optional) ==> Nombre de la tabla a eliminar. Por defecto, "Tabla_1".

    Output (returns):
        :return None ==> Sin retorno
    
    Raises:
        Exception ==> Si se detectan errores en alguna de las operaciones realizadas.  
    """
    sql_query = f"DROP TABLE IF EXISTS {nombreTabla}"

    try:

        conn = connect_to_database(motor,"Inputs/config.ini")

        execute_query(conn, sql_query)

    except Exception as ex:
        close_connection_to_database(motor, conn)
        print(f"ERROR! No se pudo eliminar la tabla: {str(ex)}")

    finally:
        if conn is not None:
            close_connection_to_database(motor, conn)


def create_magnitude_database_table(nombreTabla):
    """
    Crea una tabla con el nombre indicado, sobre una DB dada.

    Parametros (args):
        :param nombreTabla (str) ==> Nombre de la tabla a crear. 

    Output (returns):
        :return None ==> Sin retorno

    Raises:
        Exception ==> Si se detectan errores en alguna de las operaciones realizadas.  
    """
    try:
        sql_query = f"""              
            CREATE TABLE IF NOT EXISTS {nombreTabla} (
                place TEXT PRIMARY KEY,
                magnitud_minima FLOAT,
                magnitud_promedio FLOAT,
                magnitud_maxima FLOAT,
                impacto_maximo TEXT
            )
        """

        conn = connect_to_database("postgres","Inputs/config.ini")

        execute_query(conn, sql_query)

        #sql_query_check_fields = f"SELECT * FROM {nombreTabla}"
        #print(execute_query(conn, sql_query_check_fields)) ## DEBUG!
        #print(pds.read_sql_query(sql_query,conn).columns.values) ## DEBUG!

    except Exception as ex:
        close_connection_to_database("postgres", conn)
        print(f"ERROR! No se pudo crear la tabla: {str(ex)}")
    finally:
        if conn is not None:
            close_connection_to_database("postgres", conn)

def load_database(nombreTabla, df_load, motor="postgres"):
    """
    Funcion para cargar una tabla con el contenido de un dataframe dado. 
    Requiere que los datos de conexion a la DB se enceuntren en un archivo "Inputs/config.ini".

        Cargar una tabla con el nombre indicado, sobre una DB dada.

    Parametros (args):
        nombreTabla (str, optional) ==> Nombre de la tabla a consultar.
        df_load (Pandas Dataframe) ==> Un DataFrame de Pandas con los datos estructurados a cargar.
        motor (str, optional) ==> Nombre del motor correspondiente a la base de datos a donde nos coenectaremos. Por defecto, "postgres".

    Output (returns):
        :return (None) 

    Raises:
        TypeError ==>  Si el argumento df no es un DataFrame de pandas.
        Exception ==> Si se detectan otros tipos de errores en alguna de las operaciones realizadas.  
    """

    try:
        ##Control de tipos
        if JG_Alm.isDataframe(df_load) is False:
            raise TypeError("El argumento 'df' debe ser un DataFrame de pandas.")
        
        if motor=="postgres":
            load_postgress_database(nombreTabla, df_load)
        else:
            print("No es posible conectarse a la base de datos solicitada.")
    except Exception as ex:
        print(f"ERROR! No se pudo concluir la operacion solicitada: {str(ex)}")

def load_postgress_database(nombreTabla, df_load):
    """
    Funcion para cargar una tabla con el contenido de un dataframe dado. 
    Requiere que los datos de conexion a la DB se enceuntren en un archivo "Inputs/config.ini".

    Parametros (args):
        nombreTabla (str, optional) ==> Nombre de la tabla a consultar.
        df_load (Pandas Dataframe) ==> Un DataFrame de Pandas con los datos estructurados a cargar.

    Output (returns):
        :return (None) 

    Raises:
        Exception ==> Si se detectan otros tipos de errores en alguna de las operaciones realizadas.  
    """

    try:

        conn = connect_to_database("postgres","Inputs/config.ini")
                
        # Inserta los datos del DataFrame en la tabla
        df_load.to_sql(nombreTabla, conn, method="multi", if_exists="append")

    except Exception as ex:
        close_connection_to_database("postgres", conn)
        print(f"ERROR! No se pudo cargar la tabla: {str(ex)}") 

    finally:
        if conn is not None:
            close_connection_to_database("postgres", conn)
    
def print_contenido_tabla(nombreTabla, motor="postgres"):
    """
    Imprime un archivo parquet en caso de que exista.
    Requiere que los datos de conexion a la DB se enceuntren en un archivo "Inputs/config.ini".
    
    Parametros (args):
        :param nombreTabla (str, optional) ==> Nombre de la tabla a consultar.
        :param motor (str, optional) ==> Nombre del motor correspondiente a la base de datos a donde nos coenectaremos. Por defecto, "postgres".

    Output (returns):
        :return (None) 

    Raises:
        Exception ==> Si se detectan otros tipos de errores en alguna de las operaciones realizadas.  
    """
    try:
        if motor=="postgres":
            print_contenido_tabla_postgress(nombreTabla)
        else:
            print("No es posible conectarse a la base de datos solicitada.")
    except Exception as ex:
        print(f"ERROR! No se pudo concluir la operacion solicitada: {str(ex)}")

def print_contenido_tabla_postgress(nombreTabla):
    """
    Imprime un archivo parquet en caso de que exista.
    Requiere que los datos de conexion a la DB se enceuntren en un archivo "Inputs/config.ini".
    
    Parametros (args):
        :param nombreTabla (str, optional) ==> Nombre de la tabla a leer e imprimir.

    Output (returns):
        :return (None) 

    Raises:
        Exception ==> Si se detectan otros tipos de errores en alguna de las operaciones realizadas.  
    """
    try:
        conn = connect_to_database("postgres","Inputs/config.ini")

        print(pds.read_sql_table(table_name=nombreTabla, con=conn).to_string())
        
    except Exception as ex:
        close_connection_to_database("postgres", conn)
        print(f"ERROR! No se pudo concluir la operacion solicitada: {str(ex)}")
    finally:
        if conn is not None:
            close_connection_to_database("postgres", conn)

### Métodos Genericos / Utilitarios de Tranformación de los Datos ### 
def reemplazar_nulos(df, column_name, fill_value):
    """
    Rellenar y/o reemplazar los valores nulos de una columna con un valor en específico.

    Parametros (args):
        :param df (pd.DataFrame) ==> El DataFrame que contiene los datos.
        :param column_name (str) ==> El nombre de la columna en la que se deben rellenar los valores nulos.
        :param fill_value ==> El valor con el que se deben rellenar los valores nulos en la columna especificada.

    Output (returns):
        :return pd.DataFrame ==> El DataFrame con los valores nulos rellenados en la columna especificada.
    """
    df[column_name] = df[column_name].fillna(fill_value)
    return df

def eliminar_nulos(df, column_name):
    """
    Eliminar las filas que contengan valores nulos en una o más columnas.

    Parametros (args):
        :param df (pd.DataFrame) ==> El DataFrame que contiene los datos.
        :param column_name (str) ==> El nombre de la columna en la que se deben localizar los valores nulos.

    Output (returns):
        :return pd.DataFrame ==> El DataFrame sin los registros con valores nulos.
    """
    df=df.dropna(subset=[column_name])

    ## Otros tipos de nulos
    df = df.drop(df[(df[column_name] == 'null')].index)

    # # Otros tipos de nulos
    # for col in column_name:
    #     mask = (df[col] == "null") or (df[col] is None)
    #     # select all rows except the ones that contain mask
    #     df = df[~mask]

    return df


def truncar_decimales(df, column_name, dec_cant: int):
    """
    Trunca los valores de una determinada columna a una cantidad dada de decimales.

    Parametros (args):
        :param df (pd.DataFrame) ==> El DataFrame que contiene los datos.
        :param column_name (str) ==> El nombre de la columna en la que se deben localizar los valores a truncar
        :param dec_cant (int) ==> Indica la cantidad de decimales a la cual truncar. Debe ser un numero entero.

    Output (returns):
        :return df (pd.DataFrame) ==> El DataFrame con los valores del campo dado truncados.

    Raises:
        Exception ==> Si se detectan errores en alguna de las operaciones realizadas.  
    """
    try:
        df[column_name] = df[column_name].round(dec_cant)

    except Exception as ex:
        print(f"ERROR! No se pudo realizar el truncado: {str(ex)}")
        raise ex
   
    return df

def isDict(param):
    """
    Evalua si el argumento recibido es un no un Dict de Python.

    Parametros (args):
        :param param (Unknown) ==> El objeto del que se desea verificar el tipo
    
    Output (returns):
        :return True ==> Si 'param' SI es un Dict de Python
        :return False ==> Si 'param' NO es un Dict de Python
    """
    # Verificar que df sea un DataFrame
    if not isinstance(param, dict):
        return False
    return True

def dict_to_df(dict):
    """
    Guardar un Dict en formato DataFrame.

    Parametros (args):
        :param dict (Dict) ==>  El Diccionario que se desea guardar en un dataframe.
    
    Output (returns):
        :return df (Pandas.DataFrame) ==> El DataFrame que se desea guardar.

    Raises:
        TypeError ==> Si el argumento df no es un Python Dict.
    """

    ##Control de tipos
    if isDict(dict) is False:
        raise TypeError("El argumento 'dict' debe ser un Dict.")

    try:
        # Guardar el Dict en formarto DataFrame de Pandas
        df = pds.DataFrame.from_dict(dict)
    except Exception as e:
        print(f"Error al tranformar el Dict en DataFrame: {str(e)}")
    
    return df

def generar_df_magnitudes_agrupado(df):
    """
    Obtener un DataFrame con agregaciones agrupadas debido a un cierto campo.

    Parametros (args):
        :param df (Pandas.DataFrame) ==> El DataFrame que se desea guardar.

    Output (returns):
        :return df_group (Pandas.DataFrame) ==> La salida es un nuevo DataFrame con el resultado de las agregaciones realizadas.
    
    Raises:
        TypeError ==> Si el parametro enviado como 'df' no se encuentra en formato DataFrame de Pandas.
    """
    
    field_group_by='properties.place'
    field_agg_by='properties.mag'

    # reemplazar_nulos(df, field_group_by, "unknown")
    eliminar_nulos(df, field_group_by)

    ##Control de tipos
    if JG_Alm.isDataframe(df) is False:
        raise TypeError("El argumento 'df' debe ser un DataFrame de pandas.")
    
    df_group = df.groupby(field_group_by).agg(
        magnitud_minima=(field_agg_by, 'min'),
        magnitud_promedio=(field_agg_by, 'mean'),         
        magnitud_maxima=(field_agg_by, 'max')
    )

    df_group.rename(columns = {'field_group_by':'place'}, inplace = True)

    return df_group

def get_impacto_magnitud(value):
    """
    Obtener el impacto segun la magnitud de un terremoto.

    Parametros (args):
        :param df (numerical value) ==> El valor a comparar.

    Output (returns):
        :return (string ) ==> La salida es un string o guion con el resultado de las validaciones realizadas.
    """
    if value is not None:
        if value < 2.9:
            return "micro"
        elif (value >= 3.0 and value < 4.0):
            return "minor"
        elif (value >= 4.0 and value < 5.09):
            return "light"
        elif (value >= 5.0 and value < 6.0):
            return "moderate"
        elif (value >= 6.0 and value < 7.0):
            return  "strong"
        elif (value >= 7.0 and value < 8.0):
            return "major"
        elif value >= 8.0:
            return  "great"
        else:
            return "-"
    else:
        return "-"

def add_columna_impacto(df):
    """
    Añadir una nueva columna al DataFrame, que contenga el impacto de los terremotos segun su magnitud.

    Parametros (args):
        :param df (Pandas.DataFrame) ==> El DataFrame que se desea analizar.

    Output (returns):
        :return df_group (Pandas.DataFrame) ==> La salida es un nuevo DataFrame que incluye a la nueva columna.
    """
    try:
        df['impacto_maximo'] = df['magnitud_maxima'].map(get_impacto_magnitud)

    except Exception as e:
        print(f"Error al añadir la nueva columna al DataFrame: {str(e)}")
    
    return df

def main():
    """
    Funcion principal del Script para modularizar el código.
    En la misma se invocan los procesos de transformacion de los datos almacenados, y carga en base de datos OLAP.

    Parametros (args):
        :param (None)

    Output (returns):
        :return (None) 
    """

    df_agrupado = generar_df_magnitudes_agrupado(JG_Alm.read_parquet("Output/datalake/landing/earthquake/Registros/Historial/terremotos-historial.parquet"))
    # print(df_agrupado) ## DEBUG!

    truncar_decimales(df_agrupado, 'magnitud_promedio', 2)
    # print(df_agrupado)  ## DEBUG!

    df_agrupado = add_columna_impacto(df_agrupado)
    # print(df_agrupado) ## DEBUG!
    
    nombreTabla="JimenaGonzalez_magnitud_terremotos"    

    print("\n########## ELIMINAR TABLA - ARRANCAR DE 0 ##########")
    delete_database_table("postgres",nombreTabla)

    print("\n########## CREACION DE TABLA ##########")
    create_magnitude_database_table(nombreTabla)

    print("\n########## CARGA DE TABLA ##########")
    load_database(nombreTabla, df_agrupado, "postgres")

    print("\n########## LECTURA DE TABLA ##########")
    print_contenido_tabla(nombreTabla, "postgres")

main()