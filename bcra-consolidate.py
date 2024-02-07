import requests
import json
import pandas as pd
import sqlalchemy
import psycopg2
import pytz
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from utils import read_api_credentials, read_config_file, get_redshift_connection, build_conn_string, conn_to_db


# (old) Opción 1 de autenticación: Leer el archivo creado "config.json" para obtener el api token y autenticar correctamente.
##with open('config.json') as config_file:
##    config = json.load(config_file)
##api_token = config['api_token']

# AUTENTICACIÓN
## Opción 2 de autenticación: Leer el archivo "config.ini" para obtener un diccionario con el token, al que accederemos luego. Cambié este modo de autenticación por el previo de .json (entiendo que es más seguro y flexible).
api_token2 = read_api_credentials("config.ini", "api_bcra")

## Definición de header con referencia al .json que contiene el token de autenticación.
headers = {
    'Authorization': f'Bearer {api_token2['api_token']}'
}

# OBTENCIÓN Y PREPARACIÓN DE DATAFRAME
def consolidate(endpoint, description):
    """
    Esta función obtiene datos de un endpoint de la API y los convierte en un DataFrame de pandas.

    Args:
        endpoint (str): El endpoint de la API desde donde se extraen los datos.
        concept (str): El concepto o categoría de los datos (ej. 'plazo fijo').

    Returns: 
        DataFrame: Un DataFrame con los datos obtenidos del endpoint de la API, o un dataframe vacío en caso de error.
    """
    url = f'https://api.estadisticasbcra.com{endpoint}'
    response = requests.get(url, headers=headers)
    start = datetime.now(pytz.timezone('America/Buenos_Aires')) - timedelta(days=30)
    end = datetime.now(pytz.timezone('America/Buenos_Aires')) - timedelta(days=1)
    
    if response.status_code == 200:
        print(f'Status code: {response.status_code}')         
        data = response.json()        
        df = pd.DataFrame(data)        
        df.rename(columns={'d': 'Date', 'v': 'Value'}, inplace= True)        
        df['Date'] = pd.to_datetime(df['Date'])
        df['Date'] = df['Date'].dt.tz_localize('America/Buenos_Aires')        
        df['Concept'] = description        
        filtered_df = df[(df['Date'] >= start) & (df['Date'] <= end)]                
        return filtered_df
                
    else: 
        print(f'Failed to fetch data from {endpoint}. Status code:', response.status_code)
        # Retorna un df vacío
        return pd.DataFrame()
        
## Endpoints y Concepts
endpoints = [
    ("/plazo_fijo", "Plazos fijos (m)"),
    ("/depositos", "Depositos (m)"),
    ("/cajas_ahorro", "Cajas Ahorro (m)"),
    ("/cuentas_corrientes", "Cuentas corrientes (m)"),
    ("/usd", "Dolar blue"),
    ("/usd_of", "Dolar oficial")
]
## Lista vacía de endpoints para alojar durante el for
dataframes = []

## Loop "for" que itera sobre la lista de tuplas, llamando a la función "consolidate" para obtener los df y agregarlos a la lista "dataframes" (siempre que la respuesta no sea None o un df vacío) 
for endpoint, description in endpoints:
    df = consolidate(endpoint, description)
    if df is not None and not df.empty:
        dataframes.append(df)
        

## Unificación de dataframes, generando un index nuevo y ordenando las columnas. Si no se obtuvo información, se arroja un mensaje que lo comenta. 
if dataframes:
    df_final = pd.concat(dataframes, ignore_index=True)
    df_final = df_final[['Date', 'Concept', 'Value']]
    print(df_final)
else: 
    print("No se lograron recolectar datos de los endpoints.")


# CONEXIÓN CON REDSHIFT
## Llamada a la función de lectura del archivo de configuración y asignación a variable "config_file". 
config_file = read_config_file("config.ini")

## Con "config_file" consolidamos el string de conexión a redshift.
conn_string = build_conn_string(config_file, "redshift", "postgresql")

## Con el string de conexión, establecemos la conexión para interactuar con al base de datos.
conn = conn_to_db(conn_string)


# CREACIÓN DE TABLA
try:
    ## Debug Print para asegurar el éxito de la ejecución hasta esta parte
    print("Intentando crear la tabla en el esquema...")
    
    ## Se agrega esta línea para pruebas durante la preparación del proyecto. 
    conn.execute("DROP TABLE IF EXISTS tomasmartindl_coderhouse.bcra;")
    
    ## En este caso se crea la tabla Date como clave de distribución y de ordenamiento. En un caso en el cual, por ejemplo, las consultas estén más relacionadas con "concept", porque se requiere hacer joins sobre registros por cada concepto, hubiese elegido "concept" como distkey, de estilo de distribución KEY. 
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tomasmartindl_coderhouse.bcra(
            date DATE DISTKEY,
            concept VARCHAR(50),
            value NUMERIC(18,2),
            PRIMARY KEY (date, concept)
        )
        SORTKEY(date);
    """)
    
    # Debug Print para asegurar que se creó la tabla o se detectó una existente
    print("La tabla ha sido creada.")
    

# Debug info: detección de errores
except Exception as e:
    print("Ocurrió un error al intentar crear la tabla:")
    print(e)

# Inserción de datos de "df_final".
try:
    df_final.to_sql(
        "bcra",
        conn,
        schema="tomasmartindl_coderhouse",
        if_exists="append",
        method="multi",
        chunksize=200,
        index=False
    )

    # Debug print: si el código SQL se ejecuta sin excepciones, muestra el mensaj de éxito. 
    print("La inserción de datos fue realizada con éxito")

except Exception as e:
    print("Ocurrión un error durante la inserción de datos: ")
    print(e)
    
# Cierra la conexión
finally:
    conn.close()
    print("Conexión cerrada.")