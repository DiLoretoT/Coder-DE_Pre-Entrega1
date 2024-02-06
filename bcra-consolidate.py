import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import pytz
from utils import read_api_credentials, read_config_file, get_redshift_connection, build_conn_string


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
        data = response.json()        
        df = pd.DataFrame(data)        
        df.rename(columns={'d': 'Date', 'v': 'Value'}, inplace= True)        
        df['Date'] = pd.to_datetime(df['Date'])
        df['Date'] = df['Date'].dt.tz_localize('America/Buenos_Aires')        
        df['Concept'] = description        
        filtered_df = df[(df['Date'] >= start) & (df['Date'] <= end)]                  
        print(filtered_df)
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

else: 
    print("No se lograron recolectar datos de los endpoints.")


# CONEXIÓN CON REDSHIFT
## Llamada a la función de lectura del archivo de configuración y asignación a variable "config_file". 
config_file = read_config_file("config.ini")

## Con "config_file" consolidamos el string de conexión a redshift.
conn_string = build_conn_string(config_file, "redshift", "postgresql")

## Con el string de conexión, establecemos la conexión para interactuar con al base de datos.
conn = get_redshift_connection("config.ini", "redshift")
cursor = conn.cursor()

# CREACIÓN DE TABLA
try:
    ## Debug Print para asegurar el éxito de la ejecución hasta esta parte
    print("Intentando crear la tabla en el esquema...")
    
    ## Se agrega esta línea para pruebas durante la preparación del proyecto. 
    cursor.execute("DROP TABLE IF EXISTS tomasmartindl_coderhouse.bcra;")
    
    ## En este caso se crea la tabla Date como clave de distribución y de ordenamiento. En un caso en el cual, por ejemplo, las consultas estén más relacionadas con "concept", porque se requiere hacer joins sobre registros por cada concepto, hubiese elegido "concept" como distkey, de estilo de distribución KEY. 
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tomasmartindl_coderhouse.bcra(
            date DATE DISTKEY,
            concept VARCHAR(50),
            value NUMERIC(18,2),
            PRIMARY KEY (date, concept)
        )
        SORTKEY(date);
    """)
    
    # Debug Print para asegurar que se creó la tabla o se detectó una existente
    print("La tabla ha sido creada o ya existía.")
    
    conn.commit()
    print("Cambios confirmados en la base de datos.")

# Debug info: detección de errores
except Exception as e:
    print("Ocurrió un error al intentar crear la tabla:")
    print(e)

# Cierra el cursor y la conexión
finally:
    cursor.close()
    conn.close()
    print("Conexión cerrada.")

# Inserción de datos de "df_final". Todavía no finalizado, debo acomodarlo con sqlalchemy
#df_final.to_sql(
#    "bcra",
#    ,
#    schema="tomasmartindl_coderhouse",
#    if_exists="append",
#    method="multi",
#    chunksize=100,
#    index=False
#)