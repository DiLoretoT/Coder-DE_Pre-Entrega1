import redshift_connector
import sqlalchemy
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from pathlib import Path
from configparser import ConfigParser


def read_api_credentials(file_path: Path, section: str) -> dict:
    """
    Lee las credenciales de la api desde el archivo "config.ini"
    
    args: 
        file_path: ruta del archivo de configuración
        section: sección del archivo con la información requerida
        
    Return:
        token de la API para construir el connection string
    """
    config = ConfigParser()
    config.read(file_path)
    api_credentials = dict(config[section])
    return api_credentials

# Leer el archivo creado "config.ini" para obtener el api token y autenticar correctamente. 
def read_config_file(file_path: str) -> ConfigParser:
    """
    Lee el archivo config.ini
    
    Args: 
    file_path: Ruta del archivo de configuración
    
    Returns:
    ConfigParser: Objeto con la configuración del archivo. 
    """
    config = ConfigParser()
    config.read(file_path)
    return config

def build_conn_string(config: ConfigParser, section: str, dbengine: str) -> str:
    """
    Construye un string de conexión a la base de datos
    
    Args: 
        config: Objeto con la configuración del archivos
        section: Sección del archivo de configuración
        dbengine: Motor de base de datos a utilizar
        
    Returns:
        str: String de conexión a la base de datos
    """
    host = config.get(section, "host")
    port = config.get(section, "port")
    user = config.get(section, "user")
    password = config.get(section, "password")
    database = config.get(section, "database")
    dbengine = config.get(section, "dbengine")
    
    conn_str = f"{dbengine}://{user}:{password}@{host}:{port}/{database}"
    return conn_str

def conn_to_db(conn_str: str):
    """
    Crea la conexión a la base de datos
    
    Args: 
        conn_str: String de conexión a la base de datos
        
    Returns: 
        Connectión: Objeto de conexión a la base de datos
    """
    engine = create_engine(conn_str)
    conn = engine.connect()
    return conn

def get_redshift_connection(file_path: str, section: str) -> redshift_connector.Connection:
    """
    Crea la conexión a la base de datos en Redshift utilizando la info de "config.ini"
    
    Args: 
        file_path: ruta del archivo .ini
        section: sección dentro del archivo con la info de redshift
        
    Returns: 
        Connectión: Objeto de conexión a la base de datos
    """
    config = ConfigParser()
    config.read(file_path)
    
    conn_info = {
        "host": config.get(section, "host"),
        "port": config.get(section, "port"),
        "database": config.get(section, "database"),
        "user": config.get(section, "user"),
        "password": config.get(section, "password")
    }
    
    # Crear y devolver la conexión
    return redshift_connector.connect(**conn_info)