# Coderhouse - Pre-entrega 1
Este proyecto contiene el script que consolida todo lo requerido para la pre-entrega N°1. 

## Configuración
Para ejecutarlo, es necesario contar con un archivo "config.ini" que contiene las secciones:
[api_bcra]
Contiene el API token

[redshift] 
Contiene los datos de conexión a Redshift.

También será necesario instalar las librerías que contiene el archivo "requirements.txt" ejecutando el comando: 

    pip install -r requirements.txt

Para ejecutar el script principal, ejecutar el siguiente comando: 

    python bcra-consolidate.py

