# Coderhouse - Pre-entrega 1
Este proyecto contiene el script que consolida todo lo requerido para la pre-entrega N°1. 

## Configuración
Para ejecutarlo, es necesario contar con un archivo "config.ini" que contiene las secciones:

[api_bcra]
Contiene el API token

[redshift] 
Contiene los datos de conexión a Redshift.

Junto con el link de GitHub, se envió un archivo txt con el detalle que debería contener este archivo "config.ini", por si es necesario validar el funcionamiento. 

También será necesario instalar las librerías que contiene el archivo "requirements.txt" ejecutando el comando: 

    pip install -r requirements.txt

Para ejecutar el script principal, ejecutar el siguiente comando: 

    python bcra-consolidate.py

NOTA: Actualmente trabajo en un equipo de infraestructura. No estoy en contacto diaria con herramientas como Python, lenguaje SQL, GitHub, y demás temas del curso. Si algo no está de acuerdo a las buenas prácticas de cómo compartir este tipo de trabajos. Estuve consultando e investigando y creo que se puede entender el código con los comentarios que agregué y la manera de ejecutarlo con este README. Además de la corrección del trabajo, agradezco cualquier feedback al respecto!
