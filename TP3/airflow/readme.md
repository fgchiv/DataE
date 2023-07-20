# ApiML
ETL de datos de MercadoLibre para construir un historial de precios de determinados productos de interés.
Puede definir nuevos productos para incluir en el script, modificando el archivo ./apiMLFiles/ApiML.json. Debe agregar los datos estructurados de la siguiente forma, separados de los anteriores y posteriores por ",":
    {
        "searchTerms": "palabras a buscar",
        "keywords": ["par", "set", "juego"...], 
        "category": "categoría",
        "name": "nombre del producto"
    }

"searchTerms" es la búsqueda que el script realizará en la tienda de MercadoLibre Argentina. Sólo se tomarán en cuenta resultados que contengan todas las palarbas incluidas.
"keywords" son palabras que el script utilizará para filtrar la búsqueda, buscando delimitar productos indivuales y no sets o conjuntos de productos que respondan a la búsqueda.
"category" y "name" son dos datos que el ETL agregará a la tabla para hacer identificables a los productos de manera homogénea.

Además de filtrar por estas palabras clave, el script utiliza otros métodos para reconocer y filtrar resultados no deseados o con particularidades que puedan alterar el precio (cuotas sin interés, envío gratis).

## Instrucciones para instalar la ETL en un DAG de Airflow.

1. Clonar / copiar el archivo docker-compose.yaml en un directorio local.
2. Crear las carpetas ./dags, ./logs, ./plugins, ./apiMLFiles y ./apiMLFiles/archivos
3. Copiar el archivo dag_api_ml.py en la carpeta ./dags
4. Copiar ApiML.json en ./apiMlFiles
5. Crear el archivo .env en la raíz con los siguientes datos:
    AIRFLOW_UID=501
    AIRFLOW_GID=0
6. Con la terminal situada en la raíz donde está docker-compose.yaml, ejecutar el comando "docker-compose up airflow-init"
7. Luego, ejecutar "docker-compose up". En la primera ejecución va a tomar su tiempo
8. Podrá acceder a la interfaz web de Airflow en la dirección localhost:8080


