import requests as r
import datetime as dt
import json
from dotenv import load_dotenv
import os
import pandas as pd
from sqlalchemy import create_engine

def searchItemsML(searchTerms):
    load_dotenv()

    site = 'MLA'
    accesToken = os.getenv('ML_ACCESS_TOKEN')
    searchExpression = searchTerms.replace(' ', '%20')
    paging_limit = 50

    headers = {'Authorization': f'Bearer {accesToken}'}

    url = f'https://api.mercadolibre.com/sites/{site}/search?q={searchExpression}'

    params = {
        'limit' : paging_limit,
        'offset' : 0
    }

    response = r.request("GET", url, headers=headers, params = params)
    data = response.json()


    metadata = {
        "site_id" : data.get('site_id'),
        'query' : data.get('query'),
        'paging'  : data.get('paging')
    }

    productos = data.get('results')

    # Paginación sobre primary_results y no sobre total para trabajar con respuestas que contengan 
    # todas las palabras de la búsqueda.
    if metadata.get('paging').get('primary_results') > paging_limit :
        iter_paging = metadata.get('paging').get('primary_results') // paging_limit
        for i in range(1, iter_paging + 1):
            params['offset'] = paging_limit * i
            response = r.request("GET", url, headers=headers, params = params)
            if response.status_code == 200:
                iter_productos = response.json().get('results')
                productos += iter_productos
    else :
        productos = productos[:metadata.get('paging').get('primary_results')]

    print(metadata)
    print(len(productos))

    # Seleccionar datos de interés
    productos_selected_data = []
    for p in productos :
        attr = {}
        for i in p['attributes']:
            attr[i['id']] = i['value_name']
        fila = {
            'id' : p['id'],
            'title' : p['title'],
            'condition' : p['condition'],
            'currency_id' : p['currency_id'],
            'price' : p['price'],
            'original_price' : p['original_price'],
            'installments_quantity' : p['installments'].get('quantity'),
            'installments_rate' : p['installments'].get('rate'),
            'tags' : p['tags'],
            'attributes' : attr,
            'seller_id' : p['seller'].get('id'),
            'seller_nickname' : p['seller'].get('nickname'),
            'address_state_id' : p['address'].get('state_id'),
            'address_state_name' : p['address'].get('state_name'),
            'seller_reputation' : p['seller'].get('seller_reputation').get('level_id'),
            'shipping_pick_up' : p['shipping'].get('store_pick_up'),
            'shipping_free' : p['shipping'].get('free_shipping'),
            'shipping_logistic_type' : p['shipping'].get('logistic_type')
        }
        productos_selected_data.append(fila)

    # Salida de JSONs    
    ahora = str(dt.date.today())
    rutaArchivoP = f'ApiML/archivos/{ahora} {searchTerms}.P.json'
    rutaArchivoPS = f'ApiML/archivos/{ahora} {searchTerms}.PS.json'
    rutaArchivoM = f'ApiML/archivos/{ahora} {searchTerms}.M.json'
    with open(rutaArchivoP, 'w') as archivo:
        json.dump(productos, archivo)
    with open(rutaArchivoPS, 'w') as archivo:
        json.dump(productos_selected_data, archivo)
    with open(rutaArchivoM, 'w') as archivo:
        json.dump(metadata, archivo)
    
    return productos_selected_data

def filterItemsML(searchTerms, keywords, category, itemName):

    load_dotenv()

    prod = pd.DataFrame(searchItemsML(searchTerms))
    prod['seller_id'] = prod['seller_id'].astype(object)

    # Filtrado, sólo productos nuevos
    prod = prod.drop(prod[~(prod['condition'] == "new")].index)
    
    # Filtrado por palabras clave
    prod['title.lower'] = prod['title'].str.lower()
    for kw in keywords:
        prod = prod.drop(prod[prod['title.lower'].str.contains(kw)].index)
    prod = prod.drop('title.lower', axis=1)

    # Filtrado por shipping_free
    prod = prod.drop(prod[(prod['price'] < int(os.getenv('ML_FREE_SHIPPING_PRICE'))) & (prod['shipping_free'] == True)].index)

    # Filtrado por cuotas sin interés
    prod = prod.drop(prod[(prod['installments_rate'] == 0)].index)

    # Filtrado por outsider: 2 StD +- de la mediana
    median = prod['price'].median()
    std = prod['price'].std()
    prod = prod.drop(prod[(prod['price'] < median-2*std) | (prod['price'] > median+2*std)].index)

    # Agregado de fecha y query en ML; ¿nombre/modelo y (why not?) categoría de ref de búsqueda?
    # La idea sería que todo esto venga en el json con searchTerms y keywords  
    prod['search_date'] = str(dt.date.today())
    prod['search_terms'] = searchTerms
    prod['product_category'] = category
    prod['product_name'] = itemName

    # Convertir valores json a string
    prod['attributes'] = prod['attributes'].apply(json.dumps)
    prod['tags'] = prod['tags'].apply(json.dumps)

    # Evitar el null en original_price
    prod['original_price'] = prod['original_price'].fillna(prod['price'])

    
    # Salidas de prueba
    print(prod.info())
    print(prod.describe())
   
    prod.to_json(f'ApiML/archivos/{str(dt.date.today())} {searchTerms}.PF.json')

    return prod

def loadToDB(data, table, pkDate):
    # Conexión con la tabla en Redshift
    user = os.getenv('REDSHIFT_USER')
    pwd = os.getenv('REDSHIFT_PASSWORD')
    host = os.getenv('REDSHIFT_HOST')
    port = os.getenv('REDSHIFT_PORT')
    database = os.getenv('REDSHIFT_DATABASE')

    engine = create_engine(f'postgresql://{user}:{pwd}@{host}:{port}/{database}')
    
    # Lectura de las columnas que permiten una PK
    # Quiero armar la función para que funcione con una PK pasada por parámetro,
    # quedará para una próxima entrega, por lo pronto uso fecha para evitar la duplicación
    # de valores ya cargados un día
    """     query = 'SELECT ' + primaryKey + ' FROM ' + table
    existing_table = pd.read_sql_query (query, engine)
    print(existing_table)
    # Creación de un df que verifique registros duplicados
    columnasJoin = ', '.join(existing_table.columns.to_list())
    dfRegDuplicados = existing_table.merge(data, on=columnasJoin, how='inner')
    print(columnasJoin)
    print('DFREGDUPLICADOS')
    print(dfRegDuplicados)
         else :
        existing_table = pd.read_sql_query (f'SELECT {primaryKey} FROM {table}', engine)
        # Creación de un df que verifique registros duplicados
        dfRegDuplicados = pd.merge(existing_table, data[primaryKey], on=list(existing_table.columns), how='inner') 
    """
    query = 'SELECT DISTINCT ' + pkDate + ' FROM ' + table
    existing_pk = pd.read_sql_query (query, engine)

    listaFechas = []
    for i in existing_pk[pkDate].tolist():
        fecha = str(i)
        listaFechas.append(fecha)

    if data[pkDate].isin(listaFechas).any() :
        print('Carga de datos cancelada. Hay registros duplicados')    

    else :
        print('No hay registros duplicados')

        # Carga de los datos en Redshift
        data.to_sql(table, engine, if_exists='append', index=False)
        print('La carga de datos fue exitosa')

    
with open('ApiML/ApiML.json') as searchListJson:
    searchList = json.load(searchListJson)

listDfPF = []

for item in searchList :
    listDfPF.append(filterItemsML(item['searchTerms'], item['keywords'], item['category'], item['name']))

# Juntar todos los DF PF en uno 
allPF = pd.concat(listDfPF)

# Algunas funciones de agrupación y agregación
allAgAg = allPF.groupby(['search_date', 'search_terms'])['price'].agg(['count', 'mean', 'median', 'min'])

allPriceAnalysis = allAgAg.reset_index()

# Carga de datos en Redshift
loadToDB(allPF, 'products_filtered', 'search_date')
loadToDB(allPriceAnalysis, 'price_analysis', 'search_date')