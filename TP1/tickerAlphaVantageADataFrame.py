import requests as r
import pandas as pd
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine

load_dotenv()
apiKey = os.getenv("ALPHA_VANTAGE_API_KEY")

#Consulta a API y acceso a los datos en el dict data
res = r.get(f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=YPF&apikey={apiKey}")
data = res.json().get('Time Series (Daily)')

#Creación del dataframe a partir de data; cambio de nombre de columnas; agregado de una PK autoincremental
dataFr = pd.DataFrame(data).T
nombresCol = ['open',
              'high',
              'low',
              'close',
              'adj_close',
              'volume',
              'dividend_amount',
              'split_coef',
              'date'
              ]
nuevosIndex = list(range(100))
dataFr['9.fecha'] = dataFr.index
dataFr.index = nuevosIndex
dataFr.columns = nombresCol

#Definición de los tipos de datos en cada columna
dataFr['open'] = dataFr['open'].astype(float)
dataFr['high'] = dataFr['high'].astype(float)
dataFr['low'] = dataFr['low'].astype(float)
dataFr['close'] = dataFr['close'].astype(float)
dataFr['adj_close'] = dataFr['adj_close'].astype(float)
dataFr['volume'] = dataFr['volume'].astype(int)
dataFr['dividend_amount'] = dataFr['dividend_amount'].astype(float)
dataFr['split_coef'] = dataFr['split_coef'].astype(float)
dataFr['date'] = dataFr['date'].astype('datetime64[s]')

print(dataFr.info())

engine = create_engine('postgresql://francojosegonzalez_coderhouse:0daNA8HJ56@data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com:5439/data-engineer-database')

dataFr.to_sql('ypf', engine, if_exists = 'append', index = False)
