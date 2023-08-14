import utils
from pandas_gbq import to_gbq
import pandas as pd
import os
from google.api_core.exceptions import NotFound

#Specify the project and the BigQuery dataset.
project_id = 'mercadolibre-test-395519'
dataset_id = 'movil'

#Specify the table name in BigQuery.
table_name = 'fact_transactions'

# Spreadsheet URL
spreadsheet_url = "https://docs.google.com/spreadsheets/d/1AJY_-BxosSmbC2-sIQZn9hD5T6BE7QfzThddXId0Tyg/edit#gid=12675925"
# Get conecction with GCP/Spreadsheets
client = utils.conn()
# Get dataframe from a worksheet
df_transactions = utils.get_data(client, spreadsheet_url, "Compras")

max_date = utils.get_max_date(project_id, dataset_id, table_name, 'FechaCompra')

# Filter transactions based on max_date
df_transactions["FechaCompra"] = pd.to_datetime(df_transactions["FechaCompra"])
df_transactions = df_transactions[df_transactions["FechaCompra"] > max_date]


###TRANSFORM
#Rename columns
df_transactions.rename(columns={"Mediopago (Tarjeta o Cash)":"Medio_Pago"}, inplace = True)

#Columns change datetype
integer_columns = ['id', 'cust_id', 'prod_id', 'Gasto']
df_transactions[integer_columns] = df_transactions[integer_columns].applymap(utils.convert_to_integer)
#df_transactions['FechaCompra'] = pd.to_datetime(df_transactions['FechaCompra'])

###LOAD
# Cargar el DataFrame a BigQuery
try:
    to_gbq(df_transactions, f'{project_id}.{dataset_id}.{table_name}', project_id=project_id, if_exists='replace')
    print("DataFrame successfully loaded into BigQuery.")
except Exception as e:
    print("Error while loading the DataFrame into BigQuery:", e)


#print(df_transactions.to_string())

