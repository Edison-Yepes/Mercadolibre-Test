import utils
from pandas_gbq import to_gbq

# Spreadsheet URL
spreadsheet_url = "https://docs.google.com/spreadsheets/d/1AJY_-BxosSmbC2-sIQZn9hD5T6BE7QfzThddXId0Tyg/edit#gid=12675925"
# Get conecction with GCP/Spreadsheets
client = utils.conn()
# Get dataframe from a worksheet
df_product = utils.get_data(client, spreadsheet_url, "Producto")


###TRANSFORM
#Rename columns
df_product.rename(columns={"nombre":"Nombre_Producto", "Cantidad Datos MB":"Megabyte", "Vigencia (dias)":"Vigencia_Dias"}, inplace = True)

#Columns change datetype
integer_columns = ['id', 'ValorUSD', 'Megabyte', 'Vigencia_Dias']
df_product[integer_columns] = df_product[integer_columns].applymap(utils.convert_to_integer)


###LOAD
#Specify the project and the BigQuery dataset.
project_id = 'mercadolibre-test-395519'
dataset_id = 'movil'

#Specify the table name in BigQuery.
table_name = 'dim_products'

# Cargar el DataFrame a BigQuery
try:
    to_gbq(df_product, f'{project_id}.{dataset_id}.{table_name}', project_id=project_id, if_exists='replace')
    print("DataFrame successfully loaded into BigQuery.")
except Exception as e:
    print("Error while loading the DataFrame into BigQuery:", e)


#print(df_product.to_string())

