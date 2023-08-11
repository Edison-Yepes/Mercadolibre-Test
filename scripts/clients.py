import utils
from pandas_gbq import to_gbq
import numpy as np

# Spreadsheet URL
spreadsheet_url = "https://docs.google.com/spreadsheets/d/1AJY_-BxosSmbC2-sIQZn9hD5T6BE7QfzThddXId0Tyg/edit#gid=12675925"
# Get conecction with GCP/Spreadsheets
client = utils.conn()
# Get dataframe from a worksheet
df_clients = utils.get_data(client, spreadsheet_url, "Clientes")


###TRANSFORM
#"Ocupacion" column
df_clients.Ocupacion = df_clients.Ocupacion.replace({"in dependiente": "Independiente", "Soltero": "", "emp":"Empleado"})

#"Estado Civil" column
df_clients["Estado Civil"] = df_clients["Estado Civil"].str.strip()
df_clients["Estado Civil"] = df_clients["Estado Civil"].replace({"Sol": "Soltero", "Cas": "Casado", "Cosado":"Casado", "Sotero":"Soltero"})

#"Genero" column
df_clients.Genero = df_clients.Genero.str.upper()
df_clients.Genero = df_clients.Genero.replace({"FF": "F", "MM": "M"})

#"Salario net USD" column
df_clients['Salario net USD'] = df_clients['Salario net USD'].apply(utils.remove_non_numeric_chars)

#"Fecha Inactividad"
df_clients['Fecha Inactividad'] = df_clients['Fecha Inactividad'].apply(utils.standardize_date_format)

#Rename columns
df_clients.rename(columns={"Estado (1 activo)": "Estado", "Salario net USD":"Salario", "Estado Civil":"Estado_Civil", "Fecha Inactividad":"Fecha_Inactividad", "Nivel Educativo":"Nivel_Educativo"}, inplace = True)

#Columns change datetype
integer_columns = ['id', 'edad', 'Score', 'Salario']
df_clients[integer_columns] = df_clients[integer_columns].applymap(utils.convert_to_integer)


###LOAD
#Specify the project and the BigQuery dataset.
project_id = 'mercadolibre-test-395519'
dataset_id = 'movil'

#Specify the table name in BigQuery.
table_name = 'dim_clients'

# Cargar el DataFrame a BigQuery
try:
    to_gbq(df_clients, f'{project_id}.{dataset_id}.{table_name}', project_id=project_id, if_exists='replace')
    print("DataFrame successfully loaded into BigQuery.")
except Exception as e:
    print("Error while loading the DataFrame into BigQuery:", e)


#print(df_clients.to_string())

