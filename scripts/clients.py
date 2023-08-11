import utils

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

###LOAD

print(df_clients.to_string())

