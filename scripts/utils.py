import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import re
from datetime import datetime
import locale
#import Levenshtein
from google.cloud import bigquery

"""
The function `conn()` returns an authorized connection to Google Sheets using JSON credentials.
"""
def conn():
    # Path to the JSON credentials file downloaded from the Google Cloud Platform Console
    cred_file = "mercadolibre-test-395519-bedbd9e6fee2.json"

    # Required scope of access for the Google Sheets API
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

    # Authentication using the JSON credentials file
    credentials = ServiceAccountCredentials.from_json_keyfile_name(cred_file, scope)

    return gspread.authorize(credentials)


"""
The function get_data() returns a dataframe with all data from worksheet
"""
def get_data(client, spreadsheet_url, worksheet_name):
    # Open the spreadsheet by its URL
    spreadsheet = client.open_by_url(spreadsheet_url)

    # Access a specific sheet by its name
    worksheet = spreadsheet.worksheet(worksheet_name)

    # Get all values from the sheet as a list of lists
    data = worksheet.get_all_values()

    # Convert the list of lists into a pandas DataFrame
    df = pd.DataFrame(data[1:], columns=data[0])  # Use the first row as column names

    return df


"""
The function removes all non-numeric characters from a given string.
"""
def remove_non_numeric_chars(value):
    return re.sub(r'[^0-9]', '', value)


"""
The function `standardize_date_format` takes a date string as input and tries to parse it in
different formats, returning the date in the format 'dd-mm-yyyy' if successful, or None if the date
cannot be parsed.
"""
def standardize_date_format(date_str):
    try:
        locale.setlocale(locale.LC_TIME, 'es_ES.utf8')
        fecha_obj = datetime.strptime(date_str, '%B %d %Y')
        fecha_formato_nuevo = fecha_obj.strftime('%d/%m/%Y')
        return fecha_formato_nuevo
    except ValueError:
        return date_str


def convert_to_integer(value):
    try:
        return int(value)
    except (ValueError, TypeError):
        return 0


"""
The function `get_max_date` retrieves the maximum value of date from a BigQuery table.
"""
def get_max_date(project_id, dataset_id, table_name, date_column):
    credentials_path = "D:\Processes\Mercadolibre-Test\mercadolibre-test-395519-bedbd9e6fee2.json"
    client = bigquery.Client.from_service_account_json(credentials_path)

    # Construye la consulta SQL
    sql_query = f"""
        SELECT MAX({date_column}) AS max_date
        FROM `{project_id}.{dataset_id}.{table_name}`
    """
    
    # Ejecuta la consulta
    try:
        query_job = client.query(sql_query)
        results = query_job.result()
        # Extrae la máxima fecha de los resultados
        for row in results:
            max_date = pd.Timestamp(row["max_date"]).strftime("%d/%m/%Y")
            if max_date:
                return pd.Timestamp(max_date).strftime("%d/%m/%Y")
    except: None
    
    # Si no se encontraron resultados, retorna None
    return pd.Timestamp("1900-01-01").strftime("%d/%m/%Y")



'''
def find_most_similar_word(target_word, word_list):
    max_similarity = 0
    most_similar_word = None

    for word in word_list:
        similarity = Levenshtein.ratio(target_word, word)
        if similarity > max_similarity:
            max_similarity = similarity
            most_similar_word = word

    return most_similar_word


def replace_with_most_similar(df_column, word_list):
    new_column = []

    for value in df_column:
        if value:
            most_similar_word = find_most_similar_word(value, word_list)
            new_column.append(most_similar_word)
        else:
            new_column.append('')  # Mantener valores vacíos

    return new_column
'''