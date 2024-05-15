import mysql.connector
import pandas as pd
import news_variables
from datetime import datetime
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import urllib.parse

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

from dotenv import load_dotenv
import os


# Load credentials from .env
load_dotenv()
azure_client_id = os.getenv("AZURE_CLIENT_ID")
azure_tenant_id = os.getenv("AZURE_TENANT_ID")
azure_client_secret = os.getenv("AZURE_CLIENT_SECRET")
key_vault_name = os.getenv("KEY_VAULT_NAME")




# extract azure vault related credentials
key_vault_url = "https://basiconomics-db-cred.vault.azure.net/"
credential = DefaultAzureCredential()
client = SecretClient(vault_url=key_vault_url, credential=credential)

safe_password = quote_plus( client.get_secret("basiconomics-db-pw").value ) 
safe_username = quote_plus( client.get_secret("basiconomics-db-user").value ) 

ssl_cert_path = 'certs/DigiCertGlobalRootCA.crt.pem'


# Database credentials and configuration
DATABASE_TYPE = 'mysql'
DBAPI = 'mysqldb'
HOST = 'basiconomics-data.mysql.database.azure.com'  # e.g., 'basiconomics-data.mysql.database.azure.com'
DATABASE = 'basiconomics_news_schema'
PORT = 3306

database_url = f"mysql+mysqldb://{safe_username}:{safe_password}@{HOST}:{PORT}/{DATABASE}?ssl_ca={ssl_cert_path}"
engine = create_engine(database_url)



argsGetSQL = {'check_col_duplicates': '', 'date_col': '', 'date_to': '', 'date_from': '', 'check_col': '', 'check_value': ''}


# UPLOADING DATA TO SQL

def getSQLQueryForUploadDF(df_var, var_tblName, **argsGetSQL):
    var_sqlString = '' 

    var_strIntroInsert = 'INSERT INTO ' + var_tblName + ' ('
    var_strIntroSelect = 'SELECT ' if 'check_col_duplicates' in argsGetSQL else 'VALUES ('

    var_strInsert = ''
    var_strSelect = ''

    for var_col in df_var.columns:
        var_strInsert += var_col
        var_strSelect += '%s'

        var_strInsert += ', '
        var_strSelect += ', '

    var_strInsert = var_strInsert[: -2]
    var_strSelect = var_strSelect[: -2] 
    if 'check_col_duplicates' not in argsGetSQL:
        var_strSelect = var_strSelect + ')'

    var_strInsert = var_strIntroInsert + var_strInsert +')'
    var_strSelect = var_strIntroSelect + var_strSelect

    var_sqlString = var_strInsert + ' ' + var_strSelect

    if 'check_col_duplicates' in argsGetSQL:
        var_checkColDuplicates = f"FROM DUAL WHERE NOT EXISTS ( SELECT 1 FROM {var_tblName} WHERE {argsGetSQL['check_col_duplicates']} = %s )"
        var_sqlString = var_sqlString + ' ' + var_checkColDuplicates

    return var_sqlString;



def uploadSQLQuery(df_var, var_tblName, **argsGetSQL):

    df_failed = pd.DataFrame(columns = df_var.columns)

    # Connect to the MySQL database
    engine = create_engine(database_url)

    # Use a context manager to handle the connection
    with engine.connect() as connection:
        # Convert DataFrame to list of tuples
        data_to_insert = list(df_var.itertuples(index=False, name=None))

        # Execute the query for each row
        for row in data_to_insert:
            if ('' not in row) and (pd.NaT not in row):
                if 'check_col_duplicates' in argsGetSQL:
                    connection.execute(getSQLQueryForUploadDF(df_var, var_tblName, check_col_duplicates=argsGetSQL['check_col_duplicates']), row + (row[1],))
                else:
                    connection.execute(getSQLQueryForUploadDF(df_var, var_tblName), row)
            else:
                df_varFailed = pd.DataFrame([row], columns=df_failed.columns)
                df_failed = pd.concat([df_failed, df_varFailed], ignore_index=True)

        # Commit the transaction
        connection.execute('commit')

    if not df_failed.empty:
        path_csv = news_variables.path_failedUpload + var_tblName + '_' + datetime.now().strftime('%Y-%m_%d') + '.csv'
        df_failed.to_csv(path_csv)




# DOWNLOADING DATA FROM SQL
# IN DJANGO
def downloadSQLQuery(var_tblName, **argsGetSQL):

    # Connect to the MySQL database
    engine = create_engine(database_url)

    # getting the SQL query
    var_sqlString = f"select * from basiconomics_news_schema.{var_tblName}"

    if 'date_col' in argsGetSQL:
        var_sqlString = var_sqlString + ' where '
        if 'date_to' in argsGetSQL:
            var_sqlString = var_sqlString + argsGetSQL['date_col'] + ' <= ' + "'" + argsGetSQL['date_to'] + "'"
            if 'date_from' in argsGetSQL:
                var_sqlString += ' and '
        
        if 'date_from' in argsGetSQL:
            var_sqlString = var_sqlString + argsGetSQL['date_col'] + ' >= ' + "'" + argsGetSQL['date_from'] + "'"

    if 'check_col' in argsGetSQL:
        var_sqlString += ' and ' if 'where' in var_sqlString else ' where '
        var_sqlString += f"""{ argsGetSQL['check_col']} = '{argsGetSQL['check_value']}' """

    df = pd.read_sql(var_sqlString, con=engine)

    return df;




# Get Last Article ID

def getLastArticleID():

    # Connect to the MySQL database
    engine = create_engine(database_url)

    var_sqlString = 'SELECT max(article_id ) as top_value FROM basiconomics_news_schema.news_id;'

    var_topAtclID = pd.read_sql(var_sqlString, con=engine)['top_value'][0]

    return var_topAtclID;


def checkIdInTbl(var_tblName, var_atclId):

    # Connect to the MySQL database
    engine = create_engine(database_url)

    var_sqlString = f"""select * from basiconomics_news_schema.{var_tblName} where article_id = '{var_atclId}' """

    df_var = pd.read_sql(var_sqlString, con=engine)

    return df_var.empty;





def checkNewsTextInTbl(var_tblName, var_url):

    # Connect to the MySQL database
    engine = create_engine(database_url)

    var_sqlString = f"""select * from basiconomics_news_schema.{var_tblName} where news_url = '{var_url}' and news_text is not null"""

    df_var = pd.read_sql(var_sqlString, con=engine)

    return df_var.empty;


def checkNewsUrlInTbl(var_tblName, var_url):

    # Connect to the MySQL database
    engine = create_engine(database_url)

    var_sqlString = f"""select * from basiconomics_news_schema.{var_tblName} where news_url = '{var_url}'"""

    df_var = pd.read_sql(var_sqlString, con=engine)

    return df_var.empty;



# This will write the query to replace items on the database
def getSQLReplaceQuery(var_tblName, var_colChk, var_chkVal, var_colChg, var_chgVal):
    # Construct the SET part of the SQL statement dynamically
    set_clause = ", ".join([f"{col} = %s" for col in var_colChg])

    # Build the complete SQL query
    query = f"""
    UPDATE {var_tblName}
    SET {set_clause}
    WHERE {var_colChk} = %s;
    """
    
    # var_chgVal now expected to be a list of values for the columns in var_colChg
    # The last element in the parameters tuple should be var_chkVal
    params = tuple(var_chgVal) + (var_chkVal,)
    
    return query, params


def replaceSQLQuery(var_tblName, var_colChk, var_chkVal, var_colChg, var_chgVal):

    if type(var_colChg) != list:
        var_colChg = [var_colChg]
    if type(var_chgVal) != list:
        var_chgVal = [var_chgVal]
        
    # Connect to the SQL database
    engine = create_engine(database_url)

    # Construct the update query with parameters
    update_query, params = getSQLReplaceQuery(var_tblName, var_colChk, var_chkVal, var_colChg, var_chgVal)

    # Use a context manager to handle the connection
    with engine.connect() as connection:
        # Execute the query
        connection.execute(update_query, params)
        # Commit the transaction
        connection.execute('commit')