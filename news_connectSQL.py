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
az_client = SecretClient(vault_url=key_vault_url, credential=credential)

SAFE_PASSWORD = quote_plus( az_client.get_secret("basiconomics-db-pw").value ) 
SAFE_USERNAME = quote_plus( az_client.get_secret("basiconomics-db-user").value ) 

var_zenrowsAPI = az_client.get_secret("zenrows-api").value
var_zenrowsAPI_wsjmkw = az_client.get_secret("zenrows-api-wsjmkw").value

var_githubToken = az_client.get_secret("github-token-news-backup").value

var_openai_summmary = az_client.get_secret("openai-summary").value
var_openai_category = az_client.get_secret("openai-category").value
var_openai_profile = az_client.get_secret("openai-profile").value 
var_openai_articleId = az_client.get_secret("openai-articleId").value 

var_wsjLogin = az_client.get_secret("wsj-login").value 
var_wsjPassword = az_client.get_secret("wsj-password").value

var_bscEmail = az_client.get_secret("basiconomics-email").value
var_bscPw = az_client.get_secret("basiconomics-emailPw").value



ssl_cert_path = 'certs/DigiCertGlobalRootCA.crt.pem'



# Database credentials and configuration
DATABASE_TYPE = 'mysql'
DBAPI = 'mysqldb'
HOST = 'basiconomics-data.mysql.database.azure.com'  # e.g., 'basiconomics-data.mysql.database.azure.com'
DATABASE = 'basiconomics_news_schema'
PORT = 3306

database_url = f"mysql+mysqldb://{SAFE_USERNAME}:{SAFE_PASSWORD}@{HOST}:{PORT}/{DATABASE}?ssl_ca={ssl_cert_path}"
engine = create_engine(database_url)



argsGetSQL = {'check_col_duplicates': '', 'date_col': '', 'date_to': '', 'date_from': '', 'check_col': '', 'check_value': '', 'upload_to_sql': '', 'params': '', 'use_quotes': ''}


def sqlPythonListToSql(list_str, **argsGetSQL):

    var_useQts = argsGetSQL['use_quotes'] if 'use_quotes' in argsGetSQL else 'y'

    var_qts = ""
    if (var_useQts == 'y') or (var_useQts == 'yes'):
        var_qts = "'"

    var_outputStr = '('
    for var_str in list_str:
        var_outputStr += var_qts + str(var_str) + var_qts + ", "

    var_outputStr = var_outputStr[ : -2]
    var_outputStr += ')'

    return var_outputStr;



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




def downloadSQLQuery(var_tblName, **argsGetSQL):
    # Connect to the MySQL database
    engine = create_engine(database_url)

    # getting the SQL query
    var_sqlString = f"SELECT * FROM basiconomics_news_schema.{var_tblName}"

    # Handle date filtering
    if 'date_col' in argsGetSQL:
        date_conditions = []
        if 'date_to' in argsGetSQL:
            date_conditions.append(f"{argsGetSQL['date_col']} <= '{argsGetSQL['date_to']}'")
        if 'date_from' in argsGetSQL:
            date_conditions.append(f"{argsGetSQL['date_col']} >= '{argsGetSQL['date_from']}'")
        if date_conditions:
            var_sqlString += ' WHERE ' + ' AND '.join(date_conditions)

    # Handle check columns and values
    if 'check_col' in argsGetSQL and 'check_value' in argsGetSQL:
        list_checkCol = argsGetSQL['check_col']
        list_checkValue = argsGetSQL['check_value']
        
        # Ensure they are lists
        if not isinstance(list_checkCol, list):
            list_checkCol = [list_checkCol]
        if not isinstance(list_checkValue, list):
            list_checkValue = [list_checkValue]
        
        # Create dictionary for checks
        dict_check = {}
        for var_key, var_value in zip(list_checkCol, list_checkValue):
            if var_key in dict_check:
                dict_check[var_key].append(var_value)
            else:
                dict_check[var_key] = [var_value]
        
        # Append conditions to the SQL string
        for var_key, var_values in dict_check.items():
            condition = f"{var_key} = '{var_values[0]}'" if len(var_values) == 1 else f"{var_key} IN {tuple(var_values)}"
            var_sqlString += ' AND ' + condition if 'WHERE' in var_sqlString else ' WHERE ' + condition

    # Execute the query and return the dataframe
    df = pd.read_sql(var_sqlString, con=engine)
    return df



# Get Last Article ID
def getLastArticleID():
    # Connect to the MySQL database
    engine = create_engine(database_url)
    var_sqlString = 'SELECT max(article_id ) as top_value FROM basiconomics_news_schema.news_id;'
    var_topAtclID = pd.read_sql(var_sqlString, con=engine)['top_value'][0]
    return var_topAtclID;



def getNextAtclId():
    var_sqlText = f"""
            select ifnull( max(article_id), 1000)
            from basiconomics_news_schema.article_entity;
        """
    var_atclId = getRawSQLQuery(var_sqlText).values[0]
    var_atclId = int(var_atclId)
    var_atclId += 1
    return var_atclId;



def checkIdInTbl(var_tblName, var_atclId):

    # Connect to the MySQL database
    engine = create_engine(database_url)

    var_sqlString = f"""select * from basiconomics_news_schema.{var_tblName} where article_id = '{var_atclId}' """

    df_var = pd.read_sql(var_sqlString, con=engine)

    return df_var.empty;


def getRawSQLQuery(var_sqlString, **argsGetSQL):
    
    # Connect to the MySQL database
    engine = create_engine(database_url)

    if 'params' in argsGetSQL:
        df_var = pd.read_sql(var_sqlString, con=engine, params = argsGetSQL['params'])
    else:
        df_var = pd.read_sql(var_sqlString, con=engine)

    return df_var;





def checkNewsTextInTbl(var_tblName, var_url):

    # Connect to the MySQL database
    engine = create_engine(database_url)

    var_sqlString = f"""select * from basiconomics_news_schema.{var_tblName} where news_url = '{var_url}' and news_text is not null"""

    df_var = pd.read_sql(var_sqlString, con=engine)

    return df_var.empty;


# Extract a DataFrame to check whether news_url already in table.
# False means url already in table. Dataframe not empty
def checkNewsUrlInTbl(var_tblName, var_url):

    # Connect to the MySQL database
    engine = create_engine(database_url)

    var_sqlString = f"""select * from basiconomics_news_schema.{var_tblName} where news_url = '{var_url}'"""

    df_var = pd.read_sql(var_sqlString, con=engine)

    return df_var.empty;



def getSQLReplaceQuery(var_tblName, var_colChk, var_chkVal, var_colChg, var_chgVal):
    # Ensure var_colChg and var_chgVal are lists
    if not isinstance(var_colChg, list):
        var_colChg = [var_colChg]
    if not isinstance(var_chgVal, list):
        var_chgVal = [var_chgVal]

    # Ensure var_colChk and var_chkVal are lists
    if not isinstance(var_colChk, list):
        var_colChk = [var_colChk]
    if not isinstance(var_chkVal, list):
        var_chkVal = [var_chkVal]

    # Construct the SET part of the SQL statement dynamically
    set_clause = ", ".join([f"{col} = %s" for col in var_colChg])
    
    # Construct the WHERE part of the SQL statement dynamically
    where_clause = " AND ".join([f"{col} = %s" for col in var_colChk])

    # Build the complete SQL query
    query = f"""
    UPDATE {var_tblName}
    SET {set_clause}
    WHERE {where_clause};
    """
    
    # Create a tuple for the query parameters
    params = tuple(var_chgVal + var_chkVal)
    
    return query, params;



def replaceSQLQuery(var_tblName, var_colChk, var_chkVal, var_colChg, var_chgVal, **argsGetSQL):

    var_outputStr = ''
    var_uploadOk = argsGetSQL['upload_to_sql'] if 'upload_to_sql' in argsGetSQL else 'y'

    # Connect to the SQL database
    engine = create_engine(database_url)

    # Construct the update query with parameters
    update_query, params = getSQLReplaceQuery(var_tblName, var_colChk, var_chkVal, var_colChg, var_chgVal)
    var_outputStr = 'in table ' + var_tblName + ', if value in column ' + str(var_colChk) + ' is ' + str(var_chkVal)[ : 100] + ' , then replace in column ' + str(var_colChg) + ' with value ' + str(var_chgVal)[ : 100]

    if (var_uploadOk == 'y') or (var_uploadOk == 'yes'):
        # Use a context manager to handle the connection
        with engine.connect() as connection:
            # Execute the query
            connection.execute(update_query, params)
            # Commit the transaction
            connection.execute('commit')
        
        var_outputStr += ' PRINTED'
    
    return var_outputStr;



## from 090724 ===============================================

def getNewEntityID():

    # Connect to the MySQL database
    engine = create_engine(database_url)

    var_sqlString = 'SELECT max(entity_id ) as top_value FROM basiconomics_news_schema.entity_table;'

    var_topAtclID = pd.read_sql(var_sqlString, con=engine)['top_value'][0]
    var_topAtclID += 1

    return var_topAtclID;


def getAliasTbl(var_entyName, var_entyType):

    engine = create_engine(database_url)

    try:
        var_sqlString = text(f"""SELECT alias.* 
                FROM basiconomics_news_schema.alias_table as alias
                left join basiconomics_news_schema.entity_table as entity on entity.entity_id = alias.entity_id
                WHERE entity.entity_type = '{var_entyType}'
                and alias.entity_name LIKE :keyPpl or alias.alias_name like :keyPpl""")
        params = {"keyPpl": f"%{var_entyName}%"}
        df_aliasVar = pd.read_sql(var_sqlString, con=engine, params=params)
    except:
        df_aliasVar = pd.DataFrame()
    return df_aliasVar;


def getCloseMatchFilterAliasDF(list_strNew, var_entyType):

    engine = create_engine(database_url)


    # Create the SQL condition dynamically
    or_clauses = " OR ".join(
        ["alias.entity_name COLLATE utf8mb4_general_ci LIKE %s OR alias.alias_name COLLATE utf8mb4_general_ci LIKE %s" for _ in list_strNew]
    )

    # Combine the base query with the dynamic OR clauses
    var_sqlString = f"""
            SELECT alias.* 
            FROM basiconomics_news_schema.alias_table as alias
            LEFT JOIN basiconomics_news_schema.entity_table as entity 
            ON entity.entity_id = alias.entity_id
            WHERE entity.entity_type = '{var_entyType}'
            AND ({or_clauses})
        """

    # Flatten the list to create the parameters tuple
    params = []
    for item in list_strNew:
        params.append(f"%{item}%")
        params.append(f"%{item}%")
    params = tuple(params)

    # Execute the query
    df_aliasVar = pd.read_sql(var_sqlString, con=engine, params=params)

    return df_aliasVar;



argsDeleteSQLRow = {'confirm_delete': ''}
def deleteSQLRow(var_tblName, var_colName, var_x, **argsDeleteSQLRow):

    var_info = 'delete from table ' + var_tblName + ' on column ' + var_colName + ' with variable ' + str(var_x)
    var_inputText = 'do you want to ' + var_info
    var_deleteConfirmation = argsDeleteSQLRow['confirm_delete'] if'confirm_delete' in argsDeleteSQLRow else input(var_inputText)

    if (var_deleteConfirmation == 'y') or (var_deleteConfirmation == 'yes'):
        # Connect to the SQL database
        engine = create_engine(database_url)

        # Construct the update query with parameters
        var_sqlString = f"""DELETE FROM {var_tblName} WHERE {var_colName} = '{var_x}';"""

        # Use a context manager to handle the connection
        with engine.connect() as connection:
            # Execute the query
            connection.execute(var_sqlString)
            # Commit the transaction
            connection.execute('commit')

            var_info += ' SUCCESSFUL!'

    else:
        var_info += ' NOT DELETED because not confirmed!'
        
    return var_info;


