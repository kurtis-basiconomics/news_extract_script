import news_variables
import pandas as pd
import time
from datetime import datetime, date, timedelta
import math
import numpy as np
import requests
from random import *
from github import Github
from io import BytesIO
import json
import re
import os
from bs4 import BeautifulSoup
import urllib.parse
from zenrows import ZenRowsClient
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import NoSuchElementException
import ast
import shutil
import difflib
import unidecode
from fuzzywuzzy import fuzz

from googlesearch import search

from openai import OpenAI

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText



import news_connectSQL



var_githubToken = news_connectSQL.var_githubToken


# key variables for news_summary and news_articles
var_dtFrAnalysis = '2024-07-01'
var_tgtAudienceMaster = 'gen z and millenials'
var_ctgrNewsDaydDelta = 10

# max tries for scraping
var_iLen = 7

pattern = re.compile(r'\n[^\n]*\n')
var_splitPattern = r'(?<=[a-z])(?=[A-Z])'
list_colsDataInfo = ['news_source', 'most_recent_date', 'oldest_date', 'number_of_today', 'count_today_not_blank', 'total_not_blank', 'extract_time']


list_removeKeywords = ["corp", "inc", "corporation", "ltd", "capital", "plc", "the", "tech", "technologies", "technology", 'services', 'enterprices', 'group', 'systèmes', 'company', 'companies', 'incorporated']

list_ctgrKeys = ['news_type', 'key_paragraph', 'key_region', 'key_people', 'key_organizations', 'recommended_headline']

list_hdrs = ['user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:104.0) Gecko/20100101 Firefox/104.0',
    'user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15',
    'user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.100.0',
    'user-agent=Mozilla/5.0 (Linux; Android 10; SM-A505FN) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.125 Mobile Safari/537.36',
    'user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.3']




# ================================ email sender ================================

def getSracpeEmailRprtText(var_runName):
    var_text = f"""Run of table {list_ctgrKeys} in server failed"""
    return var_text;


def sendEmail(var_receiverEmail, var_emailSbj, var_emailBody ):

    var_senderEmail = news_connectSQL.var_bscEmail
    var_senderPassword = news_connectSQL.var_bscPw
    
    # Create the email object
    msg = MIMEMultipart()
    msg['From'] = var_senderEmail
    msg['To'] = var_receiverEmail
    msg['Subject'] = var_emailSbj

    # Attach the body with the msg instance
    msg.attach(MIMEText(var_emailBody, 'plain'))

    # Set up the SMTP server
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()  # Secure the connection
    server.login(var_senderEmail, var_senderPassword)  # Log in to the server

    # Send the email
    text = msg.as_string()
    server.sendmail(var_senderEmail, var_receiverEmail, text)

    # Quit the server
    server.quit();




# =============================== zenrows scraper ===============================

# *************** temp for old code ***************
client = ZenRowsClient(news_connectSQL.var_zenrowsAPI) # ZenRowsClient(open(news_variables.path_zenrowsKeyMaster, 'r').read().strip('\n'))



# zenrows scrape related variables
var_urlWsjLogin = 'https://www.wsj.com/client/login'
var_urlDowJoneLogin = 'https://sso.accounts.dowjones.com/login-page'
var_zenrowsUrlAPI = 'https://api.zenrows.com/v1/'
var_userWsjLogin = news_connectSQL.var_wsjLogin
var_userWsjPassword = news_connectSQL.var_wsjPassword



argsZenrows = {'print_progress': '', 'use_this_zenrows': ''}



# zenrows params to use js rendering and json reponse
zenrows_params_1 = {"js_render": "true","json_response": "true"}

# get list of zenrows that has not got the 402 code (subscription required)
def getListActiveZenrows():
    # List secrets in the key vault
    secret_properties = news_connectSQL.az_client.list_properties_of_secrets()
    list_keyVltScrt = list()
    for secret_property in secret_properties:
        var_zenrowsAPIKeyName = secret_property.name
        if ('zenrows-' in var_zenrowsAPIKeyName) and ('zenrows-api' not in var_zenrowsAPIKeyName):
            list_keyVltScrt.append(var_zenrowsAPIKeyName)
            # print(var_zenrowsAPIKeyName)
    # get secrets that reached max tokens
    list_keyVltScrt_200 = news_connectSQL.downloadSQLQuery('zenrows_tracker', check_col = 'status_code', check_value = 402).key_vault_name.tolist()
    # only keep tokens that have not reached max
    list_actvZenrowsAPINames = list(set(list_keyVltScrt) - set(list_keyVltScrt_200))
    return list_actvZenrowsAPINames;


# wsj specific - params for wsj scrape using credentials to login
def getScrapeParamsWsj(var_zenrowsAPIKey, var_url):
    if 'www.wsj.com' in var_url:
        var_login = var_urlWsjLogin
    elif 'www.marketwatch.com' in var_url:
        var_login = var_urlDowJoneLogin
    params = {
        'url': var_login,
        'apikey': var_zenrowsAPIKey,
        'js_render': 'true',
        'js_instructions': f'''
        [
            {{ "wait_for": "input[id=emailOrUsername]" }},
            {{ "fill": [ "input[id=emailOrUsername]", "{var_userWsjLogin}" ] }},
            {{ "click": "button[id=signin-continue-btn]" }},
            {{ "wait_for": "input[id=password]" }},
            {{ "fill": [ "input[id=password]", "{var_userWsjPassword}" ] }},
            {{ "click": "button[id=signin-pass-submit-btn]" }},
            {{ "wait_for": "li[class*=profile--name]" }},
            {{ "evaluate": "document.location.href = '%s';" }},
            {{ "wait_event": "domcontentloaded" }}
        ]
        ''' % var_url,
        'premium_proxy': 'true',
        "proxy_country": "us",
        'css_extractor': '{ "header": "h1[class*=StyledHeadline]", "content": "p[class*=Paragraph]" }'
    }        
    return params;


def sendEmailUpdtZenrows(var_actvZenrowsAPIName):
    var_receiverEmail = 'kurtis@kgchua.com'
    list_actvZenrowsAPINames = getListActiveZenrows()
    var_emailBody = var_actvZenrowsAPIName + 'status code 402, will be deprecated. Email ' + var_actvZenrowsAPIName.replace('zenrows-', '') + '@gmail.com\n other working zenrows api:  ' + ', '.join(list_actvZenrowsAPINames)
    var_emailSbj = var_actvZenrowsAPIName + 'status code 402, will be deprecated'
    if len(list_actvZenrowsAPINames) <= 2:
        var_emailSbj = '2 active API remaining!! ' + var_emailSbj
        var_emailBody += '\n\n NEED MORE ACTIVE ZENROWS API'

    sendEmail(var_receiverEmail, var_emailSbj, var_emailBody )


# wsj specific - extracting text from successful scrape
def getResponceToTextWsj(response):

    try:
        data = response.json()  # Assuming the API returns JSON data
        var_fullText = '' 
        for text_value in data['content']:
            try:
                var_paragraph = text_value
                if var_paragraph != '':
                    var_fullText += var_paragraph + ' '
            except:
                pass   
    except:
        var_fullText = ''
    return var_fullText;



def getZenrowsResponse(var_url, var_param, **argsZenrows):

    var_printProgress = 'n' if 'print_progress' not in argsZenrows else argsZenrows['print_progress']
    list_actvZenrowsAPINames = getListActiveZenrows() if 'use_this_zenrows' not in argsZenrows else [argsZenrows['use_this_zenrows']]

    # list_actvZenrowsAPINames = getListActiveZenrows()
    if (var_printProgress == 'y') or (var_printProgress == 'yes'): print(list_actvZenrowsAPINames)

    var_outputStr = str(list_actvZenrowsAPINames) + ' failed to scrape data'

    # var_cntn - continue if 0, trigger 1 when successful
    var_cntn = 0
    for var_actvZenrowsAPIName in list_actvZenrowsAPINames:
        # count of the number of times the specific api run
        var_i = 0
        var_402 = 'n'
        while (var_cntn == 0) and (var_i <= var_iLen) and (var_402 == 'n'):
            var_i += 1
            var_zenrowsAPIKey = news_connectSQL.az_client.get_secret(var_actvZenrowsAPIName).value
            
            if var_param <= 1:
                client = ZenRowsClient(var_zenrowsAPIKey )
                if var_param == 0:
                    response = client.get(var_url)
                elif var_param == 1:
                    response = client.get(var_url, params=zenrows_params_1)
            elif var_param == 2:
                response = requests.get(var_zenrowsUrlAPI, params=getScrapeParamsWsj(var_zenrowsAPIKey, var_url))
            var_rspnCode = response.status_code
            if (var_printProgress == 'y') or (var_printProgress == 'yes'): print(var_actvZenrowsAPIName, var_i, var_rspnCode)
            # check status code to break look
            if var_rspnCode == 200:
                var_cntn = 1
                var_outputStr = var_actvZenrowsAPIName + '; sc' + str(var_rspnCode) + '; run ' + str(var_i) + ' scrape success'
                if (var_printProgress == 'y') or (var_printProgress == 'yes'): print(var_outputStr)
                break
            if var_rspnCode == 402:
                var_402 = 'y'
                try:
                    df_upld = pd.DataFrame(
                            [[var_actvZenrowsAPIName, var_rspnCode]],
                            columns = ['key_vault_name', 'status_code']
                        )
                    news_connectSQL.uploadSQLQuery(df_upld, 'zenrows_tracker')
                    sendEmailUpdtZenrows(var_actvZenrowsAPIName)
                    print(var_actvZenrowsAPIName, ' status 402. sent to database')
                except:
                    print(var_actvZenrowsAPIName, ' error sending to database')
        else:
            break

    return response, var_outputStr;
# ============================= zenrows scraper END =============================
# ===============================================================================





argsUploadGithubBackup = {'full_backup': '', 'hdln_backup': ''}

def uploadGithubBackup(df_var, var_tblName, **argsUploadGithubBackup):

    try:

        var_fullBackup = 'n' if 'full_backup' not in argsUploadGithubBackup else argsUploadGithubBackup['full_backup']
        var_hdlnBackup = 'n' if 'hdln_backup' not in argsUploadGithubBackup else argsUploadGithubBackup['hdln_backup']

        # Create a BytesIO buffer to hold the CSV data
        df_output = BytesIO()

        # Write the DataFrame to a CSV in the buffer
        df_var.to_csv(df_output, index=False)

        # Replace 'your_token' with your GitHub access token
        g = Github( news_connectSQL.var_githubToken )

        # Get the repository
        repo = g.get_repo(news_variables.var_githubRepo_scrape)

        # Get the CSV data from the buffer
        file_content = df_output.getvalue()


        path_csv = news_variables.var_githubRepo_scrape + '/' + var_tblName + '_'
        if (var_hdlnBackup == 'y') or (var_hdlnBackup == 'yes'):
            path_csv = path_csv + 'hdln_'
        if (var_fullBackup == 'y') or (var_fullBackup == 'yes'):
            path_csv = path_csv + 'full_'
        path_csv = path_csv + datetime.now().strftime('%Y_%m_%d_%H_%M') + '.csv'

        # print(path_csv)
        repo.create_file(path_csv, "Create new CSV file", file_content)
        print('csv upload to github success ', path_csv)
    except:
        print(var_tblName, 'csv upload to github FAILED ')



def getDataInfoTitle(var_countTdy):
    var_str = f"""{str(var_countTdy)} news source had active text"""
    return var_str;


# used in scrapers to get summary of scrape
def getDataInfo(var_tblName):

    var_sqlText = f"""
        SELECT 
            latest_date.max_created_at AS latest_date,
            date(min(news.created_at)) as earliest_date,
            SUM(CASE 
                    WHEN DATE(news.created_at) = latest_date.max_created_at 
                        AND news.news_text IS NOT NULL 
                        AND news.news_text != 'None' THEN 1
                    ELSE 0 
                END) AS with_text_today, 
            SUM(CASE 
                    WHEN DATE(news.created_at) = latest_date.max_created_at THEN 1
                    ELSE 0 
                END) AS total_today,
            SUM(CASE 
                    WHEN DATE(news.created_at) >= latest_date.max_created_at - 2
                        AND news.news_text IS NOT NULL 
                        AND news.news_text != 'None' THEN 1
                    ELSE 0 
                END) AS with_text_last3days,
            SUM(CASE 
                    WHEN DATE(news.created_at) >= latest_date.max_created_at - 2 THEN 1
                    ELSE 0 
                END) AS total_last3days,
            SUM(CASE 
                    WHEN news.news_text IS NOT NULL 
                        AND news.news_text != 'None' THEN 1
                    ELSE 0 
                END) AS with_text_overall
        FROM 
            basiconomics_news_schema.{var_tblName} news
        JOIN 
            (SELECT DATE(MAX(created_at)) AS max_created_at
            FROM basiconomics_news_schema.{var_tblName}) latest_date
        GROUP BY 
            latest_date.max_created_at;
        """
    df_var = news_connectSQL.getRawSQLQuery( var_sqlText ) 
    df_var.insert(0, 'table_name', var_tblName)
    return df_var;



def getDataInfoSumm():
    list_newsSource = news_connectSQL.downloadSQLQuery('news_source_list' ).table_name.tolist() #['news_wsj', 'news_blm', 'news_mkw', 'news_ft']
    df_output = pd.DataFrame()
    for var_tblName in list_newsSource:
        df_var = getDataInfo(var_tblName)
        df_var['extract_time'] = datetime.now()
        df_output = pd.concat([df_var, df_output], ignore_index = True)

    for var_col in ['latest_date', 'earliest_date']:
        df_output[var_col] = pd.to_datetime(df_output[var_col]).dt.date

    var_bodyText =  df_output.to_string()
    var_bodyText = '        ' + var_bodyText

    df_var = df_output.loc[pd.to_datetime(df_output.latest_date).dt.date >= datetime.today().date()]
    var_countTdy = len(df_var.loc[df_var.count_today_not_blank > 0])

    sendEmail('kurtis@kgchua.com', getDataInfoTitle(var_countTdy),  var_bodyText)



# --------------------------- cleaning files for df_newsSumm ---------------------------

# check if url ok to analyse and include in df_newsSumm
def chekIfUrlOk(var_url, var_title, list_strExclUrl, list_strExclTitle):
    var_okCntn = 'y'
    if any( ext in var_url for ext in list_strExclUrl ):
        var_okCntn ='n'
        # print('url ok')
    if len(list_strExclTitle) > 0:
        if (var_title != '') and (var_title is not None):
            if any( ext in var_title for ext in list_strExclTitle ):
                var_okCntn ='n'
                # print('title ok')
    return var_okCntn;


# filter DF is ok to analyse and include in df_newsSumm
def checkifDfOk(df_news, var_tblName):
    list_strExclUrl = news_connectSQL.downloadSQLQuery('news_exclusions', check_value = [var_tblName.replace('news_', ''), 'url'] , check_col = ['news_source', 'exclusion_on'] ).table_string.tolist()
    if len(list_strExclUrl) > 0:
        df_news = df_news.loc[df_news['news_url'].apply(lambda x: not any(substring in x for substring in list_strExclUrl))]
    list_strExclTtl = news_connectSQL.downloadSQLQuery('news_exclusions', check_value = [var_tblName.replace('news_', ''), 'title'] , check_col = ['news_source', 'exclusion_on'] ).table_string.tolist()
    if len(list_strExclTtl) > 0:
        df_news = df_news.loc[df_news['news_title'].apply(lambda x: not any(substring in x for substring in list_strExclTtl))]
    return df_news;
# ------------------------- END cleaning files for df_newsSumm -------------------------
# --------------------------------------------------------------------------------------



# IN DJANGO
var_prflList = str(news_connectSQL.downloadSQLQuery('profile_goal_options')['profile_goal'].tolist()).replace("'", "")
var_newsTypeList = str(news_connectSQL.downloadSQLQuery('news_type_options')['news_type'].tolist()).replace("'", "")






# ----------------------- BASE CHATGPT FUNCTIONS -----------------------
# IN DJANGO
argsChatGptResponse = {'api_key': ''}

# SUMBIT A LIST FOR CHAT GPT TO ANALYZE
def chatgpt_get_response_GPT4(list_message, **argsChatGptResponse):

    var_apiKey = argsChatGptResponse['api_key'] if 'api_key' in argsChatGptResponse else open(news_variables.path_openaiKey_120423, 'r').read().strip('\n')

    client = OpenAI(
        api_key = var_apiKey,  # this is also the default, it can be omitted
        )

    chat_completion = client.chat.completions.create(
            model="gpt-4o",
            messages=list_message
        )
    return  chat_completion.choices[0].message.content



# update and tracking the chat
def chatgpt_update_chat(list_message, role, content):
    list_message.append({"role": role, "content": content})
    return list_message


# OPENING MESSAGE OF CHATGPT API
def chatgpt_initial_message(var_intial_message):
    var_messagePrompt = [
        {"role": "system", "content": var_intial_message}
        ]
    return var_messagePrompt

# --------------------- BASE CHATGPT FUNCTIONS END ---------------------
# ----------------------------------------------------------------------




# --------------------------------- DATA CLEANING FUNCTIONS ---------------------------------


def split_names(text):
    
    # Split the text using the pattern
    names = re.split(var_splitPattern, text)
    
    # Join the names with a comma and a space
    result = ', '.join(names)
    
    return result



def convert_to_datetime(entry):
    # Check for absolute date pattern (e.g., "December 25 2022")
    try:
        return datetime.strptime(entry, '%B %d %Y')
    except ValueError:
        pass
    
    # Check for relative date pattern (e.g., "3 hours ago")
    hours_ago_pattern = re.match(r'(\d+) HOURS AGO', entry)
    minutes_ago_pattern = re.match(r'(\d+) minutes ago', entry)
    # ... add more patterns if needed
    
    if hours_ago_pattern:
        hours = int(hours_ago_pattern.group(1))
        return datetime.now() - timedelta(hours=hours)
    elif minutes_ago_pattern:
        minutes = int(minutes_ago_pattern.group(1))
        return datetime.now() - timedelta(minutes=minutes)
    # ... handle more patterns f added

    return None  # For any unahandled format



# CLEAN TEXT FOR UPLOAD TO CHATGPT

def clean_text(text):
    try:
        text = re.sub(r'(?<=\d)\.(?=\d)', 'prd', text)
        text = text.replace('&', ' and ')
        text = text.replace('-', ' ')
        text = text.replace('%', ' percent ')
        text = text.replace('$', ' dollar ')
        text = text.replace('£', ' pound ')
        text = text.replace('€', ' euro ')
        text = text.lower()
        text = re.sub(r'\s+', ' ', text)  # remove extra whitespace
        text = re.sub(r'[^\w\s]', '', text)  # remove punctuation
        text = text.rstrip()
        text = text.lstrip()
    except:
        print(text, ' cleaning failed')
    return text




# Function to remvaove keywords from the string
def remove_keywords(input_string):

    var_rmvKywdPattern = re.compile(r'\b(' + '|'.join(list_removeKeywords) + r')\b', re.IGNORECASE)
    words_list = input_string.split(' ')
    filtered_words_list = [var_rmvKywdPattern.sub('', word).strip() for word in words_list]
    return ' '.join(filtered_words_list)


# using chatgpt to reformat from string to dictionary
# IN DJANGO
def chatgpt_checkFotmat(var_newsCtgr):
    var_chkDictFormat = """I will pass a string and I need you to change the string so I can use the function ast.literal_eval and convert it to a python dictionary format."""
    list_checkFormat = chatgpt_initial_message(var_chkDictFormat)
    list_checkFormat = chatgpt_update_chat(list_checkFormat, 'assistant', 'Pass the string')
    list_checkFormat = chatgpt_update_chat(list_checkFormat, 'user', var_newsCtgr)
    list_checkFormat = chatgpt_update_chat(list_checkFormat, 'user', 'help me remove quotation marks that might cause an error')
    list_checkFormat = chatgpt_update_chat(list_checkFormat, 'user', 'I will be using the ensure the ast.literal_eval with the output so only return text that is ready to be converted')
    var_newsCtgr = chatgpt_get_response_GPT4(list_checkFormat)
    # dict_newsCtgr = ast.literal_eval(var_newsCtgr)

    return var_newsCtgr;


# using dictionary keys to split string and create a new dictionary
def strToDictWithKey(var_newsCtgr):
    dict_newsCtgr = dict()
    for var_x1 in range( len(list_ctgrKeys) ):
        var_str = ''
        try:
            var_ctgrKey_1 = list_ctgrKeys[var_x1]
            if var_x1 < len(list_ctgrKeys) - 1:
                var_ctgrKey_2 = list_ctgrKeys[var_x1 + 1]
                var_str = var_newsCtgr.split(var_ctgrKey_1)[1].split(var_ctgrKey_2)[0]
                var_str = var_str.replace("'", "").replace(":", "").replace("  ", "").replace('“', "").replace('”', "")
                if var_ctgrKey_1 == 'news_type':  var_str = var_str.replace(",", "")
                var_str = var_str.lstrip()
                var_str = var_str.rstrip()
                #  print(var_ctgrKey_1, var_ctgrKey_2, var_str)
            elif (var_x1 == len(list_ctgrKeys) - 1):
                var_str = var_newsCtgr.split(var_ctgrKey_1)[1].split('}')[0]
                var_str = var_str.replace("'", "").replace(":", "").replace("  ", "").replace('“', "").replace('”', "")
                if var_ctgrKey_1 == 'news_type':  var_str = var_str.replace(",", "")
                var_str = var_str.lstrip()
                var_str = var_str.rstrip()

            dict_newsCtgr[var_ctgrKey_1] = var_str
        except:
            dict_newsCtgr[var_ctgrKey_1] = var_str

    return dict_newsCtgr;


# convert string to Dictionary
# IN DJANGO
argsGetDictFromStr = {'multi_dict': '', 'use_chatgpt': ''}

def getDictFromStr(var_newsCtgr, **argsGetDictFromStr):

    var_multiDict = 'n' if 'multi_dict' not in argsGetDictFromStr else argsGetDictFromStr['multi_dict']
    var_useChatgpt = 'y' if 'use_chatgpt' not in argsGetDictFromStr else argsGetDictFromStr['use_chatgpt']
    
    dict_newsCtgr = ''

    if not isinstance(dict_newsCtgr, dict):
        try:
            dict_newsCtgr = strToDictWithKey(var_newsCtgr)
            # print('string to dict')
            # print(type(dict_newsCtgr))
        except:
            pass

    if not isinstance(dict_newsCtgr, dict):
        try:
            var_newsCtgr = (var_newsCtgr).replace("{\n    'undefined': {'summary': ''}, 'duration': 'long', 'impact_direction': 'neutral'\n}", "{\n    'undefined': {'summary': '', 'duration': 'long', 'impact_direction': 'neutral'\n}}")
            dict_newsCtgr = ast.literal_eval(var_newsCtgr)
            print('clean python')
        except:
            pass     

    if not isinstance(dict_newsCtgr, dict):
        try:
            var_newsCtgr = (var_newsCtgr).replace("{\n    'undefined': {'summary': ''}, 'duration': 'long', 'impact_direction': 'negative'\n}", "{\n    'undefined': {'summary': '', 'duration': 'long', 'impact_direction': 'negative'\n}}")
            dict_newsCtgr = ast.literal_eval(var_newsCtgr)
            # print('clean python')
        except:
            pass    

    if not isinstance(dict_newsCtgr, dict):
        try:
            var_newsCtgr = (var_newsCtgr).replace("{\n    'undefined': {'summary': ''}, 'duration': 'long', 'impact_direction': 'positive'\n}", "{\n    'undefined': {'summary': '', 'duration': 'long', 'impact_direction': 'positive'\n}}")
            dict_newsCtgr = ast.literal_eval(var_newsCtgr)
            # print('clean python')
        except:
            pass    

    if not isinstance(dict_newsCtgr, dict):
        if (var_multiDict == 'n') or (var_multiDict == 'no'):
            try:
                # print('not multi dict')
                var_newsCtgr = '{' + var_newsCtgr.split('{')[1].split('}')[0] + '}'
                dict_newsCtgr = ast.literal_eval(var_newsCtgr)
            except:
                pass
    
    if not isinstance(dict_newsCtgr, dict):
        try:
            var_newsCtgr = (var_newsCtgr).replace("python\n", '')
            dict_newsCtgr = ast.literal_eval(var_newsCtgr)
        except:
            pass    

    if not isinstance(dict_newsCtgr, dict):
        try:
            var_newsCtgr = (var_newsCtgr).replace("```", '')
            dict_newsCtgr = ast.literal_eval(var_newsCtgr)
        except:
            pass

    if not isinstance(dict_newsCtgr, dict):
        try:
            # var_newsCtgr = (var_newsCtgr).replace("'s ", 's ')
            var_newsCtgr = re.sub(r"(\w)'(\w)", r"\1\2", var_newsCtgr)
            dict_newsCtgr = ast.literal_eval(var_newsCtgr)
        except:        
            pass

    if not isinstance(dict_newsCtgr, dict):
        try:
            # var_newsCtgr = (var_newsCtgr).replace("’s  ", 's ')
            var_newsCtgr = re.sub(r"(\w)’(\w)", r"\1\2", var_newsCtgr)
            dict_newsCtgr = ast.literal_eval(var_newsCtgr)
        except:        
            pass

    if not isinstance(dict_newsCtgr, dict):
        try:
            var_newsCtgr = (var_newsCtgr).replace("s' ", 's ')
            dict_newsCtgr = ast.literal_eval(var_newsCtgr)
        except:        
            pass

    if not isinstance(dict_newsCtgr, dict):
        try:
            var_newsCtgr = (var_newsCtgr).replace("s’  ", 's ')
            dict_newsCtgr = ast.literal_eval(var_newsCtgr)
        except:        
            pass

    if not isinstance(dict_newsCtgr, dict):
        try:
            var_newsCtgr = (var_newsCtgr).replace("<short, medium, long>", "''")
            dict_newsCtgr = ast.literal_eval(var_newsCtgr)
        except:        
            pass

    if not isinstance(dict_newsCtgr, dict):
        try:
            var_newsCtgr = (var_newsCtgr).replace("<positive, negative, neutral>", "''")
            dict_newsCtgr = ast.literal_eval(var_newsCtgr)
        except:        
            pass

    if not isinstance(dict_newsCtgr, dict):
        try:
            var_newsCtgr = (var_newsCtgr).replace("{'summary': ''}, ", "{'summary': '', ")
            dict_newsCtgr = ast.literal_eval(var_newsCtgr)
        except:        
            pass


    if not isinstance(dict_newsCtgr, dict):
        try:
            var_newsCtgr = (var_newsCtgr).replace("\'", '')
            dict_newsCtgr = ast.literal_eval(var_newsCtgr)
        except:
            pass

    if not isinstance(dict_newsCtgr, dict):
        try:
            var_newsCtgr = (var_newsCtgr).replace("\n", '')
            dict_newsCtgr = ast.literal_eval(var_newsCtgr)
        except:
            pass

    if not isinstance(dict_newsCtgr, dict):
        try:
            var_newsCtgr = var_newsCtgr.replace("{", "{'").replace(": ", "': '").replace(", ", "', '").replace("}", "'}")
            dict_newsCtgr = ast.literal_eval(var_newsCtgr)
        except:
            pass

    if not isinstance(dict_newsCtgr, dict) and ((var_useChatgpt == 'y') or (var_useChatgpt == 'yes')):
        print('use chatgpt for formatting')
        try:
            var_newsCtgr = chatgpt_checkFotmat(var_newsCtgr)
            dict_newsCtgr = ast.literal_eval(var_newsCtgr)
        except:
            dict_newsCtgr = var_newsCtgr
                                        
    # else:
    #     dict_newsCtgr = var_newsCtgr

    return dict_newsCtgr;
 



def getListFromStr(var_listStr):
    var_listStr = re.sub(r"(\w)'(\w)", r"\1\2", var_listStr)
    if '[' in var_listStr:
        var_listStr = var_listStr.split('[')[1]

    if ']' in var_listStr:
        var_listStr = var_listStr.split(']')[0]
        
    var_listStr = '[' + var_listStr + ']'
    list_var = ast.literal_eval(var_listStr)
    return list_var;





def delete_chrome_profile(profile_name):
    user = os.getenv("USER") or os.getenv("USERNAME")  # Get the username

    if os.name == 'nt':  # Windows
        chrome_profile_path = f"C:\\Users\\{user}\\AppData\\Local\\Google\\Chrome\\User Data\\{profile_name}"
    elif os.name == 'posix':  # macOS and Linux
        chrome_profile_path = f"/Users/{user}/Library/Application Support/Google/Chrome/{profile_name}" if sys.platform == 'darwin' else f"/home/{user}/.config/google-chrome/{profile_name}"
    else:
        print("Unsupported OS")
        return

    if os.path.exists(chrome_profile_path):
        try:
            shutil.rmtree(chrome_profile_path)
            print(f"Chrome profile '{profile_name}' has been deleted successfully.")
        except Exception as e:
            print(f"An error occurred while deleting the profile: {e}")
    else:
        print(f"The profile '{profile_name}' does not exist.")







# ------------------------------------ ENTITY ALLOCATION ------------------------------------

argsEntityAlias = {'upload_to_sql': '', 'create_new_entity': '', 'stock_ticker': ''}

def get_ticker(company_name):
    query = f"{company_name} stock ticker"
    for result in search(query):
        try:
            response = requests.get(result)
            soup = BeautifulSoup(response.content, 'html.parser')
            # Find the ticker symbol in the page content
            if "symbol" in result:
                return result.split("symbol=")[1].split("&")[0]
            elif "quote" in result:
                return result.split("quote/")[1].split("/")[0]
        except Exception as e:
            continue
    return None


def getCurrAliasTbl(var_cmpyNameFull, var_entyType):

    df_aliasVar = news_connectSQL.getAliasTbl(var_cmpyNameFull, var_entyType)

    if var_entyType != 'region':

        # exclude last word only
        list_cmpyNameNotLast = remove_keywords(var_cmpyNameFull)
        if ( len( list_cmpyNameNotLast.split(' ')[ : -1] ) > 1 ):
            var_cmpyNameNotLast = ''
            for var_cmpyNameNotLast_1 in list_cmpyNameNotLast.split(' ')[ : -1]:
                var_cmpyNameNotLast += var_cmpyNameNotLast_1 + ' '
            var_cmpyNameNotLast = var_cmpyNameNotLast [ : -1]    
            # print('not last')
            df_aliasVarNotLast = news_connectSQL.getAliasTbl(var_cmpyNameNotLast, var_entyType)
            df_aliasVar = pd.concat([df_aliasVar, df_aliasVarNotLast], ignore_index = True)

        if var_entyType == 'company':

            # get first word only
            var_cmpyNameFirst = remove_keywords(var_cmpyNameFull)
            if ( len(var_cmpyNameFirst.split(' ')) > 1 ):
                var_cmpyNameFirst = var_cmpyNameFirst.split(' ')[ : -1][0]
                # print('first')
                df_aliasVarFirst = news_connectSQL.getAliasTbl(var_cmpyNameFirst, var_entyType)
                df_aliasVar = pd.concat([df_aliasVar, df_aliasVarFirst], ignore_index = True)

            # get last word only
            var_cmpyNameLast = remove_keywords(var_cmpyNameFull)
            if ( len(var_cmpyNameLast.split(' ')) > 1 ):
                var_cmpyNameLast = var_cmpyNameLast.split(' ')[ : -1][-1]
                # print(' last name')
                df_aliasVarLast = news_connectSQL.getAliasTbl(var_cmpyNameLast, var_entyType)
                df_aliasVar = pd.concat([df_aliasVar, df_aliasVarLast], ignore_index = True)     

    df_aliasVar.drop_duplicates(subset = ['alias_name'], inplace = True, ignore_index = True)

    return df_aliasVar;



var_cntyEntyListStr = ', '.join(news_connectSQL.downloadSQLQuery('entity_table', check_col = 'entity_type', check_value = 'region')['entity_name'].to_list())

# using chatgpt to reformat from string to dictionary
def chatgpt_checkCountry(var_cntr):

    var_initialMsg = 'you will share a name of a place and i will pick a name from you list of options on what that country or region best represents it.'


    var_asst = f"""I will chose from { var_cntyEntyListStr }. If there is no good option, I will return simply 'unallocated'. I will only return the country or region name with no other text and I will not provide any explanation""" 

    list_check = chatgpt_initial_message(var_initialMsg)
    list_check = chatgpt_update_chat(list_check, 'user', var_cntr)
    list_check = chatgpt_update_chat(list_check, 'assistant', var_asst)
    var_newsStr = chatgpt_get_response_GPT4(list_check, api_key = open(news_variables.path_openaiKey_country_code, 'r').read().strip('\n') ) 

    return var_newsStr;





def createNewCompanyEntity(var_entyName, var_entyType, **argsEntityAlias):

    var_uploadOk = argsEntityAlias['upload_to_sql'] if 'upload_to_sql' in argsEntityAlias else 'y'

    if ('stock_ticker' in argsEntityAlias) and (var_entyType == 'company'):
        var_tckr = argsEntityAlias['stock_ticker']  
    elif ('stock_ticker' not in argsEntityAlias) and (var_entyType == 'company'): 
        var_tckr = get_ticker(var_entyName)
    else:
        var_tckr = ''

    var_entityOutputStr = ''

    var_entyId = news_connectSQL.getNewEntityID()

    list_values = [var_entyName, var_entyId, var_entyType]
    list_cols = ['entity_name', 'entity_id', 'entity_type']

    if (var_tckr is not None) and (var_tckr != ''):
        var_addnInfo = 'tckr ' + var_tckr
        list_values.append( var_addnInfo )
        list_cols.append( 'additional_info' )

    df_varEnty = pd.DataFrame( [list_values], columns = list_cols )

    var_entityOutputStr = 'entity: ' + var_entyName + ' ' + str(var_entyId) + ' created'

    if (var_uploadOk == 'yes') or (var_uploadOk == 'y'):
        news_connectSQL.uploadSQLQuery(df_varEnty, 'entity_table')
        var_entityOutputStr += ' PRINTED'

    return var_entityOutputStr, df_varEnty;





# main entity alias allocation code
def checkStringInTable(var_strFull, var_entyType, **argsEntityAlias):

    # print(var_strFull)
    var_strFull = var_strFull.rstrip()
    var_strFull = var_strFull.lstrip()

    var_uploadOk = argsEntityAlias['upload_to_sql'] if 'upload_to_sql' in argsEntityAlias else 'y'
    var_forceNewEntity = argsEntityAlias['create_new_entity'] if 'create_new_entity' in argsEntityAlias else 'n'

    
    # setting default responses
    var_aliasOutputStr = ''
    var_chatgptRslt =''

    var_createNewEnty = 'y'
    var_createNewAlias = 'y'


    # ACCOMODATE FOR REGION
    # get alias table 
    df_aliasVar = pd.DataFrame()
    if (var_forceNewEntity == 'n') or (var_forceNewEntity == 'no'):
        df_aliasVar = getCurrAliasTbl(var_strFull, var_entyType) 
        # print(var_strFull, df_aliasVar)
        if (var_entyType == 'region'):
            if (df_aliasVar.empty == False) and (var_strFull in df_aliasVar.alias_name.unique() ):
                var_aliasName = var_strFull
                var_entyName, var_entyId = df_aliasVar.loc[ df_aliasVar.alias_name == var_aliasName ][['entity_name', 'entity_id']].values[0]
            else:
                var_entyName = chatgpt_checkCountry(var_strFull)
                print(var_entyName, 'chatgpt output')
                if (var_entyName != 'unallocated'):
                    var_entyName = clean_text(var_entyName)
                    df_aliasVar = getCurrAliasTbl(var_entyName, var_entyType)
                else:
                    var_entityOutputStr = var_strFull + ' country unallocated '
                    var_createNewEnty = 'n'
                    var_createNewAlias = 'n'
                    var_entyName, var_entyId = 'unallocated', 'unallocated '

        if var_entyType != 'region':
            if (df_aliasVar.empty == False) and ((var_forceNewEntity == 'n') or (var_forceNewEntity == 'no')):
                if var_strFull not in df_aliasVar.alias_name.unique():

                    # company alias checker
                    list_closestMatch = difflib.get_close_matches(var_strFull, df_aliasVar.alias_name.unique(), cutoff = 0.8)
                    # only include from closest match if any of the cords from company are present
                    list_filteredClosestMatch = [var_item for var_item in list_closestMatch if any(var_substring in var_item for var_substring in remove_keywords(var_strFull).split(' ') )]
                    if len(list_closestMatch) > 0:
                        # print(list_closestMatch)
                        try:
                            var_aliasName = list_closestMatch[0]
                            var_entyName, var_entyId = df_aliasVar.loc[ df_aliasVar.alias_name == var_aliasName ][['entity_name', 'entity_id']].values[0]
                            var_createNewEnty = 'n'
                            var_entityOutputStr = var_strFull + ' ' + var_entyType + ' using closest match of entity '
                        except:
                            print(var_strFull, 'with error using closest match' )
                            var_entityOutputStr = var_strFull + ' ' + var_entyType + ' ERROR on using closest match of entity '
                # alias is found. no further actions needed
                else:
                    var_aliasName = var_strFull
                    var_entyName, var_entyId = df_aliasVar.loc[ df_aliasVar.alias_name == var_aliasName ][['entity_name', 'entity_id']].values[0]
                    var_createNewEnty = 'n'
                    var_createNewAlias = 'n'
                    var_entityOutputStr = var_strFull + ' ' + var_entyType + ' already in table '
                    # print('no new entity or alias required')
        
        else:
            if (df_aliasVar.empty == False) and (var_entyName != 'unallocated') and ((var_forceNewEntity == 'n') or (var_forceNewEntity == 'no')):
                if var_entyName in df_aliasVar.entity_name.unique():
                    # print(var_entyName, ' found')
                    var_entityOutputStr = var_strFull + ' ' + var_entyType + ' already in table '
                    if var_strFull in df_aliasVar.alias_name.unique(): 
                        print(var_entyName, ' entity available and alias ', var_strFull, ' found')
                        var_aliasName = var_strFull
                        var_entyName, var_entyId = df_aliasVar.loc[ df_aliasVar.alias_name == var_aliasName ][['entity_name', 'entity_id']].values[0]
                        var_createNewEnty = 'n'
                        var_createNewAlias = 'n'
                    else:
                        (var_entyName, ' entity available but not alias ', var_strFull)
                        var_entyName, var_entyId = df_aliasVar.loc[ df_aliasVar.entity_name == var_entyName ][['entity_name', 'entity_id']].values[0]
                        var_createNewEnty = 'n'
                
    # ADD ENTITY_TYPE IN  CREATING NEW ENTITY
    # create a new entity
    if (var_createNewEnty == 'y') or (var_createNewEnty == 'yes') or (var_forceNewEntity == 'yes') or (var_forceNewEntity == 'y'):
        var_entityNew = createNewCompanyEntity(var_strFull, var_entyType, upload_to_sql = var_uploadOk)
        var_aliasName = var_strFull
        var_entityOutputStr, df_varEnty = var_entityNew
        var_entyName = df_varEnty['entity_name'][0]
        var_entyId = int( df_varEnty['entity_id'][0] )

        # reassign alias if var_forceNewEntity = yes if alias already available, if not available go through full process of creating a new alias
        # could be moved to create alias section
        if (var_forceNewEntity == 'yes') or (var_forceNewEntity == 'y'):
            df_aliasVarEnty = news_connectSQL.downloadSQLQuery('alias_table', check_col = 'alias_name', check_value = var_aliasName)
            var_aliasOutputStr = 'alias: ' + var_aliasName + ' to entity_name:  ' + var_entyName + ' and entity_id:  ' + str(var_entyId) + ' from entity_name:  ' + df_aliasVarEnty['entity_name'].values[0] + ' entity_id:  ' + str(df_aliasVarEnty['entity_id'].values[0])
            if df_aliasVarEnty.empty == False:
                try:
                    news_connectSQL.replaceSQLQuery('alias_table', 'alias_name', var_aliasName, ['entity_name', 'entity_id', 'updated_at'], [ var_entyName,  var_entyId, datetime.now()], upload_to_sql = var_uploadOk)
                    news_connectSQL.replaceSQLQuery('article_entity', ['alias_name', 'entity_type'], [var_aliasName, var_entyType], ['entity_name', 'entity_id', 'updated_at'], [ var_entyName,  var_entyId, datetime.now()], upload_to_sql = var_uploadOk)
                    var_createNewAlias = 'n'
                    if (var_uploadOk == 'yes') or (var_uploadOk == 'y'):
                        var_aliasOutputStr += ' PRINTED'
                except:
                    print('\n', var_strFull, ' error changing alias for ', str(var_aliasName), '\n')
            else:
                var_createNewAlias = 'y'

    # create alias section
    list_alias = [var_strFull]
    if (var_createNewAlias == 'y') or (var_createNewAlias == 'yes'):

        func_contains_keyword = any(var_keyword.lower() in var_entyName.lower() for var_keyword in list_removeKeywords)
        try:
            if func_contains_keyword:
                modified_string = remove_keywords(var_strFull)
                if modified_string not in df_aliasVar.alias_name.unique():
                    list_alias.append(modified_string)
                else:
                    var_entyName2 = df_aliasVar.loc[df_aliasVar.alias_name == modified_string].entity_table.values[0]
                    # print(modified_string, ' already in alias_table with entity name ', var_entyName2)
        except:
            pass
        df_varAlias = pd.DataFrame(list_alias, columns = ['alias_name'])
        df_varAlias[['entity_name', 'entity_id', 'entity_type']] = var_entyName, var_entyId, var_entyType
        df_varAlias.drop_duplicates(subset = ['alias_name'], inplace = True)

        var_aliasOutputStr = 'alias: ' + str(list_alias)[1: -1] + ' of ' + var_entyName + ' created'
        # print(df_varAlias.tail())

        if (var_uploadOk == 'yes') or (var_uploadOk == 'y'):    
            try:        
                news_connectSQL.uploadSQLQuery(df_varAlias, 'alias_table')
                var_aliasOutputStr += ' PRINTED'
            except:
                print('\n', var_strFull, ' error on uploading alias\n', str(df_varAlias), '\n')
            

    # NEED TO ADD TO UNALLOCATED TABLE
    if (var_entyName == 'unallocated') and ((var_uploadOk == 'yes') or (var_uploadOk == 'y')):    
        df_unAlloc = pd.DataFrame([[var_strFull, var_entyName]], columns = ['alias_name', 'entity_name'])
        var_aliasOutputStr += var_strFull + ' unallocated'
        try:        
            news_connectSQL.uploadSQLQuery(df_unAlloc, 'alias_additions')
            var_aliasOutputStr += ' PRINTED'
        except:
            print('\n', var_strFull, ' error on uploading alias\n', str(df_varAlias), '\n')

    # print(var_strFull, var_entityOutputStr, var_aliasOutputStr)

    var_outputStr = var_entityOutputStr + ';  ' + var_aliasOutputStr

    return var_outputStr, var_entyName, var_entyId, list_alias;

# ---------------------------------- ENTITY ALLOCATION END ----------------------------------
# -------------------------------------------------------------------------------------------




def getSummNews(list_url):
    for var_url in list_url:
        var_rcmdHdln, var_hdlnDt, var_summ, var_newsType, var_ctgr, var_keyPpl, var_keyRgn, var_keyOrg = news_connectSQL.downloadSQLQuery('news_summary', check_col = 'news_url', check_value = var_url)[['recommended_headline', 'headline_date', 'summary', 'news_type', 'category', 'key_people', 'key_region', 'key_organizations']].values[0]
        print(var_rcmdHdln, var_hdlnDt, var_newsType, var_ctgr)
        print(var_url)
        print(var_keyPpl, '; ', var_keyRgn, '; ', var_keyOrg)
        print(var_summ, '\n\n')


