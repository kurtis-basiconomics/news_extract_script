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

from openai import OpenAI

# OLD FUNCTION TO CALL API
# openai.api_key = open('api_key_120423.txt', 'r').read().strip('\n')

import news_connectSQL


client = ZenRowsClient(open(news_variables.path_zenrowsKeyMaster, 'r').read().strip('\n'))


pattern = re.compile(r'\n[^\n]*\n')

list_colsDataInfo = ['news_source', 'most_recent_date', 'oldest_date', 'number_of_today', 'count_today_not_blank', 'total_not_blank', 'extract_time']

var_tgtAudienceMaster = 'gen z and millenials'


list_hdrs = ['user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:104.0) Gecko/20100101 Firefox/104.0',
    'user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15',
    'user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.100.0',
    'user-agent=Mozilla/5.0 (Linux; Android 10; SM-A505FN) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.125 Mobile Safari/537.36',
    'user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.3']




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
        g = Github(open( news_variables.path_githubKey , 'r').read().strip('\n'))

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




# --------------------------- cleaning files for df_newsSumm ---------------------------
argsCleanSQLDownload = {'source_label': '', 'date_from': ''}

def cleanSQLDownload(var_TblName, **argsCleanSQLDownload):

    var_newsSrc = var_TblName if 'source_label' not in argsCleanSQLDownload else argsCleanSQLDownload['source_label']

    if 'date_from' not in argsCleanSQLDownload:
        df_var = news_connectSQL.downloadSQLQuery(var_TblName)
    else:
        df_var = news_connectSQL.downloadSQLQuery(var_TblName, date_col = 'created_at', date_from = argsCleanSQLDownload['date_from'])

    df_var.sort_values(by = ['headline_date', 'news_text'], ascending = True, inplace = True, ignore_index = 0)
    df_var.drop_duplicates(subset = ['news_url'], inplace = True, ignore_index = True, keep = 'last'    )

    df_var.created_at.fillna(pd.NaT, inplace = True)
    df_var.news_text.replace('', pd.NaT, inplace = True)
    df_var.replace('None', pd.NaT, inplace = True)
    # df_var = df_var[df_var.headline_date != datetime.date(1, 1, 1)]
    for var_col in ['headline_date', 'created_at']:
        df_var[var_col] = pd.to_datetime(df_var[var_col], format = 'mixed').dt.date
    df_var['news_source'] = var_newsSrc
    
    df_var.dropna(subset = ['news_text'], inplace = True, ignore_index = True)
    
    return df_var;


def getDataInfo(var_TblName, **argsCleanSQLDownload):

    if 'date_from' not in argsCleanSQLDownload:
        df_var = news_connectSQL.downloadSQLQuery(var_TblName)
    else:
        df_var = news_connectSQL.downloadSQLQuery(var_TblName, date_col = 'created_at', date_from = argsCleanSQLDownload['date_from'])

    var_newsSrc = var_TblName if 'source_label' not in argsCleanSQLDownload else argsCleanSQLDownload['source_label']

    df_var.dropna(subset = ['created_at'], inplace = True)
    var_lastDt = pd.to_datetime(df_var.created_at.dropna().max())
    var_firstDt = pd.to_datetime(df_var.created_at.dropna().min())
    var_tdyCount = len(df_var.loc[pd.to_datetime(df_var.created_at) >= pd.to_datetime(var_lastDt.date())] )

    var_tdyWithText = len(df_var.loc[(df_var.created_at >=  pd.to_datetime(var_lastDt.date())) & (~pd.isna(df_var.news_text) ) & (df_var.news_text != '') ])
    var_ttlWithText = len(df_var.loc[(~pd.isna(df_var.news_text) ) | (df_var.news_text != '')])

    df_var_1 = pd.DataFrame(
        [[var_newsSrc, var_lastDt, var_firstDt, var_tdyCount, var_tdyWithText, var_ttlWithText, datetime.now() ]], 
        columns = list_colsDataInfo)
    
    return df_var_1;


def cleanCSVDownload(var_fileName, var_newsSrc):
    df_var = pd.read_csv(var_fileName, index_col = 0)
    df_var.drop_duplicates(subset = ['news_url'], inplace = True, ignore_index = True, keep = 'last'    )

    df_var.created_at.fillna(pd.NaT, inplace = True)
    df_var.news_text.replace('', pd.NaT, inplace = True)
    df_var.replace('None', pd.NaT, inplace = True)

    for var_col in ['headline_date', 'created_at']:
        df_var[var_col] = pd.to_datetime(df_var[var_col], format = 'mixed').dt.date
        
    df_var['news_source'] = var_newsSrc
    
    df_var.dropna(subset = ['news_text'], inplace = True, ignore_index = True)
    
    return df_var;

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




# ------------------------------------ REGION ALLOCATION ------------------------------------

# using chatgpt to reformat from string to dictionary
def chatgpt_checkCountry(var_cntr):
    
    var_initialMsg = 'you will share a name of a place and i will pick the a name from a list on what that country it is.'

    var_asst = f"""I will chose from { str(news_connectSQL.downloadSQLQuery('country_code')['country_name'].tolist()) }. If there is no good option, I will return simply 'unallocated'. I will only return the country name with no other text and I will not provide any explanation""" 

    list_check = chatgpt_initial_message(var_initialMsg)
    list_check = chatgpt_update_chat(list_check, 'user', var_cntr)
    list_check = chatgpt_update_chat(list_check, 'assistant', var_asst)
    var_newsStr = chatgpt_get_response_GPT4(list_check, api_key = open(news_variables.path_openaiKey_country_code, 'r').read().strip('\n') ) 

    return var_newsStr;


def uploadRegionUnallocated(var_rgn):
    df_var = pd.DataFrame( [[var_rgn, datetime.now(), 1]] , columns = ['region_allocation', 'created_at', 'status_id'] )
    news_connectSQL.uploadSQLQuery( df_var , 'region_missing')
    

def getRegionAllocation(var_rgn):

    var_ii = 1 # Check to see if upload worked

    if var_rgn not in news_connectSQL.downloadSQLQuery('country_mapping')['region_allocation'].tolist():

        var_ii = 0

        print(var_rgn, ' creating new region')

        var_cnty = clean_text( chatgpt_checkCountry( var_rgn ) )

        if var_cnty in news_connectSQL.downloadSQLQuery('country_code')['country_name'].tolist():

            try:
                var_cntyCode = news_connectSQL.downloadSQLQuery('country_code', check_col = 'country_name', check_value = var_cnty)['country_code'][0]
                df_var = pd.DataFrame( [[var_rgn, var_cnty, var_cntyCode, datetime.now()]] , columns = ['region_allocation', 'country_name', 'country_code', 'created_at'])
                news_connectSQL.uploadSQLQuery(df_var, 'country_mapping')
                var_ii = 1
                print(df_var)
            except:
                uploadRegionUnallocated(var_rgn)
                print(var_cnty, ' error cant allocate mapping')

        else:
            uploadRegionUnallocated(var_rgn)
            print(var_rgn, ' country not recognized')

    if var_ii == 1:
        df_var_1 = news_connectSQL.downloadSQLQuery('country_mapping', check_col = 'region_allocation', check_value = var_rgn)
        df_var_1 = df_var_1[['region_allocation', 'country_name', 'country_code']]
    else:
        df_var_1 = pd.DataFrame()
        
    return df_var_1;
# ---------------------------------- REGION ALLOCATION END ----------------------------------
# -------------------------------------------------------------------------------------------




# --------------------------------- DATA CLEANING FUNCTIONS ---------------------------------

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
    # ... handle more patterns if added

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
    except:
        print(text, ' cleaning failed')
    return text




# using chatgpt to reformat from string to dictionary
# IN DJANGO
def chatgpt_checkFotmat(var_newsCtgr):
    var_chkDictFormat = """I will pass a string and you make sure it is in a python dictionary format. change the format of the text if necessary"""
    list_checkFormat = chatgpt_initial_message(var_chkDictFormat)
    list_checkFormat = chatgpt_update_chat(list_checkFormat, 'assistant', 'Pass the string')
    list_checkFormat = chatgpt_update_chat(list_checkFormat, 'user', var_newsCtgr)
    list_checkFormat = chatgpt_update_chat(list_checkFormat, 'user', 'ensure that your output will already be the correct dictionary format without any other text')
    var_newsCtgr = chatgpt_get_response_GPT4(list_checkFormat)
    dict_newsCtgr = ast.literal_eval(var_newsCtgr)

    return dict_newsCtgr;


# convert string to Dictionary
# IN DJANGO
argsGetDictFromStr = {'multi_dict': '', 'use_chatgpt': ''}

def getDictFromStr(var_newsCtgr, **argsGetDictFromStr):

    var_multiDict = 'n' if 'multi_dict' not in argsGetDictFromStr else argsGetDictFromStr['multi_dict']
    var_useChatgpt = 'y' if 'use_chatgpt' not in argsGetDictFromStr else argsGetDictFromStr['use_chatgpt']
    
    dict_newsCtgr = ''
    
    if type(dict_newsCtgr) != dict:
        try:
            var_newsCtgr = (var_newsCtgr).replace("{\n    'undefined': {'summary': ''}, 'duration': 'long', 'impact_direction': 'neutral'\n}", "{\n    'undefined': {'summary': '', 'duration': 'long', 'impact_direction': 'neutral'\n}}")
            dict_newsCtgr = ast.literal_eval(var_newsCtgr)
            print('clean python')
        except:
            pass     

    if type(dict_newsCtgr) != dict:
        try:
            var_newsCtgr = (var_newsCtgr).replace("{\n    'undefined': {'summary': ''}, 'duration': 'long', 'impact_direction': 'negative'\n}", "{\n    'undefined': {'summary': '', 'duration': 'long', 'impact_direction': 'negative'\n}}")
            dict_newsCtgr = ast.literal_eval(var_newsCtgr)
            # print('clean python')
        except:
            pass    

    if type(dict_newsCtgr) != dict:
        try:
            var_newsCtgr = (var_newsCtgr).replace("{\n    'undefined': {'summary': ''}, 'duration': 'long', 'impact_direction': 'positive'\n}", "{\n    'undefined': {'summary': '', 'duration': 'long', 'impact_direction': 'positive'\n}}")
            dict_newsCtgr = ast.literal_eval(var_newsCtgr)
            print('clean python')
        except:
            pass    
    try:
        dict_newsCtgr = ast.literal_eval(var_newsCtgr)
    except:
        dict_newsCtgr = ''

    if type(dict_newsCtgr) != dict:
        if (var_multiDict == 'n') or (var_multiDict == 'no'):
            try:
                # print('not multi dict')
                var_newsCtgr = '{' + var_newsCtgr.split('{')[1].split('}')[0] + '}'
                dict_newsCtgr = ast.literal_eval(var_newsCtgr)
            except:
                pass
    
    if type(dict_newsCtgr) != dict:
        try:
            var_newsCtgr = (var_newsCtgr).replace("python\n", '')
            dict_newsCtgr = ast.literal_eval(var_newsCtgr)
        except:
            pass    

    if type(dict_newsCtgr) != dict:
        try:
            var_newsCtgr = (var_newsCtgr).replace("```", '')
            dict_newsCtgr = ast.literal_eval(var_newsCtgr)
        except:
            pass

    if type(dict_newsCtgr) != dict:
        try:
            # var_newsCtgr = (var_newsCtgr).replace("'s ", 's ')
            var_newsCtgr = re.sub(r"(\w)'(\w)", r"\1\2", var_newsCtgr)
            dict_newsCtgr = ast.literal_eval(var_newsCtgr)
        except:        
            pass

    if type(dict_newsCtgr) != dict:
        try:
            # var_newsCtgr = (var_newsCtgr).replace("’s  ", 's ')
            var_newsCtgr = re.sub(r"(\w)’(\w)", r"\1\2", var_newsCtgr)
            dict_newsCtgr = ast.literal_eval(var_newsCtgr)
        except:        
            pass

    if type(dict_newsCtgr) != dict:
        try:
            var_newsCtgr = (var_newsCtgr).replace("s' ", 's ')
            dict_newsCtgr = ast.literal_eval(var_newsCtgr)
        except:        
            pass

    if type(dict_newsCtgr) != dict:
        try:
            var_newsCtgr = (var_newsCtgr).replace("s’  ", 's ')
            dict_newsCtgr = ast.literal_eval(var_newsCtgr)
        except:        
            pass

    if type(dict_newsCtgr) != dict:
        try:
            var_newsCtgr = (var_newsCtgr).replace("<short, medium, long>", "''")
            dict_newsCtgr = ast.literal_eval(var_newsCtgr)
        except:        
            pass

    if type(dict_newsCtgr) != dict:
        try:
            var_newsCtgr = (var_newsCtgr).replace("<positive, negative, neutral>", "''")
            dict_newsCtgr = ast.literal_eval(var_newsCtgr)
        except:        
            pass

    if type(dict_newsCtgr) != dict:
        try:
            var_newsCtgr = (var_newsCtgr).replace("{'summary': ''}, ", "{'summary': '', ")
            dict_newsCtgr = ast.literal_eval(var_newsCtgr)
        except:        
            pass


    if type(dict_newsCtgr) != dict:
        try:
            var_newsCtgr = (var_newsCtgr).replace("\'", '')
            dict_newsCtgr = ast.literal_eval(var_newsCtgr)
        except:
            pass

    if type(dict_newsCtgr) != dict:
        try:
            var_newsCtgr = (var_newsCtgr).replace("\n", '')
            dict_newsCtgr = ast.literal_eval(var_newsCtgr)
        except:
            pass

    if type(dict_newsCtgr) != dict:
        try:
            var_newsCtgr = var_newsCtgr.replace("{", "{'").replace(": ", "': '").replace(", ", "', '").replace("}", "'}")
            dict_newsCtgr = ast.literal_eval(var_newsCtgr)
        except:
            pass



    if (type(dict_newsCtgr) != dict) and ((var_useChatgpt == 'y') or (var_useChatgpt == 'yes')):
        print('use chatgpt for formatting')
        try:
            dict_newsCtgr = chatgpt_checkFotmat(var_newsCtgr)
        except:
            dict_newsCtgr = var_newsCtgr
                                        
    # else:
    #     dict_newsCtgr = var_newsCtgr

    return dict_newsCtgr;
 



def getListFromStr(var_listStr):
    if '[' in var_listStr:
        var_listStr = var_listStr.split('[')[1]

    if ']' in var_listStr:
        var_listStr = var_listStr.split(']')[0]
        
    var_listStr = '[' + var_listStr + ']'
    list_var = ast.literal_eval(var_listStr)
    return list_var;





# dict_newsTypePrompts = {
#     'company_specific':{
#         'prompt':
#             """Equity Investment Decisions: How might the news affect the company's stock value and investor interest, including both active and passive investment strategies? Consider the implications for dividend-focused investments if applicable.
#             Industry and Market Trends: What does this news indicate about broader industry trends, and how might it affect competitors and related sectors? Explore potential opportunities or risks for investors.
#             Investment in Related Commodities or Technologies: If the news involves product launches or technological advancements, assess any impacts on commodity markets or investment in related technologies.
#             Consumer Behavior and Market Demand: Evaluate how new products, services, or management changes might influence consumer behavior and market demand, potentially affecting investments in related sectors.
#             Business Operations and Financial Health: For news on mergers, acquisitions, or earnings, discuss the implications for the company's operational efficiency, market positioning, and financial health. Highlight any potential impacts on local or multinational business investments.
#             Portfolio Diversification and Risk Management: Consider how this news might influence portfolio diversification strategies, especially for investors holding or considering investment in the company or its competitors.
#             Fintech and Financial Services Sector: If applicable, analyze any implications for the fintech sector or financial services, especially if the news involves digital products, payment systems, or financial technologies.
#             Global Economic Indicators: For multinational companies, assess how the news might reflect or influence global economic trends, currency markets, or international trade dynamics.
#             Investor Sentiment and Market Confidence: Gauge the potential impact of the news on investor sentiment and market confidence, particularly in relation to the company’s future growth prospects and the overall investment climate.
#             """, 
#         'description':
#             'related to earnigns, Mergers, news about management changes, product launches' 
#             },

#     'commodities':{
#         'prompt':
#             """Commodity Investment Strategies: How do current trends in commodity prices and forecasts affect investment strategies? Consider the implications for both direct commodity investments and commodity-related stocks or ETFs.
#             Manufacturing and Production Costs: Assess the impact of raw material supply changes and commodity price fluctuations on manufacturing and production costs. What does this mean for businesses in relevant sectors and their stock prices?
#             Inflation and Economic Indicators: Evaluate how changes in commodity prices and supply chain dynamics could influence inflation rates and other key economic indicators. Discuss the potential effects on monetary policy and interest rates.
#             Consumer Prices and Spending: Consider the downstream effects on consumer prices and how this might influence consumer spending behavior, particularly in sectors heavily dependent on specific commodities.
#             Global Trade and Economic Health: Analyze the implications for global trade patterns and overall economic health, especially for economies heavily reliant on exporting or importing certain commodities.
#             Supply Chain Management and Strategy: Reflect on the lessons and strategies that businesses can adopt for managing supply chain risks and ensuring resilience in the face of raw material shortages or disruptions.
#             Sector-Specific Impacts: Identify the sectors most likely to be affected by the discussed commodity price changes and supply chain issues, such as energy, agriculture, or technology, and explore the potential impacts on investments and business operations within these sectors.
#             Portfolio Diversification and Risk Management: Discuss how the information might influence portfolio diversification strategies, especially in terms of hedging against inflation or sector-specific risks.
#             Sustainability and Environmental Considerations: If applicable, assess how sustainability and environmental concerns related to commodity production and supply chains might influence market dynamics and investment decisions.
#             Long-term Trends and Projections: Examine any long-term trends or projections for commodity markets discussed in the article. How might these influence strategic planning for investors and businesses over the long term?
#             """, 
#         'description':
#             'commodity price, raw material supply, supply chain impact of commodities, demand forecast of commodity' 
#             },

#     'central_bank':{
#         'prompt':
#             """Real estate investments, including primary and rental property purchases, by affecting mortgage rates and property values.
#             Equity investments, both passive and active, as well as dividend-focused investments, by influencing stock market trends and corporate earnings potential.
#             Bond investment strategies, considering changes in interest rates and yield prospects.
#             Cryptocurrency and commodity investments, in light of economic sentiment and potential shifts towards alternative assets.
#             Retirement savings and strategies, with a focus on long-term saving rates and investment options' viability.
#             Remortgaging opportunities, including how current or projected interest rates affect refinancing decisions.
#             Strategies to maximize savings account returns, especially in the context of changes to national savings rates.
#             Economic factors influencing emigration decisions, including job prospects and currency strength.
#             The operational and financial outlook for local and multinational businesses, particularly regarding borrowing costs and economic growth forecasts.
#             Forex trading and portfolio diversification strategies, in response to currency fluctuations and global economic health.
#             The impact on fintech innovations and investments, especially those related to financial services and digital currencies.
#             Building capital and investment opportunities in vehicles, taking into account economic trends and consumer confidence.
#             """,
#         'description':
#             'related to interest rates, jobs, employment'    
#             },

#     'economic_performance': {
#         'prompt':
#             """Equity and Bond Investments: How do the reported economic performance metrics influence the outlook for equity and bond markets? Consider implications for both active and passive investment strategies, as well as dividend-focused investments.
#             Real Estate Market: Assess the impact of economic indicators on the real estate market, including potential effects on mortgage rates, property values, and investment opportunities in both primary and rental properties.
#             Commodity and Crypto Investments: Evaluate how changes in economic performance may affect commodity prices and cryptocurrency markets, considering their sensitivity to economic trends and investor sentiment.
#             International Trade and Forex Trading: Discuss the implications for international trade and forex markets, especially in relation to currency strength, trade balances, and cross-border investment flows.
#             Consumer Confidence and Retail Sector: Analyze the potential impact on consumer confidence and the retail sector, including how changes in economic performance might influence consumer spending and business revenues.
#             Industrial Production and Supply Chains: Explore the effects of production levels and economic health on industrial sectors and supply chains, identifying potential risks and opportunities for investors and businesses.
#             Employment and Wage Trends: Consider how economic indicators related to employment and wages might influence consumer savings, spending patterns, and overall economic growth.
#             Inflation and Central Bank Policies: Assess the implications for inflation rates and central bank policy decisions, especially regarding interest rates, which can influence savings and borrowing costs.
#             Retirement Savings and Pensions: Reflect on how economic performance impacts long-term savings strategies and the viability of pension funds, considering the effects of inflation and interest rates.
#             Business Investment and Growth: For local and multinational businesses, examine how economic trends might affect investment decisions, growth opportunities, and strategic planning.
#             Portfolio Diversification: Discuss how economic performance indicators can inform portfolio diversification strategies, helping investors manage risk and capitalize on emerging opportunities.
#             """,
#         'description':
#             'GDP, trade, production'
#             },

#     'biographical': {
#         'prompt':
#             """Leadership and Management Insights: What can be learned from the individual’s or company’s approach to leadership and management? Identify key traits or decisions that contributed to their success or failures, and consider how these insights could benefit current business leaders and managers.
#             Innovation and Product Development: Explore the role of innovation in the subject’s achievements. How did their approach to innovation or product development lead to market success? Assess the implications for businesses seeking to innovate in today’s market.
#             Risk Management and Strategic Decision Making: Examine any notable risks taken or strategic decisions made. What were the outcomes, and what lessons can be drawn about balancing risk and opportunity?
#             Market Trends and Industry Impacts: Analyze how the individual or company responded to or influenced market trends and industry dynamics. What can current investors and businesses learn from their ability to adapt to or shape the market environment?
#             """,
#         'description':
#             'biography of a person or history of a company'
#             },

#     'politics': {
#         'prompt':
#             """International Trade and Market Access: How might the political developments affect international trade flows and market access for goods and services? Consider the implications for both importing and exporting countries, as well as global supply chains.
#             Investment Climate and Market Sentiment: Assess the influence of these political actions on the investment climate and market sentiment, particularly in affected sectors and markets. Discuss the potential for both short-term volatility and long-term shifts in investor confidence.
#             Economic Policies and Growth Forecasts: Evaluate the implications for economic policies in the countries involved and their potential impact on domestic and global economic growth forecasts. How might changes in policy direction influence economic recovery or expansion efforts?
#             Currency Valuations and Forex Markets: Analyze the potential effects on currency valuations and forex market dynamics. How could the news influence investor perceptions of risk and currency strength in the affected countries?
#             Sector-Specific Impacts: Identify the sectors that are likely to be most directly affected by the political developments, such as energy, technology, manufacturing, or agriculture. Explore the potential risks and opportunities for businesses within these sectors.
#             Corporate Strategy and Business Operations: Reflect on how businesses, especially multinational corporations, might need to adjust their strategies and operations in response to the political news. Consider the need for strategic pivots, supply chain diversification, or market repositioning.
#             Regulatory Environment and Compliance: Discuss the impact on the regulatory environment and compliance requirements for businesses operating in or with the affected countries. How might companies navigate increased complexity or restrictions?
#             Portfolio Diversification and Risk Management: Consider how investors might adjust their portfolio diversification and risk management strategies in light of the political developments. What strategies could mitigate exposure to geopolitical risk or capitalize on emerging opportunities?
#             Consumer Behavior and Spending Patterns: If applicable, assess how the political events might influence consumer behavior and spending patterns, particularly in regions directly impacted by the news.
#             Global Economic Stability and Cooperation: Evaluate the broader implications for global economic stability and international cooperation. How might these political developments affect efforts to address global challenges, such as climate change, public health, or economic inequality?
#             """,
#         'description':
#             'news about politics like policy changes, elections, blocking on international mergers, tariffs, trade agreements, sanctions'
#             },

#     'personal_finance': {
#         'prompt':
#             """Retirement Planning: How do the insights or updates affect retirement planning strategies? Consider implications for both early and traditional retirement planning, including the viability of different retirement savings accounts and investment options.
#             Savings Account Options: Evaluate the details provided on various savings account options. How do interest rates, benefits, and restrictions compare to existing options? Discuss the potential impacts on short-term and long-term savings goals.
#             Debt Management: Assess the advice or strategies for managing debt. How can individuals apply these techniques to effectively reduce or manage debt, including credit card debt, student loans, and mortgages?
#             Investment Strategies: If discussed, analyze how the news influences investment strategies for individuals. Consider the impact on portfolio diversification, risk management, and choosing between passive vs. active investing approaches.
#             Financial Products and Services: Explore any new financial products or services mentioned and their potential benefits or risks for personal finance management. How do these products fit into an overall financial planning strategy?
#             Tax Planning: Examine any implications for tax planning, including maximizing deductions, tax-efficient investing, and navigating changes to tax laws or rates.
#             Emergency Funds and Financial Security: Consider advice or information related to building and maintaining emergency funds. How does this align with strategies for enhancing financial security and preparedness?
#             Economic Trends and Personal Finance: Discuss how broader economic trends highlighted in the article, such as inflation or unemployment rates, could affect personal finance strategies and decision-making.
#             Credit Scores and Financial Health: If applicable, analyze the impact of financial decisions or products on credit scores and overall financial health. Include tips for improving or maintaining a healthy credit score.
#             Long-term Financial Goals: Reflect on how the discussed topics influence the planning and achievement of long-term financial goals, such as buying a home, funding education, or building wealth.
#             """,
#         'description':
#             'retirement planning and savings account options and debt management'
#             }
        
# }


# var_listNewsType = ''
# for var_dictKey in dict_newsTypePrompts.keys():
#     var_listNewsType = var_listNewsType + var_dictKey + " <" + dict_newsTypePrompts[var_dictKey]['description'] + ">\n"
# var_listNewsType = var_listNewsType + "other <all other categories>"




# ------------------------------- OLD REGION ALLOCATION CODE -------------------------------

# # using chatgpt to reformat from string to dictionary
# def chatgpt_checkCountry(var_cntr, df_cntrList):
    
#     var_initialMsg = 'you will share a name of a place and i will pick the a name from a list on what that country is called.'

#     var_asst = f"""I will chose from {str(df_cntrList.adjusted_country.unique())[1 : -1]}. If there is no good option, I will share a new name. I will only return the country name with no other text and I will not provide any explanation""" 

#     list_check = chatgpt_initial_message(var_initialMsg)
#     list_check = chatgpt_update_chat(list_check, 'user', var_cntr)
#     list_check = chatgpt_update_chat(list_check, 'assistant', var_asst)
#     var_newsStr = chatgpt_get_response_GPT4(list_check)

#     return var_newsStr;



# def getCountryCleanName(var_ii):

#     df_cntrList = news_connectSQL.downloadSQLQuery('country_name_clean')

#     var_ii = clean_text(var_ii)
#     var_ii = var_ii.rstrip()
#     var_ii = var_ii.lstrip()
#     if var_ii in df_cntrList.country_output.unique():
#         var_iiClean = df_cntrList[df_cntrList.country_output == var_ii]['adjusted_country'].values[0]
#     else:

#         var_iiClean = chatgpt_checkCountry(var_ii, df_cntrList)
#         var_iiClean = clean_text(var_iiClean)
#         var_iiClean = var_iiClean.rstrip()
#         var_iiClean = var_iiClean.lstrip()
#         print('\n\n country not in list! new country category created! ', var_ii, ' - ', var_iiClean, '\n\n')
#         df_var = pd.DataFrame([[var_ii, var_iiClean, datetime.now()]], columns = df_cntrList.columns [ : -1])
#         news_connectSQL.uploadSQLQuery(df_var, 'country_name_clean')

#     return var_iiClean;


# def clean_region(var_rgn):
#     var_outputStr = ''
#     if pd.isna(var_rgn) == False:
#         for var_ii in var_rgn.split(','):
#             if var_ii not in var_outputStr:
#                 var_ii = getCountryCleanName(var_ii)

#                 var_outputStr += var_ii + ', '

#     return var_outputStr;

# ------------------------------- REGION ALLOCATION CODE END -------------------------------
# ------------------------------------------------------------------------------------------