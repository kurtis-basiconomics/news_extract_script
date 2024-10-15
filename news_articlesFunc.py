from news_tools import *


var_yy = 500 # number of urls to assign in articles to check and group, for article grouping in news_articlesSumm

var_chckNewsFr = datetime.today().strftime('%Y-%m-%d')

var_ctgrNewsDaydDelta = 10
var_tgtAudience = var_tgtAudienceMaster
var_nbrBulletPoints = 6
var_newsCtgrToDictFormat = 3
var_summCharacters = 750
var_hdlnCharacters = 150

var_daysInclAtclEnty = 3 # number of days from the headline_date where article is grouped


list_atclCols = ['article_id', 'recommended_headline', 'summary', 'news_source', 'news_type', 'key_people', 'region', 'news_url', 'headline_date', 'source_count', 'created_at', 'updated_at', 'analysis_version', 'article_type', 'additional_info']

argsGetSumm = {'upload_to_sql': '', 'create_new_ctgr': '', 'create_new_summ': '', 'number_of_category_articles': '', 'print_progress': ''}


# prepare list of categories and news_type
df_newsCtgr = news_connectSQL.downloadSQLQuery('news_type_category')
df_newsType = news_connectSQL.downloadSQLQuery('news_type_options')


# VARIABLES FOR KEY_SUBJECT -  to be uploaded to sql
list_titlesToExclPpl = ['president', 'miniser', 'prime minister', 'baroness', 'sir', 'lord', 'senator', 'congressman', 'house of representative', 'baron', 'ceo', 'cheif executive officer', 'chief executive', 'cfo', 'chief of finance', 'finance chief', 'coo', 'chief operating officer', 'director', 'chairman', 'chairman of the board of directors', 'chairperson', 'parter', 'councilor', 'commissioner', 'ambassador', 'mayor', 'consul', 'secretary', 'judge ', 'justice miniter', 'justice']
list_titlesToExclOrg = ['corp', 'corporation', 'limited', 'group', 'ltd', 'partners']
list_titlesToExcl = list(set(list_titlesToExclOrg + list_titlesToExclPpl))
list_exclRgn = ['multiple', 'global', 'international', 'world', 'worldwide']



# ================================ UPLOAD TO article_entity ================================
def upldToAtclEnty(df):
    var_i = 0
    var_iLen = len(df)
    for var_aliasName, var_entyName, var_entyId in zip( df.alias_name, df.entity_name, df.entity_id):
        var_i += 1
        var_counterStr = str(var_i) + ' of ' + str(var_iLen) + ' '
        var_outputStr = var_counterStr + var_aliasName + var_entyName
        try:
            var_outputStr += news_connectSQL.replaceSQLQuery('article_entity', 'alias_name', var_aliasName, ['entity_name', 'entity_id', 'updated_at'], [ var_entyName, var_entyId, datetime.now() ] , upload_to_sql = 'y' )
        except:
            var_outputStr += ' ERROR!! '
        print(var_outputStr)
# ============================== UPLOAD TO article_entity END ==============================
# ==========================================================================================


# ================================ RUN CATEGORY AND SUMMARY ================================

# create the category intro including putting the keys and definitions into a dictionary
dict_newsCtgr = {}
for var_newsCtgr in df_newsCtgr.category.unique():
    var_ctgrDef = df_newsCtgr.loc[df_newsCtgr.category == var_newsCtgr]['definition'].values[0]
    dict_newsCtgr[var_newsCtgr] =  var_ctgrDef
def getCategoryIntro():
    var_ctgrIntro = f"""I will help choose the most appropriate category for the news article you will share based on the list of categories and their definitions. Only return the exact string of the category. choose from the following:\n """
    for var_key in dict_newsCtgr.keys():
        var_ctgrIntro += 'news type: ' + var_key + ' , definition: ' + dict_newsCtgr[var_key].rstrip() + ';\n '
    return var_ctgrIntro;
var_introCtgr = getCategoryIntro()

# create the news_type intro including putting the keys and definitions into a dictionary
dict_newsType = {}
for var_newsCtgr in df_newsType.category.unique():
    dict_typeVar = dict(df_newsType.loc[df_newsType.category == var_newsCtgr][['news_type', 'definition']].values)
    dict_newsType[var_newsCtgr] =  dict_typeVar
def getNewsTypeList(var_ctgr):
    var_newsTypeOptn = ''
    for var_key in dict_newsType[var_ctgr].keys():
        var_newsTypeOptn += 'news type: ' + var_key + ' , definition: ' + dict_newsType[var_ctgr][var_key].rstrip() + ';\n '
    return var_newsTypeOptn;

def getNewsTypeOptions(var_ctgr):
    var_newsTypeText = f"""
        I will assist in extracting key information from a financial news article that is {var_ctgr.replace('_', '')}. The different information I will extract are:
            - news_type (choose the type of subject of the news article from the list): {getNewsTypeList(var_ctgr)}
            - key_paragraph (identify the key paragraph that led to the respective news_type category; return text in the exact format as found in the article)
            - key_region (what country or countries will be impacted by the key subject of the article. focus on the main regions)
            - key_people (key individuals, bill gates, elon musk, president joe biden, prime minister of uk in the news story. only focus on the main individuals. state full name if known and avoid using one name only unless it is from a very popular person )
            - key_organizations (name of the companies, groups of people, or organizations like tesla, governement of canada, microsoft corp, us congress who are the subject of the article. only focus on the main organizations mentioned in the article)
            - recommended_headline (recommend an engaging headline that would attract {var_tgtAudience}; ensure it is direct and suitable for an Instagram-like format and limit to {var_hdlnCharacters} characters)
            {{
            'news_type': <choose from the options above>, 
            'key_paragraph': <key paragraph>, 
            'key_region': <for example united states, united kingdom, africa, middle east>, 
            'key_people': <for example vladimir putin, founder elon musk, former president trump, microsoft ceo satya nadella>, 
            'key_organizations': <for example tesla, federal reserve, opec, european union>, 
            'recommended_headline': <recommended headline>
            }}
        """
    return var_newsTypeText;

def getCvrnHstrList(var_intro, var_newsText,  var_title):
    list_cvrnHstr = [
            {"role": "system", "content": var_intro},
            {"role": "assistant", "content": "Pass the news article you would like to categorize."},
            {"role": "user", "content": var_newsText},
            {"role": "user", "content": 'the title of the news article is ' + var_title},
            {"role": "user", "content": "Only return the exact output requested without any additional text."}
        ]
    return list_cvrnHstr;

    
def chatgpt_getNewsCategories(var_newsText, var_title):

    try:
        var_ok = 0
        var_ii = 0

        while (var_ok == 0) and (var_ii <= 3):
            var_ii += 1

            var_chatgptRspn = chatgpt_get_response_GPT4( getCvrnHstrList(var_introCtgr, var_newsText,  var_title) , api_key = news_connectSQL.var_openai_category ) 
            # print(str(var_chatgptRspn))
            if ':' in var_chatgptRspn: var_chatgptRspn = var_chatgptRspn.split(':')[-1]
            var_chatgptRspn = var_chatgptRspn.rstrip()
            var_chatgptRspn = var_chatgptRspn.lstrip()

            if str(var_chatgptRspn) in dict_newsCtgr.keys():
                var_ctgr = str(var_chatgptRspn)
                var_ok = 1
        if var_ok == 1:
            var_chatgptRspn = chatgpt_get_response_GPT4( getCvrnHstrList(getNewsTypeOptions(var_ctgr), var_newsText,  var_title) , api_key = news_connectSQL.var_openai_category ) 

    except:
        var_chatgptRspn = "{\n  'news_type': '', \n  'key_paragraph': '', \n  'key_region': '', \n  'key_people': '', \n  'key_organizations': '', \n  'recommended_headline': ''\n}"
        var_ctgr = ''

    return var_chatgptRspn, var_ctgr;



def chatgpt_getNewsSummary(var_newsText, var_newsType, var_region, var_keyPeople, var_keyOrg):   

    # try:
    # clean data that will be used for analysis
    if  isinstance(var_newsType, list):
        var_newsType = ', '.join(var_newsType)    
        var_newsType =  str(var_newsType.replace('_', ' '))
    if  isinstance(var_region, list):
        var_region = ', '.join(var_region)    
        var_region =  str(var_region.replace('_', ' '))
    if  isinstance(var_keyPeople, list):
        var_keyPeople = ', '.join(var_keyPeople)    
        var_keyPeople =  str(var_keyPeople.replace('_', ' '))
    if  isinstance(var_keyOrg, list):
        var_keyOrg = ', '.join(var_keyOrg)    
        var_keyOrg =  str(var_keyOrg.replace('_', ' '))

    var_newsSummIntro = f"""I will help summarize a financial news article that will be posted on Instagram. 
        I will emphasize up to {var_nbrBulletPoints} keys points from the article that can tell the story as eye catcy, engaging and informative.
        Specify the who, what and where will be impacted and the reason it is expected to occur.
        The ideal audience are {var_tgtAudience}."""

    var_newsDetails = f"""I will passed an article with a focus on {var_newsType.replace('_', ' ')} type of news""" 
    
    if (var_region is not None and var_region != ''):
        var_newsDetails += f"""is occured in {var_region}"""
    
    if (var_keyPeople is not None and var_keyPeople != '') or (var_keyOrg is not None and var_keyOrg != ''):
        var_newsDetails += ', the news speaks about '
        if var_keyPeople is not None and var_keyPeople != '':
            var_newsDetails += var_keyPeople + ' and '
        if var_keyOrg is not None and var_keyOrg != '':
            var_newsDetails += var_keyOrg

    var_outputDetails = f"""only return the key points in a python list format output without any additional text. ensure that the whole summary has a maximum of {var_summCharacters} characters """

    list_cvrnHstr = [
        {"role": "system", "content": var_newsSummIntro},
        {"role": "assistant", "content": "Pass the news article you would like to categorize."},
        {"role": "user", "content": var_newsText},
        {"role": "user", "content": var_newsDetails},
        {"role": "user", "content": var_outputDetails}
    ]

    var_chatgptRspn = chatgpt_get_response_GPT4(list_cvrnHstr, api_key = news_connectSQL.var_openai_category ) 

    # except:
    #     var_chatgptRspn = '[]'
    
    return var_chatgptRspn;


def cleanArticleSumm(var_summ):
    try:
        list_summ = getListFromStr(var_summ)
        var_outputStr = ''
        for var_i in list_summ:
            var_outputStr += '- ' + clean_text(var_i) + '\n'
        var_outputStr = var_outputStr[ : -1]
    except:
        var_outputStr = var_summ
    return var_outputStr;


def getUrlInfoToSumm(var_url, var_tblName, **argsGetSumm):

    var_src = var_tblName
    if 'news_' not in var_tblName:
        var_tblName = 'news_' + var_tblName
    var_src = var_src.replace('news_', '')

    var_uploadOk = argsGetSumm['upload_to_sql'] if 'upload_to_sql' in argsGetSumm else 'n'
    var_newCtgr = argsGetSumm['create_new_ctgr'] if 'create_new_ctgr' in argsGetSumm else 'n'
    var_newSumm = argsGetSumm['create_new_summ'] if 'create_new_summ' in argsGetSumm else 'n'

    var_strOutput = ''


    df_newsVar = news_connectSQL.downloadSQLQuery(var_tblName, check_col = 'news_url', check_value = var_url)
    var_hdlnDt, var_newsText, var_title = df_newsVar.iloc[0][['headline_date', 'news_text', 'news_title']]

    if df_newsVar.empty == False:

        # Check if url is already in table. add if not
        if news_connectSQL.checkNewsUrlInTbl('news_summary', var_url):
            news_connectSQL.uploadSQLQuery( pd.DataFrame([ [var_url, var_hdlnDt, var_src] ], columns = ['news_url', 'headline_date', 'news_source']) , 'news_summary' )
            var_strOutput += ' ADDED to summary table;  '
        else:
            var_strOutput += ' already to summary table;  '

        df_varDB = news_connectSQL.downloadSQLQuery('news_summary', check_col = 'news_url', check_value = var_url)


        # check if need to make categories
        var_createNewCtgr = var_newCtgr
        if ( df_varDB.loc[df_varDB.news_url == var_url]['news_type'].values[0] == None ) or ( df_varDB.loc[df_varDB.news_url == var_url]['recommended_headline'].values[0] == None ):
            var_createNewCtgr = 'y'

        if (var_createNewCtgr == 'y') or (var_createNewCtgr == 'yes'):
            var_ii = 0
            var_newsCtgrToDictSuccess = 0
            while (var_ii <= var_newsCtgrToDictFormat) and (var_newsCtgrToDictSuccess == 0):
                var_ii += 1 
                var_newsType, var_newsCtgr = chatgpt_getNewsCategories(var_newsText, var_title)
                dict_newsCtgr = getDictFromStr(var_newsType)
                # print(dict_newsCtgr)
                if isinstance(dict_newsCtgr, dict):
                    var_newsCtgrToDictSuccess = 1
                    dict_newsCtgr['category'] = var_newsCtgr
                    # print(var_newsCtgrToDictSuccess, 'success\n\n')
            if (var_newsCtgrToDictSuccess == 1):
                var_strOutput += 'new category created;  '
            else:
                var_strOutput += 'new category failed because not dictinary;  '

        else:
            dict_newsCtgr = df_varDB[['news_type', 'key_paragraph', 'key_region', 'key_people', 'key_organizations', 'recommended_headline', 'category']].iloc[0].to_dict()
            var_strOutput += 'existing category pulled;  '


        # check if need to make summary
        var_createNewSumm = var_newSumm
        if  isinstance(dict_newsCtgr, dict):
            # check if summary already created
            if ( df_varDB.loc[df_varDB.news_url == var_url]['summary'].values[0] == None ) and ( dict_newsCtgr['news_type'] != '' ):
                var_createNewSumm = 'y'

        if (var_createNewSumm == 'y') or (var_createNewSumm == 'yes'): 
            var_newsSumm = chatgpt_getNewsSummary(var_newsText, dict_newsCtgr['news_type'], dict_newsCtgr['key_region'], dict_newsCtgr['key_people'], dict_newsCtgr['key_organizations'])
            list_newsSumm = cleanArticleSumm(var_newsSumm)
            var_strOutput += 'new summary created;  '
        else:
            list_newsSumm = df_varDB['summary'].iloc[0]
            var_strOutput += 'existing summary pulled; '


        # add required new data to download list
        list_colsSQL = list()
        list_varSQL = list()
        try:
            if 'new category created;  ' in var_strOutput:
                for var_key in dict_newsCtgr.keys():
                    if dict_newsCtgr[var_key] != '' and dict_newsCtgr[var_key] is not None:
                        var_keyValue = dict_newsCtgr[var_key]
                        if  isinstance(var_keyValue, list):
                            var_keyValue = ', '.join(var_keyValue)
                        list_colsSQL.append( var_key )
                        list_varSQL.append( var_keyValue )
                list_colsSQL.append( 'category_raw' )
                list_varSQL.append( var_newsType )
        except:
            var_strOutput += ' FAILED ON CREATING UPLOAD LIST CATEGORY;  '
        try:
            if 'new summary created;  ' in var_strOutput:
                if str(list_newsSumm) != '' and list_newsSumm is not None and len(list_newsSumm) > 0:
                    list_colsSQL.append( 'summary' )
                    list_varSQL.append( str(list_newsSumm) )
        except:
            var_strOutput += ' FAILED ON CREATING UPLOAD LIST SUMMARY;  '
        if len(list_varSQL) > 0:
            
            list_colsSQL.extend( ['updated_at', 'analysis_version'] )
            list_varSQL.extend( [datetime.now(), var_analysisVrsn] )


        if (len(list_varSQL) > 0) and ( (var_uploadOk == 'yes') or (var_uploadOk == 'y') ):
            # print(str(list_colsSQL), str(list_varSQL))
            try:
                news_connectSQL.replaceSQLQuery('news_summary', 'news_url', var_url, list_colsSQL, list_varSQL, upload_to_sql = var_uploadOk)
                var_strOutput += str(list_colsSQL) + ' PRINTED'
            except:
                var_strOutput += str(list_colsSQL) + ' print failed'


    else:
        var_strOutput += ' error on extracting news data'
        dict_newsCtgr, list_newsSumm = {}, []
    
    return dict_newsCtgr, list_newsSumm, var_strOutput;


# RUN FUNCTION TO GET CATEGORY AND SUMMARIZE
# input table name
def runCtgrAndSummFunc(var_tblName, **argsGetSumm):

    var_analysisLen = argsGetSumm['number_of_category_articles'] if 'number_of_category_articles' in argsGetSumm else var_nbrCtGrAnsSummAnalysis
    
    list_strExclUrl = news_connectSQL.downloadSQLQuery('news_exclusions', check_value = [var_tblName.replace('news_', ''), 'url'] , check_col = ['news_source', 'exclusion_on'] ).table_string.tolist()
    list_strExclTitle = news_connectSQL.downloadSQLQuery('news_exclusions', check_value = [var_tblName.replace('news_', ''), 'title'] , check_col = ['news_source', 'exclusion_on'] ).table_string.tolist()
    if len(list_strExclUrl) == 0: list_strExclUrl = ['abcdefghijklmn']

    df_newsSumm_news = news_connectSQL.downloadSQLQuery('news_summary', date_col = 'headline_date', date_from = var_chckNewsFr, check_col = 'news_source', check_value = var_tblName.replace('news_', '') )
    df_newsSumm_news = df_newsSumm_news.loc[ 
        (df_newsSumm_news.news_type !=  '') & (~df_newsSumm_news.news_type.isnull() ) &
        (df_newsSumm_news.summary !=  '') & (~df_newsSumm_news.summary.isnull() )
        ]
        
    df_news = news_connectSQL.downloadSQLQuery(var_tblName, date_col = 'headline_date', date_from = var_chckNewsFr ) #(datetime.today() - timedelta(days = var_ctgrNewsDaydDelta) ).strftime('%Y-%m-%d') )
    df_news = df_news.loc[ (~df_news.news_text.isnull() ) & (df_news.news_text != '') & (~df_news.news_title.isnull() ) & (df_news.news_title != '') ]
    df_news = df_news.loc[ ~df_news.news_url.isin( df_newsSumm_news.news_url.unique() ) ]
    df_news = checkifDfOk(df_news, var_tblName)


    df_news.sort_values(by = ['headline_date', 'created_at'] , ascending = False, ignore_index = True, inplace = True)
    # df_news.reset_index(drop = True, inplace = True)

    list_urlDwld = df_news.news_url.unique()[ : var_analysisLen]

    var_i = 0
    var_len = len(list_urlDwld)

    var_srcapeOk = 0

    print('\n\n\n', var_tblName, ' ', {var_len},  ' articles category and summary analysis start \n', 'exclusing:  ', list_strExclUrl, list_strExclTitle, '\n\n')
    
    for var_url in list_urlDwld:

        var_i += 1
        var_counterStr = str(var_i) + ' of ' + str(var_len)
        var_outputStr = var_counterStr + ' ' + var_url + ' '

        var_title = df_news.loc[df_news.news_url == var_url]['news_title'].values[0]

        if chekIfUrlOk(var_url, var_title, list_strExclUrl, list_strExclTitle) == 'y':
            try:
                dict_newsCtgr, list_newsSumm, var_strOutput = getUrlInfoToSumm(var_url, var_tblName, upload_to_sql = 'y')

                if ('new category created' in var_strOutput) and ('new summary created' in var_strOutput):
                    var_strOutput += ' SUCCESS'
                    var_srcapeOk += 1
                var_outputStr +=  var_strOutput    
                # var_outputStr +=  ' SUCCESS'  
            
            except Exception as e:
                var_outputStr += ' with error on getUrlInfoToSumm  '

        else:
            var_outputStr += ' not to analyse'

        print(var_outputStr)

    return var_srcapeOk;

# ============================== RUN CATEGORY AND SUMMARY END ==============================
# ==========================================================================================



# ======================== UPLOAD NEW SPLIT TEXT  TO article_entity ========================

def upldNewAliasFromSplitText(df_atclEnty_new):
    var_outputStr = 'Uploading new alias from article split\n'

    df_alias = news_connectSQL.downloadSQLQuery('alias_table')

    df_atclEnty_new = df_atclEnty_new.merge( df_alias[['alias_name', 'entity_name', 'entity_id']] , on = 'alias_name', how = 'left')

    if df_atclEnty_new.empty == False:
        # Upload alias_name already available
        df_atclEnty_new_upld = df_atclEnty_new.dropna(subset = ['entity_name', 'entity_id'])
        if df_atclEnty_new_upld.empty == False:
            try:
                print('uploading article_entity with available alias_name, uploading ', str(len(df_atclEnty_new_upld)), ' rows' )
                news_connectSQL.uploadSQLQuery(df_atclEnty_new_upld, 'article_entity')
                var_str_1 = f"""successfully uploaded all variables found alias_name {str(len(df_atclEnty_new_upld))} rows"""
            except:
                var_str_1 = 'FAILED ON UPLOADING AVAILABLE found alias_name'
            print(var_str_1)
            var_outputStr += var_str_1 +'\n'
        else:
            var_outputStr += ' no FOUND alias_name\n'
        # Upload alias_name NOT available
        df_atclEnty_new_new = df_atclEnty_new.loc[pd.isna( df_atclEnty_new.entity_name ) ][[ 'alias_name', 'entity_type' , 'news_url' ]]
        if df_atclEnty_new_new.empty == False:
            try:
                print('uploading article_entity with available alias_name, uploading ', str(len(df_atclEnty_new_new)), ' rows' )
                news_connectSQL.uploadSQLQuery(df_atclEnty_new_new, 'article_entity')

                var_str_2 = f"""successfully uploaded all variables MISSING alias_name {str(len(df_atclEnty_new_new))} rows"""
            except:
                var_str_2 = 'FAILED ON UPLOADING AVAILABLE MISSING alias_name'
            print(var_str_2)
            var_outputStr += var_str_2 +'\n'
        else:
            var_outputStr += ' no MISSING alias_name\n'

    return var_outputStr;
# ====================== UPLOAD NEW SPLIT TEXT  TO article_entity END ======================
# ==========================================================================================



# ================================== SPLIT NEWS TEXT FUNC ==================================

def runCtgrSplitText():
    df_newsSumm = news_connectSQL.downloadSQLQuery('news_summary', date_col = 'created_at', date_from = var_dtFrAnalysis)
    df_atclEnty = news_connectSQL.downloadSQLQuery('article_entity')

    df_newsSumm = df_newsSumm.loc[
        (~df_newsSumm.news_url.isin( df_atclEnty.news_url.unique() ) ) &
        (pd.isna(df_newsSumm.news_type) == False)
        ]
    
    list_urlDwld = df_newsSumm.news_url.unique()

    var_len = len(list_urlDwld)
    var_i = 0
    df_atclEnty_new = pd.DataFrame(columns = ['alias_name', 'entity_type', 'news_url'])

    var_outputErr = 'The following errors occured while splitting text:\n'
    var_iErr = 0


    for var_url in list_urlDwld:
        

        var_i += 1
        var_counterStr = f"""{str(var_i)} of {str(var_len)}"""

        var_outputStr = var_counterStr + ' ' + var_url + ' '


        var_keyPpl, var_rgn, var_keyOrg = df_newsSumm.loc[df_newsSumm.news_url == var_url][list_colsToEnty].values[0]
        try:
            for var_keyCol, var_entyType in zip( [var_keyPpl, var_rgn, var_keyOrg], list_entyType ):
                
                if (var_keyCol != '') and (var_keyCol != None):
                    for var_alias in var_keyCol.split(', '):

                        if (var_alias != '') and (var_alias != None) and (var_alias != 'not applicable') and (var_alias != 'not specified'):

                            var_outputStr += ' added:  ' + var_alias + var_entyType
                            try:
                                var_alias = clean_text(var_alias)
                                var_alias = var_alias.replace(',', '')
                                var_alias = var_alias.rstrip()
                                var_alias = var_alias.lstrip()
                                if (var_alias != '') and (var_alias != 'none') and (var_alias is not None):
                                    
                                    df_var = pd.DataFrame(
                                        [[var_alias, var_entyType, var_url]],
                                        columns = df_atclEnty_new.columns
                                        )
                                    
                                    df_atclEnty_new = pd.concat([df_atclEnty_new, df_var], ignore_index = True)
                                    var_outputStr += ' success  '
                                    
                            except:
                                var_outputStr += ' with ERROR  '
                        else:
                            var_outputStr += var_alias + ' not to include  '
        except:
            var_outputStr += ' FAILED  '

        if (' with ERROR  ' in var_outputStr) or (' FAILED  ' in var_outputStr):
            var_outputErr += var_outputStr + '\n'
            var_iErr += 1

        print(var_outputStr)

    # for email of errors
    var_outputStrSumm = f"""Run of Split Text Success with {str(var_i)} categories split on {datetime.now().strftime('%Y-%m-%d')}"""
    if var_iErr > 0:
        var_outputStrSumm += f""" with {str(var_iErr)} errors\n\n"""
        var_outputStrSumm += var_outputErr

    var_outputStrSumm += upldNewAliasFromSplitText(df_atclEnty_new)

    return var_outputStrSumm;
    

# ================================ SPLIT NEWS TEXT FUNC END ================================
# ==========================================================================================





# ===================================== tag key_subject ====================================

# get list of URL without key_subject
def getListUrlNoKeySubject():
    list_url = list() 
    try:
        var_sqlText = f"""
                select distinct news_url 
                from basiconomics_news_schema.article_entity 
                where key_subject is null
                group by 1;
            """
        list_url = news_connectSQL.getRawSQLQuery(var_sqlText).news_url.tolist()
    except:
        pass
    return list_url;


def removeTitlesFrStr(var_str):
    for word in list_titlesToExcl:
        var_str = var_str.replace(word, "")
    var_str = var_str.strip()
    return var_str;


# Function to check for fuzzy matches
def get_fuzzy_matches(names_list, var_title, threshold=80):
    list_matches = []
    for var_name in names_list:
        if fuzz.partial_ratio(removeTitlesFrStr(var_name).lower(), var_title.lower()) >= threshold:
            list_matches.append(var_name)
    return list_matches


def findNameInTitle(var_title, list_name):
    list_frTitle = list()
    if (var_title != '') and( pd.isna(var_title) == False) and (var_title is not None ):   
        list_frTitle = [var_name for var_name in list_name if removeTitlesFrStr(var_name).lower() in var_title.lower()]
        if len(list_frTitle) == 0:
            list_frTitle = get_fuzzy_matches(list_name, var_title, threshold=80)
    return list_frTitle;


def getListOfAlias(var_url):
    list_aliasName = list() 
    try:
        var_sqlText = f"""
                select distinct alias_name
                from basiconomics_news_schema.article_entity
                where entity_id in (select distinct entity_id
                        from basiconomics_news_schema.article_entity
                        where news_url = '{var_url}') ;
            """
        list_aliasName = news_connectSQL.getRawSQLQuery(var_sqlText).alias_name.tolist()
    except:
        pass
    return list_aliasName;

def getListOfEntyId(list_keySbj):
    list_entyId = list() 
    try:
        var_sqlText = f"""
                select distinct entity_id
                from basiconomics_news_schema.article_entity
                where alias_name in {news_connectSQL.sqlPythonListToSql(list_keySbj)} ;
            """
        list_entyId = news_connectSQL.getRawSQLQuery(var_sqlText).entity_id.tolist()
    except:
        pass
    return list_entyId;

def getKeySubject(var_url):

    df_newsSumm_var = news_connectSQL.downloadSQLQuery('news_summary', check_col = 'news_url', check_value = var_url)

    list_entyId = list()
    if len(df_newsSumm_var) > 0:
        var_newsSrc, var_rcmdHdln = df_newsSumm_var.iloc[0][['news_source', 'recommended_headline' ]]
        var_tblName = 'news_' + var_newsSrc

        df_atclEnty_var = news_connectSQL.downloadSQLQuery('article_entity', check_col = 'news_url', check_value = var_url)

        df_news_var = news_connectSQL.downloadSQLQuery(var_tblName, check_col = 'news_url', check_value = var_url)
        var_title = df_news_var.iloc[0]['news_title']

        list_name = getListOfAlias(var_url)

        # Find which names are mentioned in the title
        list_frTitle = findNameInTitle(var_title, list_name)
        list_frRcmdHdln = findNameInTitle(var_rcmdHdln, list_name)

        if (len(list_frTitle) > 0) and (len(list_frRcmdHdln) > 0):
            list_keySbj = list(set(list_frTitle + list_frRcmdHdln))
        elif (len(list_frTitle) > 0) and (len(list_frRcmdHdln) == 0):
            list_keySbj = list_frTitle
        elif (len(list_frTitle) == 0) and (len(list_frRcmdHdln) > 0):
            list_keySbj = list_frRcmdHdln
        else: 
            list_keySbj = list()

        if len(list_keySbj) > 0:
            list_keySbj = set(list_keySbj) - set(list_exclRgn)
            list_entyId = getListOfEntyId(list_keySbj)

    return list_entyId;
# =================================== tag key_subject END ==================================
# ==========================================================================================


# ==================================== article grouping ====================================

def getUrlForAtcl(**argsAtclBuild):

    var_dtldAnalysis = argsAtclBuild['detailed_analysis'] if 'detailed_analysis' in argsAtclBuild else 'n'
    var_urlCnt = argsAtclBuild['url_count'] if 'url_count' in argsAtclBuild else 2
    var_urlCnt = str(var_urlCnt)

    var_dtldAnlsVal = 0
    if (var_dtldAnalysis == 'y') or (var_dtldAnalysis == 'yes'):
        var_dtldAnlsVal = 1

    var_sqlTextFindNullAtcl = f"""
            select distinct ae.news_url, ns.headline_date, ns.category, ns.news_type
            from basiconomics_news_schema.article_entity ae
            left join basiconomics_news_schema.news_summary ns on ns.news_url = ae.news_url
            where ae.article_id is null
            and ns.category in ( select category from basiconomics_news_schema.news_type_category where detailed_analysis = {var_dtldAnlsVal} )
            group by 1, 2, 3, 4
            order by ns.headline_date desc
            limit {var_urlCnt};
        """
    df_urlToAnalyse = news_connectSQL.getRawSQLQuery( var_sqlTextFindNullAtcl )    
    
    return df_urlToAnalyse;


def getSmlrAtclInfo(var_newsType, var_hdlnDt, list_entyNameRgn, **argsAtclBuild):

    # 0 for getting days before headline date, 1 for days after the headline. 1 is for checking available article_id for th for similar news
    var_dtType = argsAtclBuild['after_headline_date'] if 'after_headline_date' in argsAtclBuild else 'y'
    # 0 for not detailed, 1 for detailed and match key_subject
    var_dtldAnalysis = argsAtclBuild['detailed_analysis'] if 'detailed_analysis' in argsAtclBuild else 'y'


    if (var_dtType == 'y') or (var_dtType == 'yes'):
        var_dt_1 = (var_hdlnDt).strftime('%Y-%m-%d')
        var_dt_2 = (var_hdlnDt + timedelta(days=var_daysInclAtclEnty)).strftime('%Y-%m-%d')
        var_valRtn = 'article_id'
        var_atclFltr = 'NOT NULL'
    else:
        var_dt_1 = (var_hdlnDt - timedelta(days=var_daysInclAtclEnty)).strftime('%Y-%m-%d')
        var_dt_2 = (var_hdlnDt).strftime('%Y-%m-%d')
        var_valRtn = 'news_url'
        var_atclFltr = 'NULL' 


    # filter for detailed analysis to only include key_subject
    if (var_dtldAnalysis == 'y') or (var_dtldAnalysis == 'yes'):
        list_keySbjId = argsAtclBuild['key_subject_list']
        
        var_entyIdFltr = f"""
                AND ae.news_url in (select ae.news_url
                    from basiconomics_news_schema.article_entity ae
                    LEFT JOIN basiconomics_news_schema.news_summary ns ON ns.news_url = ae.news_url      
                    where ns.news_type = '{var_newsType}'
                        AND ns.headline_date >= '{var_dt_1}'
                        AND ns.headline_date <= '{var_dt_2}'
                        and ae.key_subject = 1  
                        and ae.entity_id in { news_connectSQL.sqlPythonListToSql(list_keySbjId, use_quotes = 'n') } )
            """
    else:
        var_entyIdFltr = ''


    var_sqlText = f"""
            SELECT ae.news_url, ae.article_id
            FROM basiconomics_news_schema.article_entity ae
            LEFT JOIN basiconomics_news_schema.news_summary ns ON ns.news_url = ae.news_url
            WHERE
                ns.news_type = '{var_newsType}'
                AND ae.entity_type = 'region'
                AND ae.entity_name IN { news_connectSQL.sqlPythonListToSql(list_entyNameRgn) }
                AND ns.headline_date >= '{var_dt_1}'
                AND ns.headline_date <= '{var_dt_2}'
                AND ae.article_id IS {var_atclFltr}
                {var_entyIdFltr}
            GROUP BY ae.news_url, ae.article_id
            HAVING COUNT(DISTINCT ae.entity_name) = {str(len(list_entyNameRgn))}
                AND COUNT(DISTINCT ae.entity_name) = 
                    (SELECT COUNT(DISTINCT ae2.entity_name)
                    FROM basiconomics_news_schema.article_entity ae2
                    WHERE ae2.news_url = ae.news_url
                    AND ae2.entity_type = 'region');
        """

    df_var = news_connectSQL.getRawSQLQuery( var_sqlText )
    list_var = df_var[var_valRtn].tolist()
    
    return list_var;


def upldAtclIdPipeline(var_atclId, **argsAtclBuild):
    var_addtnInfo = argsAtclBuild['additional_info'] if 'additional_info' in argsAtclBuild else 'new'

    if not isinstance( var_atclId, list):
        list_atclId = [var_atclId]
    else:
        list_atclId = var_atclId
    
    # try:
    df_var = pd.DataFrame( list_atclId, columns = ['article_id'])
    df_var[['article_status', 'additional_info', 'updated_at']] = 0, var_addtnInfo, datetime.now()
    
    news_connectSQL.uploadSQLQuery(df_var , 'article_pipeline')
    var_outputStr = str(list_atclId) + ' article_id uploaded to article_pipeline'
    # except:
    #     var_outputStr = str(list_atclId) + ' article_id uploaded to article_pipeline FAILED'
    return var_outputStr;



def getAtclIdListFromUrl(list_urlDwld):

    var_listForSql = news_connectSQL.sqlPythonListToSql(list_urlDwld)
    var_sqlTextGetAtclIdChg = f"""
            select distinct ae.article_id
            from basiconomics_news_schema.article_entity ae
            where ae.article_id is not null 
            and ae.news_url in {var_listForSql}
        """

    df_atclId = news_connectSQL.getRawSQLQuery( var_sqlTextGetAtclIdChg )
    list_atclId = list()
    if df_atclId.empty == False:
        df_atclId['article_id'] = df_atclId['article_id'].astype(float)
        list_atclId = df_atclId['article_id'].tolist()

    return list_atclId;

# ================================== article grouping END ==================================
# ==========================================================================================




# =================================== CREATE ARTICLE SUMM ==================================

def getSummConsolidationTextForArticle(list_urlAtclSumm):

    listAtcl_keyRgn = list()
    listAtcl_keyPpl = list()
    listAtcl_keyOrg = list()

    var_sqlText = f"""
            select ns.news_url, ns.news_type, ns.recommended_headline, ns.summary, ns.news_source
            from basiconomics_news_schema.news_summary ns
            where ns.news_url in (select distinct ae.news_url 
                from basiconomics_news_schema.article_entity ae
                where news_url in { news_connectSQL.sqlPythonListToSql(list_urlAtclSumm) })
        """
    df_newsSumm_var = news_connectSQL.getRawSQLQuery(var_sqlText)

    var_sqlText = f"""
            select ae.*
            from basiconomics_news_schema.article_entity ae
            where ae.news_url in (select distinct ae.news_url 
                from basiconomics_news_schema.article_entity ae
                where news_url in { news_connectSQL.sqlPythonListToSql(list_urlAtclSumm) } )
        """
    df_atclEnty_var = news_connectSQL.getRawSQLQuery(var_sqlText)

    var_textConsolidated = ''
    for var_urlSumm in list_urlAtclSumm:

        var_rcmdHdln, var_summ = df_newsSumm_var.loc[df_newsSumm_var.news_url == var_urlSumm].iloc[0][['recommended_headline', 'summary']]

        list_keyPpl = df_atclEnty_var.loc[(~pd.isna(df_atclEnty_var.entity_name)) & (df_atclEnty_var.news_url == var_urlSumm) & (df_atclEnty_var.entity_type == 'person')].entity_name.unique()
        list_keyOrg = df_atclEnty_var.loc[(~pd.isna(df_atclEnty_var.entity_name)) & (df_atclEnty_var.news_url == var_urlSumm) & (df_atclEnty_var.entity_type == 'company')].entity_name.unique()
        list_keyRgn = df_atclEnty_var.loc[(~pd.isna(df_atclEnty_var.entity_name)) & (df_atclEnty_var.news_url == var_urlSumm) & (df_atclEnty_var.entity_type == 'region')].entity_name.unique()
        list_keySrc = df_newsSumm_var.news_source.unique()

        var_textConsolidated += f"""the headline {var_rcmdHdln} with a summary {var_summ}, """
        if len(list_keyRgn) > 0:
            var_textConsolidated += f"""the key regions are { ', '.join(list_keyRgn) }, """
            listAtcl_keyRgn.extend( list( set(list_keyRgn) - set(listAtcl_keyRgn)) )
        if len(list_keyPpl) > 0:
            var_textConsolidated += f"""the key indivuals are { ', '.join(list_keyPpl) }, """
            listAtcl_keyPpl.extend( list( set(list_keyPpl) - set(listAtcl_keyPpl)) )
        if len(list_keyOrg) > 0:
            var_textConsolidated += f"""the key organizations are { ', '.join(list_keyOrg) }, """
            listAtcl_keyOrg.extend( list( set(list_keyOrg) - set(listAtcl_keyOrg)) )

        var_textConsolidated += '; '

    list_newsType = df_newsSumm_var.news_type.unique()

    return var_textConsolidated, listAtcl_keyRgn, listAtcl_keyPpl, listAtcl_keyOrg, list_newsType, list_keySrc;




argSmlrAtcl = {'current_summary': '', 'current_headline': '', 'current_key_region': '', 'current_key_people': '', 'current_key_organizations': ''}
def getSummOfSmlrAtcl(list_urlAtclSumm, **argSmlrAtcl):

    var_textConsolidated, list_keyRgn, list_keyPpl, list_keyOrg, list_newsType, list_keySrc = getSummConsolidationTextForArticle(list_urlAtclSumm)    

    var_initialMsg = f"""I will help in create a summary of financial news from a list of summaries from news articles about {', '.join(list_newsType).replace('_', ' ')[ : -1]}. """
    if 'current_summary' in argSmlrAtcl:
        var_initialMsg += f""" I will adjust the current summary based on new information you will share. """
        if 'current_key_region' in argSmlrAtcl:
            var_initialMsg += f"""the subject of the current summary is occuring in {', '.join( argSmlrAtcl['current_key_region'] )}. """
            listAtcl_keyRgn = argSmlrAtcl['current_key_region']  
        if 'current_key_people' in argSmlrAtcl:
            var_initialMsg += f"""the main individuals subject of the current summary are {', '.join( argSmlrAtcl['current_key_people'] )}. """
            listAtcl_keyPpl = argSmlrAtcl['current_key_people']            
        if 'current_key_organizations' in argSmlrAtcl:
            var_initialMsg += f"""the main companies subject of the current summary are {', '.join( argSmlrAtcl['current_key_organizations'] )}. """
            listAtcl_keyOrg = argSmlrAtcl['current_key_organizations']            
        var_initialMsg +=  f"""the current headline for the summary is {argSmlrAtcl['current_headline']}, current summary is {argSmlrAtcl['current_summary']} """
    else:
        listAtcl_keyRgn = list()
        listAtcl_keyPpl = list()
        listAtcl_keyOrg = list()

    listAtcl_keyRgn.extend( list( set(list_keyRgn) - set(listAtcl_keyRgn)) )
    listAtcl_keyPpl.extend( list( set(list_keyPpl) - set(listAtcl_keyPpl)) )
    listAtcl_keyOrg.extend( list( set(list_keyOrg) - set(listAtcl_keyOrg)) )

    var_asst_2 = f"""share the table of news articles and their key information and I can consolidate the summaries and return a maximum of {var_nbrBulletPoints} bullet points, with a maximum of {var_summCharacters} characters.  """ 
    var_user_2 = """the output will be in this format. ['some summary information here', 'more summary text here', 'more summary text here']. do not include other text not related to the output""" # format of the output
    var_user_3 = f"""from news articles information shared, create a summary with {var_nbrBulletPoints} bullet points, and a maximum of {var_summCharacters} characters.  """

    # then ask for recommended headline

    list_cvrnHstr = [
            {"role": "system", "content": var_initialMsg },
            {"role": "assistant", "content": var_asst_2 },
            {"role": "user", "content": var_textConsolidated },
            {"role": "user", "content": var_user_2},
            {"role": "user", "content": var_user_3}
        ]

    var_newsSummList = chatgpt_get_response_GPT4(list_cvrnHstr, api_key = open(news_variables.path_openaiKey_news_articleId, 'r').read().strip('\n') )

    var_asst_3 = f"""here is the summary for the articles: {str(var_newsSummList)}"""
    var_user_4 = f"""based on the information I provided and the summary you provided, give me a good headline for the summary targeting {var_tgtAudience} and limit to {var_hdlnCharacters} characters. just return the text for the headline"""

    list_cvrnHstr.append( {"role": "assistant", "content": var_asst_3} )
    list_cvrnHstr.append( {"role": "user", "content": var_user_4} )

    var_rcmdHdln = chatgpt_get_response_GPT4(list_cvrnHstr, api_key = open(news_variables.path_openaiKey_news_articleId, 'r').read().strip('\n') )

    # print(list_cvrnHstr)

    return var_newsSummList, var_rcmdHdln, listAtcl_keyRgn, listAtcl_keyPpl, listAtcl_keyOrg, list_newsType, list_keySrc;



def getMaxHdlnDt(list_urlAtclSumm):
    var_sqlText = f"""
            select max(headline_date)
            from basiconomics_news_schema.news_summary ns
            where ns.news_url in (select distinct ae.news_url 
                from basiconomics_news_schema.article_entity ae
                where news_url in { news_connectSQL.sqlPythonListToSql(list_urlAtclSumm) }  )
        """
    var_hdlnDt = news_connectSQL.getRawSQLQuery(var_sqlText).values[0]

    return var_hdlnDt;





def createAtclSumm(var_atclId):
    try:
        list_urlAtclSumm = news_connectSQL.downloadSQLQuery('article_entity', check_col = 'article_id', check_value = var_atclId)['news_url'].unique()
        # print(list_urlAtclSumm)
        if len(list_urlAtclSumm) > 0:
            if len(list_urlAtclSumm) > 1:
                # print(str(len(list_urlAtclSumm)), 'use gpt to merge articles')
                var_summ, var_rcmdHdln, list_keyRgn, list_keyPpl, list_keyOrg, list_newsType, list_newsSrc = getSummOfSmlrAtcl(list_urlAtclSumm)

            elif len(list_urlAtclSumm) == 1:
                # print(list_urlAtclSumm, 'NO gpt to merge articles')
                var_url = list_urlAtclSumm[0]
                df_newsSumm_var = news_connectSQL.downloadSQLQuery('news_summary', check_col = 'news_url', check_value = var_url)
                df_atclEnty_var = news_connectSQL.downloadSQLQuery('article_entity', check_col = 'news_url', check_value = var_url)

                var_rcmdHdln, var_summ = df_newsSumm_var.iloc[0][['recommended_headline', 'summary']]

                list_keyPpl = df_atclEnty_var.loc[(~pd.isna(df_atclEnty_var.entity_name)) & (df_atclEnty_var.entity_type == 'person')].entity_name.unique()
                list_keyOrg = df_atclEnty_var.loc[(~pd.isna(df_atclEnty_var.entity_name)) & (df_atclEnty_var.entity_type == 'company')].entity_name.unique()
                list_keyRgn = df_atclEnty_var.loc[(~pd.isna(df_atclEnty_var.entity_name)) & (df_atclEnty_var.entity_type == 'region')].entity_name.unique()
                list_newsType = df_newsSumm_var.news_type.unique()
                list_newsSrc = df_newsSumm_var.news_source.unique()    

            var_hdlnDt = getMaxHdlnDt(list_urlAtclSumm)
            var_summ = cleanArticleSumm(var_summ)

            list_valUpld = [var_atclId, 'article_summ', 1, var_rcmdHdln, var_summ, list_newsType[0], len(list_newsSrc), datetime.now(), pd.to_datetime(var_hdlnDt).date[0] ]
            list_colUpld = ['article_id', 'article_type', 'analysis_version', 'recommended_headline', 'summary', 'news_type', 'source_count', 'created_at', 'headline_date' ]

            for var_yy, var_col in zip([list_newsSrc, list_keyPpl, list_keyRgn, list_keyOrg, list_urlAtclSumm] , ['news_source', 'key_people', 'region', 'key_organizations', 'news_url']):
                if len(var_yy) > 0 :
                    var_yy = ', '.join(var_yy)

                    list_valUpld.append(var_yy)
                    list_colUpld.append(var_col)

            df_var = pd.DataFrame([list_valUpld], columns = list_colUpld)

            news_connectSQL.uploadSQLQuery(df_var, 'news_articles')

            var_outputStr = f"""article {str(var_atclId)} with headline {str(var_rcmdHdln)[ : 50]} successfully uploaded """

            news_connectSQL.replaceSQLQuery('article_pipeline', ['article_id', 'article_status'], [var_atclId, 0], ['article_status', 'updated_at'], [ 1, datetime.now()], upload_to_sql = 'y') 

        else:
            var_outputStr = f"""article {str(var_atclId)} no URLs, closing article_id"""
            news_connectSQL.replaceSQLQuery('article_pipeline', ['article_id', 'article_status'], [var_atclId, 0], ['article_status', 'updated_at'], [ 2, datetime.now()], upload_to_sql = 'y') 

    except:
        var_outputStr = f"""article {str(var_atclId)} upload FAILED"""

    return var_outputStr;


# ================================= CREATE ARTICLE SUMM END ================================
# ==========================================================================================

