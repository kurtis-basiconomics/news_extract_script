from news_tools import *


var_analysisVrsn = 1.1

var_ctgrNewsDaydDelta = 10
var_tgtAudience = var_tgtAudienceMaster
var_nbrBulletPoints = 6
var_newsCtgrToDictFormat = 3
var_summCharacters = 750
var_hdlnCharacters = 150


argsGetSumm = {'upload_to_sql': '', 'create_new_ctgr': '', 'create_new_summ': '', 'number_of_category_articles': ''}


# prepare list of categories and news_type
df_newsCtgr = news_connectSQL.downloadSQLQuery('news_type_category')
df_newsType = news_connectSQL.downloadSQLQuery('news_type_options')



# ================================ UPLOAD TO article_entity ================================
# insert df that will be uploaded to article_entity and replace na
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

    df_newsSumm_news = news_connectSQL.downloadSQLQuery('news_summary', date_col = 'headline_date', date_from = var_dtFrAnalysis, check_col = 'news_source', check_value = var_tblName.replace('news_', '') )
    df_newsSumm_news = df_newsSumm_news.loc[ 
        (df_newsSumm_news.news_type !=  '') & (~df_newsSumm_news.news_type.isnull() ) &
        (df_newsSumm_news.summary !=  '') & (~df_newsSumm_news.summary.isnull() )
        ]
        
    df_news = news_connectSQL.downloadSQLQuery(var_tblName, date_col = 'headline_date', date_from = var_dtFrAnalysis ) #(datetime.today() - timedelta(days = var_ctgrNewsDaydDelta) ).strftime('%Y-%m-%d') )
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
# ==========================================================================================
def upldNewAliasFromSplitText(df_atclEnty_new):
    df_alias = news_connectSQL.downloadSQLQuery('alias_table')

    df_atclEnty_new = df_atclEnty_new.merge( df_alias[['alias_name', 'entity_name', 'entity_id']] , on = 'alias_name', how = 'left')

    if df_atclEnty_new.empty == False:
        # Upload alias_name already available
        df_atclEnty_new_upld = df_atclEnty_new.dropna(subset = ['entity_name', 'entity_id'])
        if df_atclEnty_new_upld.empty == False:
            try:
                print('uploading article_entity with available alias_name, uploading ', str(len(df_atclEnty_new_upld)), ' rows' )
                news_connectSQL.uploadSQLQuery(df_atclEnty_new_upld, 'article_entity')
                print('successfully uploaded all variables ', str(len(df_atclEnty_new_upld)), ' rows' )
            except:
                print('FAILED ON UPLOADING AVAILABLE alias_name ', '\n\n')

        # Upload alias_name NOT available
        df_atclEnty_new_new = df_atclEnty_new.loc[pd.isna( df_atclEnty_new.entity_name ) ][[ 'alias_name', 'entity_type' , 'news_url' ]]
        if df_atclEnty_new_new.empty == False:
            try:
                print('uploading article_entity with available alias_name, uploading ', str(len(df_atclEnty_new_new)), ' rows' )
                news_connectSQL.uploadSQLQuery(df_atclEnty_new_new, 'article_entity')
                print('successfully uploaded all variables ', str(len(df_atclEnty_new_new)), ' rows' )
            except:
                print('FAILED ON UPLOADING AVAILABLE new alias_name ', '\n\n')  
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

    # send email of errors
    var_title = f"""Run of Split Text Success with {str(var_i)} categories split on {datetime.now().strftime('%Y-%m-%d')}"""
    var_body = var_title
    if var_iErr > 0:
        var_body += f""" with {str(var_iErr)} errors\n\n"""
        var_body += var_outputErr
        sendEmail(var_receiverEmail, var_title, var_body )

    upldNewAliasFromSplitText(df_atclEnty_new)
    

# ================================ SPLIT NEWS TEXT FUNC END ================================
# ==========================================================================================