from news_tools import *

import news_articlesFunc


var_titleEmail = f"""Report on Article Summary on {datetime.now().strftime('%Y-%m-%d')}"""

var_upldOk = 'y'
var_printUpdt = 'n'


print('\n\n')
var_outputStr = 'article summary and analysis below:\n'
var_outputStr += f"""****** article summary and analysis start on {datetime.now().strftime('%Y-%m-%d %H:%M')}******\n"""


# =========================== get key_subject ===========================
var_outputStr += f"""****** key_subject start on {datetime.now().strftime('%Y-%m-%d %H:%M')}******\n"""
try:
    list_urlDwld = news_articlesFunc.getListUrlNoKeySubject()

    var_len = len(list_urlDwld)
    var_i = 0
    var_iKeySbjtFound = 0
    var_iKeySbjtMiss = 0
    var_iKeySbjtErr = 0

    print(str(len(list_urlDwld)), ' articles to to assign key_subject' )

    for var_url in list_urlDwld:
        var_i += 1
        var_counterStr = f"""{str(var_i)} of {str(var_len)} """

        var_str_1 = var_counterStr + ' ' + var_url + ' '

        df_atclEnty_var = news_connectSQL.downloadSQLQuery('article_entity', check_col = ['news_url'], check_value = [var_url])
        df_atclEnty_var = df_atclEnty_var.loc[pd.isna(df_atclEnty_var.key_subject)]

        if df_atclEnty_var.empty == False:

            news_connectSQL.replaceSQLQuery('article_entity', ['news_url'], [var_url], ['key_subject', 'updated_at'] , [0, datetime.now()], upload_to_sql = 'y')

            try:
                list_entyId = news_articlesFunc.getKeySubject(var_url)
                if len(list_entyId)> 0:
                    var_str_1_a = news_connectSQL.replaceSQLQuery('article_entity', ['news_url', 'entity_id'], [var_url, list_entyId], ['updated_at', 'key_subject'], [datetime.now(), 1], use_quotes = 'n', upload_to_sql = 'y')
                    var_str_1_a = var_str_1 + var_str_1_a
                    print(var_str_1_a)
                    var_iKeySbjtFound += 1
                else:
                    var_str_1 += ' no aticle_id for key subject!! '
                    var_iKeySbjtMiss += 1
                    print(var_str_1)            

            except:
                var_str_1 += ' with ERROR!! '
                var_iKeySbjtErr += 1
                print('\n', var_str_1, '\n\n')

            # # checkpoint
            # if str(var_i)[-3: ] == '000':
            #     # to delete checkpoint
            #     var_title_1 = f"""Report on Article Summary on {datetime.now().strftime('%Y-%m-%d')} with {str(var_i)} urls proccessed checkpoint"""
            #     var_outputStr_1 = var_outputStr + f"""articles with key_subject assigned: {str(var_iKeySbjtFound)}\narticles with NO key_subject {str(var_iKeySbjtMiss)}\narticles with key_subject ERROR {str(var_iKeySbjtErr)}\n"""
            #     sendEmail(var_receiverEmail, var_title_1, var_outputStr_1 )

        else:
            var_str_1 += ' url key subject already assigned!! '

    var_outputStr += f"""articles with key_subject assigned: {str(var_iKeySbjtFound)}\narticles with NO key_subject {str(var_iKeySbjtMiss)}\narticles with key_subject ERROR {str(var_iKeySbjtErr)}\n"""
except:
    var_outputStr += f"""articles with key_subject assigned: {str(var_iKeySbjtFound)}\narticles with NO key_subject {str(var_iKeySbjtMiss)}\narticles with key_subject ERROR {str(var_iKeySbjtErr)}\n"""
    var_outputStr += 'FAILED\n'
var_outputStr += f"""\n****** key_subject END on {datetime.now().strftime('%Y-%m-%d %H:%M')}******\n\n"""

# ========================= get key_subject END =========================
# =======================================================================



# # send checkpoint email
# var_titleEmail_1 = 'checkpoint 1 ' + var_titleEmail
# var_outputStr_1 = '************************** checkpoint update **************************\n' + var_outputStr
# sendEmail(var_receiverEmail, var_titleEmail_1, var_outputStr_1 )
# print(var_outputStr)



# ========================== article groupping ==========================


var_len = news_articlesFunc.var_yy

for var_dtldAnalysis in ['y', 'n']:

    var_outputStr += f"""****** article grouping detailed analysis {var_dtldAnalysis} start on {datetime.now().strftime('%Y-%m-%d %H:%M')}******\n"""

    var_lmtVal = 2 # limit of returned sql. increase when there is undefined alias_name
    var_pickVal = 0 # will pick from returned sql. increase when there is undefined alias_name
    var_i = 0
    var_end = 0

    var_iOldGroupUrl = 0
    var_iNewGroupUrl = 0
    var_iUnallocated = 0
    var_iNoRegion = 0

    # try:

    while (var_i < var_len) and (var_end < 1):

        if (var_printUpdt =='y') or (var_printUpdt =='yes'): print('limit value:  ', str(var_lmtVal), '; pick value:  ', str(var_pickVal))

        var_counterStr = f"""{str(var_i)} of {str(var_len)} """


        df_urlToAnalyse = news_articlesFunc.getUrlForAtcl(detailed_analysis = var_dtldAnalysis, url_count = var_lmtVal)
        # print(df_urlToAnalyse, '\n\n')
        if (df_urlToAnalyse.empty == False) and (len(df_urlToAnalyse) >= (var_pickVal + 1) ):
            var_url, var_newsType, var_hdlnDt = df_urlToAnalyse.iloc[var_pickVal][['news_url', 'news_type', 'headline_date']]
            df_atclEntyVar1 = news_connectSQL.downloadSQLQuery('article_entity', check_col = 'news_url', check_value = var_url)
            list_entyNameRgn = df_atclEntyVar1.loc[(df_atclEntyVar1.entity_type == 'region')].entity_name.tolist()
            list_keySbjId = df_atclEntyVar1.loc[(df_atclEntyVar1.key_subject == '1') & (df_atclEntyVar1.entity_type != 'region')].entity_id.tolist()

            # check if article_entity for url has region
            if (len(list_entyNameRgn) > 0) and pd.isna(var_hdlnDt) == False:
                # look for similar articles already tagged
                list_atclId = list()
                if (var_dtldAnalysis == 'y') or (var_dtldAnalysis == 'yes'):
                    if len(list_keySbjId) > 0:
                        list_atclId = news_articlesFunc.getSmlrAtclInfo(var_newsType, var_hdlnDt, list_entyNameRgn, after_headline_date = 'y', detailed_analysis = 'y', key_subject_list = list_keySbjId )
                else:
                    list_atclId = news_articlesFunc.getSmlrAtclInfo(var_newsType, var_hdlnDt, list_entyNameRgn, after_headline_date = 'y', detailed_analysis = 'n' )

                if len(list_atclId) > 0:
                    var_atclId = list_atclId[0]
                    if (var_printUpdt =='y') or (var_printUpdt =='yes'): print('previous similar article_id found ', str(var_atclId), ' with news_type: ', var_newsType)
                    var_str_1 = var_counterStr + str(var_atclId) + ' ' + var_url + ' '
                    news_connectSQL.replaceSQLQuery('article_entity', 'news_url', var_url, ['article_id', 'updated_at'] , [var_atclId, datetime.now() ] , upload_to_sql = var_upldOk)
                    var_str_1 += news_articlesFunc.upldAtclIdPipeline(var_atclId, additional_info = 'change') 
                    # print(var_str_1)

                    var_iOldGroupUrl += 0


                else:
                    list_entyNameNA = df_atclEntyVar1.loc[(df_atclEntyVar1.entity_id.isnull()) | (pd.isna(df_atclEntyVar1.entity_id)) | ((df_atclEntyVar1.entity_id == ''))].alias_name.unique()
                    
                    # look for unallocation alias_name
                    if (len(list_entyNameNA) == 0):
                        var_atclId = news_connectSQL.getNextAtclId()  
                        if (var_dtldAnalysis == 'y') or (var_dtldAnalysis == 'yes'):
                            if len(list_keySbjId) > 0:
                                list_urlIncl = news_articlesFunc.getSmlrAtclInfo(var_newsType, var_hdlnDt, list_entyNameRgn, after_headline_date = 'n', detailed_analysis = 'y', key_subject_list = list_keySbjId )
                                if var_url not in list_urlIncl: list_urlIncl.append(var_url)
                                if (var_printUpdt =='y') or (var_printUpdt =='yes'): print('detailed analysis with key_subject ', str(var_atclId), ' with news_type: ', var_newsType, ' with urls to link:  ', len(list_urlIncl)  )
                            else:
                                list_urlIncl = [var_url]
                                if (var_printUpdt =='y') or (var_printUpdt =='yes'): print('single detailed analysis ', str(var_atclId), ' with news_type: ', var_newsType, ' with urls to link:  ', len(list_urlIncl)  )
                        else:
                            list_urlIncl = news_articlesFunc.getSmlrAtclInfo(var_newsType, var_hdlnDt, list_entyNameRgn, after_headline_date = 'n', detailed_analysis = 'n' )
                            if var_url not in list_urlIncl: list_urlIncl.append(var_url)
                            if (var_printUpdt =='y') or (var_printUpdt =='yes'): print('not detailed analysis ', str(var_atclId), ' with news_type: ', var_newsType, ' with urls to link:  ', len(list_urlIncl)  )

                        # update article pipeline
                        if len(list_urlIncl) > 0:
                            list_rplcAtclId = news_articlesFunc.getAtclIdListFromUrl(list_urlIncl)
                            if len(list_rplcAtclId) > 0:
                                var_str_2 =  news_articlesFunc.upldAtclIdPipeline(list_rplcAtclId, additional_info = 'change') 

                            for var_urlRplc in list_urlIncl:
                                news_connectSQL.replaceSQLQuery('article_entity', 'news_url', var_urlRplc, ['article_id', 'updated_at'] , [var_atclId, datetime.now() ] , upload_to_sql = var_upldOk)
                                var_strVar = var_counterStr + f""" assigned to {var_urlRplc} link to {str(var_atclId)}""" 
                                # print(var_strVar)
                            
                            var_str_2 = news_articlesFunc.upldAtclIdPipeline(var_atclId)
                            var_iNewGroupUrl += 1
                        else:
                            var_str_1 = var_counterStr
                            var_strVar = var_str_1 + var_url + f""" no linked url for {str(var_atclId)}""" 
                            # print(var_strVar)                
                            var_lmtVal += 1
                            var_pickVal += 1
                            var_iUnallocated += 1 
                    # if there is unallocation alias_name, end url.
                    else:
                        var_str_1 = var_counterStr
                        var_strVar = var_str_1 + var_url + f""" contains unallocated entity_name {str(list_entyNameNA)}""" 
                        # print(var_strVar)                
                        var_lmtVal += 1
                        var_pickVal += 1
                        var_iUnallocated += 1 

                # except Exception as e:
                #     print('\n\n')
                #     for var_urlRplc in list_urlIncl:
                #         var_strVar = var_str_1 + '  with ERROR assigning article_id'
                #         print(var_strVar)
                #     print('\n')

                
                # print('\n\n')

            else:
                var_str_1 = var_counterStr
                var_strVar = var_str_1 + var_url + f""" does not have region as entity_type""" 
                # print(var_strVar)                
                var_lmtVal += 1
                var_pickVal += 1
                var_iNoRegion += 1                     

        else:
            var_end = 1
            var_strVar = f"""no more articles to summarize with detailed analysis {var_dtldAnalysis}\n\n\n"""

        var_i += 1
        print(var_strVar)

    var_outputStr += f"""url grouped with previous articles: {str(var_iOldGroupUrl)}\nnew articles created {str(var_iNewGroupUrl)}\nurl with unallocated ERROR {str(var_iUnallocated)}\nurl with no region {str(var_iNoRegion)}\n"""

    # except:
    #     var_outputStr += f"""url grouped with previous articles: {str(var_iOldGroupUrl)}\nnew articles created {str(var_iNewGroupUrl)}\nurl with unallocated ERROR {str(var_iUnallocated)}\nurl with no region {str(var_iNoRegion)}\n"""
    #     var_outputStr += f""" ERROR article grouping detailed analysis {var_dtldAnalysis}"""

    var_outputStr += f"""\n****** article grouping detailed analysis {var_dtldAnalysis} END on {datetime.now().strftime('%Y-%m-%d %H:%M')}******\n\n"""

# ======================== article groupping END ========================
# =======================================================================



# # send checkpoint email
# var_titleEmail_1 = 'checkpoint 2 ' + var_titleEmail
# var_outputStr_1 = '************************** checkpoint update **************************\n' + var_outputStr
# sendEmail(var_receiverEmail, var_titleEmail_1, var_outputStr_1 )
# print(var_outputStr)




# ========================= create news article =========================
var_outputStr += f"""****** article writing start on {datetime.now().strftime('%Y-%m-%d %H:%M')}******\n"""


var_i = 0

var_iSccs = 0
var_iNUrl = 0
var_iFail = 0
var_iAsgn = 0

# try:
df_atclPpln = news_connectSQL.downloadSQLQuery('article_pipeline', check_col = 'article_status', check_value = 0)

df_atclPpln.sort_values(by = 'created_at', ascending = False, inplace = True)

list_atclId = df_atclPpln.article_id.unique()[ : 550]

var_len = len(list_atclId)

print(str(len(list_atclId)), ' articles to add to news_articles table' )

for var_atclId in list_atclId:
    # print(var_atclId)
    # df_atclSumm_var = news_connectSQL.downloadSQLQuery('news_articles', check_col = 'article_id', check_value = var_atclId)
    df_atclPpln_var = news_connectSQL.downloadSQLQuery('article_pipeline', check_col = ['article_id', 'article_status'], check_value = [var_atclId, 0])
    var_i += 1
    var_counterStr = f"""{str(var_i)} of {str(var_len)}  """

    var_str_1 = var_counterStr + str(var_atclId) + ' '

    # if (df_atclSumm_var.empty == False):
    #     news_connectSQL.deleteSQLRow('news_articles', 'article_id', var_atclId, confirm_delete = 'y')
    #     var_str_1 += ' atcl to be REPLACED; '

    if (df_atclPpln_var.empty == False):
        # try:
        var_str_1 += news_articlesFunc.createAtclSumm(var_atclId)

        if 'successfully uploaded' in var_str_1:
            var_iSccs += 1
        elif 'no URLs' in var_str_1:
            var_iNUrl += 1
        elif 'upload FAILED' in var_str_1:
            var_iFail += 1
        else:
            pass
    else:
        var_str_1 += ' article already assigned'
        var_iAsgn += 1
    print(var_str_1)

var_outputStr += f"""article_id allocated: {str(var_iSccs)}\narticle_id with no url {str(var_iNUrl)}\narticle_id with ERROR {str(var_iFail)}article_id already assigned {str(var_iAsgn)}\n"""
# except:
#     var_outputStr += f"""article_id allocated: {str(var_iSccs)}\narticle_id with no url {str(var_iNUrl)}\narticle_id with ERROR {str(var_iFail)}article_id already assigned {str(var_iAsgn)}\n"""
#     var_outputStr += f""" ERROR article writing"""
var_outputStr += f"""\n****** article writing END on {datetime.now().strftime('%Y-%m-%d %H:%M')}******\n\n"""

# ======================= create news article END =======================
# =======================================================================



sendEmail(var_receiverEmail, var_titleEmail, var_outputStr )
print(var_outputStr)
    