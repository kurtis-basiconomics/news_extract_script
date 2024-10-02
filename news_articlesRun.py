from news_tools import *

import news_articlesFunc

print('\n\n')
var_titleEmail = f"""Report on Category Summary on {datetime.now().strftime('%Y-%m-%d')}"""

var_outputStr = 'see category and summary run analysis below:\n'
var_outputStr += f"""****** category and summary analysis start on {datetime.now().strftime('%Y-%m-%d %H:%M')}******\n"""

list_newsSource = news_connectSQL.downloadSQLQuery('news_source_list' ).table_name.tolist() #['news_wsj', 'news_blm', 'news_mkw', 'news_ft'] #  

list_actualSrc = list()
list_scrapeOk = list()
for var_tblName in list_newsSource:
    var_scrapeOk = news_articlesFunc.runCtgrAndSummFunc(var_tblName)
    if var_scrapeOk > 0:
        list_actualSrc.append(var_tblName)
        list_scrapeOk.append(var_scrapeOk)
var_outputStr += f"""\n****** category and summary analysis END on {datetime.now().strftime('%Y-%m-%d %H:%M')}******\n\n"""

if len(list_scrapeOk) > 0:
    for var_scrapeOk, var_actualSrc in zip(list_scrapeOk, list_actualSrc):
        var_outputStr += f"""table {var_actualSrc} with {str(var_scrapeOk)} successful category \n""" 
else: 
    var_outputStr += ' no successful category'


var_outputStr += '\n\n\n'
var_outputStr += f"""****** split text analysis start on {datetime.now().strftime('%Y-%m-%d %H:%M')}******\n\n"""
print('\n\n start category split\n')
var_outputStrCtgrSplit = news_articlesFunc.runCtgrSplitText()
var_outputStr += var_outputStrCtgrSplit
var_outputStr += f"""\n****** split text analysis END on {datetime.now().strftime('%Y-%m-%d %H:%M')}******\n\n\n"""
print('\n\n category split END')

# send checkpoint email
var_titleEmail_1 = 'checkpoint 1  ' + var_titleEmail
var_outputStr_1 = '************************** checkpoint update **************************\n' + var_outputStr
sendEmail(var_receiverEmail, var_titleEmail_1, var_outputStr_1 )
print(var_outputStr)



# ======================== ASSIGNING ALIAS ========================

# ==================== download missing entity_name ====================
var_outputStr += f"""****** download missing alias_name start on {datetime.now().strftime('%Y-%m-%d %H:%M')}******\n"""
var_sqlText = f"""
        select distinct alias_name, entity_type 
        from basiconomics_news_schema.article_entity 
        where entity_name is null
        group by 1, 2;
    """

df_atcleEntyMissing = news_connectSQL.getRawSQLQuery(var_sqlText)
df_atcleEntyMissing['alias_plane'] = df_atcleEntyMissing['alias_name'].str.lower().apply(unidecode.unidecode)

df_atcleEntyMissing_noEntyType = df_atcleEntyMissing.loc[pd.isna(df_atcleEntyMissing.entity_type)]
if df_atcleEntyMissing_noEntyType.empty == False:
    df_atcleEntyMissing_noEntyType['additional_info'] = 'no entity_type'
    news_connectSQL.uploadSQLQuery(df_atcleEntyMissing_noEntyType[['alias_name', 'additional_info']] , 'alias_additions')
    var_outputStr += f"""uploaded to alias_additions table because of no entity_type: {str(len(df_atcleEntyMissing_noEntyType))}\n"""
df_atcleEntyMissing.dropna(subset = 'entity_type', ignore_index = True, inplace = True)

var_outputStr += f"""missing alias_name from article_entity: {str(len(df_atcleEntyMissing))}\n"""
var_outputStr += f"""\n****** download missing alias_name END on {datetime.now().strftime('%Y-%m-%d %H:%M')}******\n\n\n"""



# ==================== found alias_match ====================
var_outputStr += f"""****** match on alias_table start on {datetime.now().strftime('%Y-%m-%d %H:%M')}******\n"""

df_alias = news_connectSQL.downloadSQLQuery('alias_table')
df_alias['curr_alias'] = df_alias['alias_name']
df_alias['alias_plane'] = df_alias['alias_name'].str.lower().apply(unidecode.unidecode)

# match missing data from article_entity to alias_table to get entity_name and entity_id using alias_plane
df_atcleEntyMissing = df_atcleEntyMissing.merge( df_alias[['alias_plane', 'entity_id', 'entity_name', 'curr_alias']] , how = 'left', on = 'alias_plane')
df_atcleEntyMissing_foundAlias = df_atcleEntyMissing.loc[~pd.isna(df_atcleEntyMissing.entity_name)]
if df_atcleEntyMissing_foundAlias.empty == False:
    # matching alias_plane but not alias_name, need to upload new alias_name
    df_atcleEntyMissing_foundAlias_newAlias = df_atcleEntyMissing_foundAlias.loc[df_atcleEntyMissing_foundAlias.alias_name != df_atcleEntyMissing_foundAlias.curr_alias]
    if df_atcleEntyMissing_foundAlias_newAlias.empty == False:
        try:
            print('match on alias_table, uploading to alias_table ', str(len(df_atcleEntyMissing_foundAlias_newAlias)), ' rows' )
            news_connectSQL.uploadSQLQuery(df_atcleEntyMissing_foundAlias_newAlias[['alias_name', 'entity_name', 'entity_id', 'entity_type']] , 'alias_table')
            var_str_1 = f"""successfully uploaded match on alias_table {str(len(df_atcleEntyMissing_foundAlias_newAlias))} rows"""
        except:
            var_str_1 = 'FAILED ON UPLOADING AVAILABLE match on alias_table'
            var_titleEmail_2 = 'ERROR  ' + var_titleEmail_1
            var_outputStr_2 = var_str_1 + '  ' + datetime.now().strftime('%Y-%m-%d %H:%M')
            sendEmail(var_receiverEmail, var_titleEmail_2, var_outputStr_2 )

        print(var_str_1)
        var_outputStr += var_str_1 +'\n'

    var_outputStr += f"""match on alias_table from article_entity: {str(len(df_atcleEntyMissing_foundAlias))} """

    news_articlesFunc.upldToAtclEnty(df_atcleEntyMissing_foundAlias)
    var_outputStr += ' SUCCESS'
    df_atcleEntyMissing = df_atcleEntyMissing.loc[ pd.isna(df_atcleEntyMissing.entity_id) ]
else:
    print('no alias_match found ')
    var_outputStr += 'no alias_match found!!!'
var_outputStr += '\n'

var_outputStr += f"""\n****** match on alias_table END on {datetime.now().strftime('%Y-%m-%d %H:%M')}******\n\n\n"""


# send checkpoint email
var_titleEmail_1 = 'checkpoint 2  ' + var_titleEmail
var_outputStr_1 = '************************** checkpoint update **************************\n' + var_outputStr
sendEmail(var_receiverEmail, var_titleEmail_1, var_outputStr_1 )
print(var_outputStr)



# ==================== found close match ====================
var_outputStr += f"""****** close_match start on {datetime.now().strftime('%Y-%m-%d %H:%M')}******\n"""

df_atcleEntyMissing['alias_match'] = None

var_i = 0
var_iLen = len(df_atcleEntyMissing)
var_iSccs = 0

for var_aliasNew, var_entyType in zip(df_atcleEntyMissing.alias_name, df_atcleEntyMissing.entity_type):
    var_i += 1
    var_counterStr = str(var_i) + ' of ' + str(var_iLen) + ' '

    var_str_2 = var_counterStr + var_aliasNew + ' ' + var_entyType
    try:
        if var_entyType == 'person':
            # Iterate through the list and remove any occurrences of strings found in var_aliasNew
            for title in news_articlesFunc.list_titlesToExclPpl:
                var_aliasNew = var_aliasNew.replace(title, '')
            var_aliasNew = var_aliasNew.strip()

        list_strNew = var_aliasNew.split(' ')

        df_aliasVar = news_connectSQL.getCloseMatchFilterAliasDF(list_strNew, var_entyType)
        list_closestMatch = difflib.get_close_matches(var_aliasNew, df_aliasVar.alias_name.unique(), cutoff = 0.90)
        # print(list_closestMatch)

        if len(list_closestMatch) > 0:
            # print(list_closestMatch[0])

            df_atcleEntyMissing.loc[
                    (df_atcleEntyMissing.alias_name == var_aliasNew) &
                    (df_atcleEntyMissing.entity_type == var_entyType),
                    ['alias_match', 'entity_id', 'entity_name' ]
                ] = df_aliasVar.loc[df_aliasVar.alias_name == list_closestMatch[0]][['alias_name', 'entity_id', 'entity_name' ]].values
            var_str_2 += f""" found match with {list_closestMatch[0]} """
            var_iSccs += 1
            var_outputStr += var_str_2 + '\n'
            # get alias_name from df_aliasVara
        else:
            var_str_2 += ' match NOT found'
    except:
        var_str_2 += ' with error'
    print(var_str_2)

var_outputStr += f"""number of success close match found {str(var_iSccs)}""" + '\n'

# print(df_atcleEntyMissing)
df_atcleEntyMissing_foundCloseMatch = df_atcleEntyMissing.dropna(subset = ['entity_id'])
# print(df_atcleEntyMissing_foundCloseMatch)
# var_outputStr += '\n\n\n'
# var_outputStr += df_atcleEntyMissing_foundCloseMatch.to_string()
# var_outputStr += '\n\n\n'

if df_atcleEntyMissing_foundCloseMatch.empty == False:
    # df_atcleEntyMissing_foundCloseMatch = df_atcleEntyMissing_foundCloseMatch.merge( df_alias[['curr_alias', 'entity_name', 'entity_id']] , how = 'left', left_on = 'alias_match', right_on = 'curr_alias')

    # matching alias_plane but not alias_name, need to upload new alias_name
    df_atcleEntyMissing_foundCloseMatch_newAlias = df_atcleEntyMissing_foundCloseMatch.loc[df_atcleEntyMissing_foundCloseMatch.alias_name != df_atcleEntyMissing_foundCloseMatch.curr_alias]
    if df_atcleEntyMissing_foundCloseMatch_newAlias.empty == False:

        try:
            print('close_match, uploading to alias_table ', str(len(df_atcleEntyMissing_foundCloseMatch_newAlias)), ' rows' )
            news_connectSQL.uploadSQLQuery(df_atcleEntyMissing_foundCloseMatch_newAlias[['alias_name', 'entity_name', 'entity_id', 'entity_type']] , 'alias_table')
            var_str_1 = f"""successfully uploaded all close_match { str(len(df_atcleEntyMissing_foundCloseMatch_newAlias)) } rows """
        except:
            var_str_1 = 'FAILED ON UPLOADING AVAILABLE found close_match'
            var_titleEmail_2 = 'ERROR  ' + var_titleEmail_1
            var_outputStr_2 = var_str_1 + '  ' + datetime.now().strftime('%Y-%m-%d %H:%M')
            sendEmail(var_receiverEmail, var_titleEmail_2, var_outputStr_2 )
        print(var_str_1)
        var_outputStr += var_str_1 + '\n'

    var_outputStr += f"""match on close_match from article_entity: {str(len(df_atcleEntyMissing_foundCloseMatch))} """
    news_articlesFunc.upldToAtclEnty(df_atcleEntyMissing_foundCloseMatch)
    var_outputStr += ' SUCCESS'
    df_atcleEntyMissing = df_atcleEntyMissing.loc[ pd.isna(df_atcleEntyMissing.entity_id) ]
else:
    print(' no data found ')
    var_outputStr += ' no close match to upload found!!!'
    var_outputStr += '\n'

var_outputStr += f"""\n****** close_match from article_entity END on {datetime.now().strftime('%Y-%m-%d %H:%M')}******\n\n\n"""



# send checkpoint email
var_titleEmail_1 = 'checkpoint 3  ' + var_titleEmail
var_outputStr_1 = '************************** checkpoint update **************************\n' + var_outputStr
sendEmail(var_receiverEmail, var_titleEmail_1, var_outputStr_1 )
# print(var_outputStr)



# ==================== create new entity ====================
df_enty = news_connectSQL.downloadSQLQuery('entity_table')
df_enty['curr_entity'] = df_enty['entity_name']
df_enty['entity_plane'] = df_enty['entity_name'].str.lower().apply(unidecode.unidecode)
print(df_atcleEntyMissing)
df_atcleEntyMissing_new = df_atcleEntyMissing.loc[pd.isna(df_atcleEntyMissing.entity_id)]

df_atcleEntyMissing_new_enty = df_atcleEntyMissing_new.loc[
    ~(df_atcleEntyMissing_new.alias_name.isin( df_enty.entity_name.unique() ))
    ]
print(df_atcleEntyMissing_new_enty)
if df_atcleEntyMissing_new_enty.empty == False:

    var_entyId = news_connectSQL.getNewEntityID()

    df_atcleEntyMissing_new_enty['entity_id'] = np.arange(var_entyId, (var_entyId + len(df_atcleEntyMissing_new_enty)) )
    df_atcleEntyMissing_new_enty['entity_name'] = df_atcleEntyMissing_new_enty['alias_name']
    df_atcleEntyMissing_new_enty.dropna(subset = ['entity_name'], inplace = True, ignore_index = True)

    if df_atcleEntyMissing_new_enty.empty == False:

        try:
            print('uploading new entity_name to entity_table, uploading ', str(len(df_atcleEntyMissing_new_enty)), ' rows' )
            news_connectSQL.uploadSQLQuery( df_atcleEntyMissing_new_enty[['entity_name', 'entity_id', 'entity_type']] , 'entity_table')
            var_str_1 = f"""successfully uploaded new entity on entity_table {str(len(df_atcleEntyMissing_new_enty)) } rows"""
        except:
            var_str_1 = 'FAILED ON UPLOADING AVAILABLE new entity on entity_table'
            var_titleEmail_2 = 'ERROR  ' + var_titleEmail_1
            var_outputStr_2 = var_str_1 + '  ' + datetime.now().strftime('%Y-%m-%d %H:%M')
            sendEmail(var_receiverEmail, var_titleEmail_2, var_outputStr_2 )
        print(var_str_1)
        var_outputStr += var_str_1 +'\n'

        try:
            print('uploading new entity_name to alias_table, uploading ', str(len(df_atcleEntyMissing_new_enty)), ' rows' )
            news_connectSQL.uploadSQLQuery( df_atcleEntyMissing_new_enty[['alias_name', 'entity_name', 'entity_id', 'entity_type']] , 'alias_table')
            var_str_1 = f"""successfully uploaded new entity on alias_table {str(len(df_atcleEntyMissing_new_enty)) } rows"""
        except:
            var_str_1 = 'FAILED ON UPLOADING AVAILABLE new entity on alias_table'
            var_titleEmail_2 = 'ERROR  ' + var_titleEmail_1
            var_outputStr_2 = var_str_1 + '  ' + datetime.now().strftime('%Y-%m-%d %H:%M')
            sendEmail(var_receiverEmail, var_titleEmail_2, var_outputStr_2 )
        print(var_str_1)
        var_outputStr += var_str_1 +'\n'

        var_outputStr += f"""created new entity_id and add to article_entity table: {str(len(df_atcleEntyMissing_new_enty))} """
        news_articlesFunc.upldToAtclEnty(df_atcleEntyMissing_new_enty )
        var_outputStr += ' SUCCESS'

    else:
        var_outputStr += ' NO new entity_id required. dataframe empty\n'
else:
    var_outputStr += ' NO new entity_id required. dataframe empty\n'

var_outputStr += f"""\n****** new entity_name to alias_table END on {datetime.now().strftime('%Y-%m-%d %H:%M')}******\n\n\n"""


sendEmail(var_receiverEmail, var_titleEmail, var_outputStr )
print(var_outputStr)

