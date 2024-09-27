from news_tools import *

import news_articlesFunc


print('\n\n')
var_outputStr = 'assigning alias_name and entity_name below:\n'
var_outputStr += f"""******assigning alias_name and entity_name start on {datetime.now().strftime('%Y-%m-%d %H:%M')}******\n"""


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

var_outputStr += f"""missing alias_name from article_entity: {str(str(df_atcleEntyMissing))}\n"""
var_outputStr += f"""\n****** download missing alias_name END on {datetime.now().strftime('%Y-%m-%d %H:%M')}******\n\n"""




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
        print(var_str_1)
        var_outputStr += var_str_1 +'\n'

    var_outputStr += f"""match on alias_table from article_entity: {str(str(df_atcleEntyMissing_foundAlias))} """

    news_articlesFunc.upldToAtclEnty(df_atcleEntyMissing_foundAlias)
    var_outputStr += ' SUCCESS'
else:
    print(' no data found ')
    var_outputStr += ' no data found!!!'
var_outputStr += '\n'

var_outputStr += f"""\n****** match on alias_table END on {datetime.now().strftime('%Y-%m-%d %H:%M')}******\n\n"""




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
        list_strNew = var_aliasNew.split(' ')

        df_aliasVar = news_connectSQL.getCloseMatchFilterAliasDF(list_strNew, var_entyType)
        list_closestMatch = difflib.get_close_matches(var_aliasNew, df_aliasVar.alias_name.unique(), cutoff = 0.90)

        if len(list_closestMatch) > 0:
            # df_atcleEntyMissing.loc[
            #         (df_atcleEntyMissing.alias_name == var_aliasNew) &
            #         (df_atcleEntyMissing.entity_type == var_entyType),
            #         'alias_match'
            #     ] = list_closestMatch[0]

            df_atcleEntyMissing.loc[
                    (df_atcleEntyMissing.alias_name == var_aliasNew) &
                    (df_atcleEntyMissing.entity_type == var_entyType),
                    ['alias_match', 'entity_id', 'entity_name', 'entity_type' ]
                ] = df_aliasVar.loc[df_aliasVar.alias_name == list_closestMatch[0]][['alias_name', 'entity_id', 'entity_name', 'entity_type' ]]
            var_str_2 += f""" found match with {list_closestMatch[0]} """
            var_iSccs += 1
            # get alias_name from df_aliasVara
        else:
            var_str_2 += ' match NOT found'
    except:
        var_str_2 += ' with error'
    print(var_str_2)

var_outputStr += f"""number of success close match found {str(var_iSccs)}""" + '\n'


# df_alias = news_connectSQL.downloadSQLQuery('alias_table')
# df_alias['curr_alias'] = df_alias['alias_name']
        
df_atcleEntyMissing_foundCloseMatch = df_atcleEntyMissing.dropna(subset = ['alias_match'])

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
        print(var_str_1)
        var_outputStr += var_str_1 + '\n'

    var_outputStr += f"""match on close_match from article_entity: {str(str(df_atcleEntyMissing_foundAlias))} """
    news_articlesFunc.upldToAtclEnty(df_atcleEntyMissing_foundCloseMatch)
    var_outputStr += ' SUCCESS'
else:
    print(' no data found ')
    var_outputStr += ' no data found!!!'
    var_outputStr += '\n'

var_outputStr += f"""\n****** close_match from article_entity END on {datetime.now().strftime('%Y-%m-%d %H:%M')}******\n\n"""




# ==================== create new entity ====================
df_enty = news_connectSQL.downloadSQLQuery('entity_table')
df_enty['curr_entity'] = df_enty['entity_name']
df_enty['entity_plane'] = df_enty['entity_name'].str.lower().apply(unidecode.unidecode)

df_atcleEntyMissing_new = df_atcleEntyMissing .loc[pd.isna(df_atcleEntyMissing.alias_match)]

df_atcleEntyMissing_new_enty = df_atcleEntyMissing_new.loc[
    ~(df_atcleEntyMissing_new.alias_name.isin( df_enty.entity_name.unique() ))
    ]

if df_atcleEntyMissing_new_enty.empty == False:

    var_entyId = news_connectSQL.getNewEntityID()

    df_atcleEntyMissing_new_enty['entity_id'] = np.arange(var_entyId, (var_entyId + len(df_atcleEntyMissing_new_enty)) )
    df_atcleEntyMissing_new_enty['entity_name'] = df_atcleEntyMissing_new_enty['alias_name']

    try:
        print('uploading new entity_name to entity_table, uploading ', str(len(df_atcleEntyMissing_new_enty)), ' rows' )
        news_connectSQL.uploadSQLQuery( df_atcleEntyMissing_new_enty.dropna(subset = ['entity_name'])[['entity_name', 'entity_id', 'entity_type']] , 'entity_table')
        var_str_1 = f"""successfully uploaded new entity on entity_table {str(len(df_atcleEntyMissing_new_enty)) } rows"""
    except:
        var_str_1 = 'FAILED ON UPLOADING AVAILABLE new entity on entity_table'
        print(var_str_1)
        var_outputStr += var_str_1 +'\n'

    try:
        print('uploading new entity_name to alias_table, uploading ', str(len(df_atcleEntyMissing_new_enty)), ' rows' )
        news_connectSQL.uploadSQLQuery( df_atcleEntyMissing_new_enty.dropna(subset = ['entity_name'])[['entity_name', 'entity_id', 'alias_name', 'entity_type']] , 'entity_table')
        var_str_1 = f"""successfully uploaded new entity on alias_table {str(len(df_atcleEntyMissing_new_enty)) } rows"""
    except:
        var_str_1 = 'FAILED ON UPLOADING AVAILABLE new entity on alias_table'
        print(var_str_1)
        var_outputStr += var_str_1 +'\n'

    var_outputStr += f"""match on close_match from article_entity: {str(str(df_atcleEntyMissing_foundAlias))} """
    news_articlesFunc.upldToAtclEnty(df_atcleEntyMissing_new_enty.dropna(subset = ['entity_name']) )
    var_outputStr += ' SUCCESS'

    var_outputStr += f"""\n****** new entity_name to alias_table END on {datetime.now().strftime('%Y-%m-%d %H:%M')}******\n\n"""


var_title = f"""assigning alias_name on {datetime.now().strftime('%Y-%m-%d')}"""
sendEmail(var_receiverEmail, var_title, var_outputStr )
print(var_outputStr)
