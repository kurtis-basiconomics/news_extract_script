from news_tools import *


var_tblName = 'news_wsj'
var_tblName_1 = 'news_mkw'


var_upldOk = 'y'


# downloading urls to scrape for WSJ
list_strExcl = news_connectSQL.downloadSQLQuery('news_exclusions', check_value = var_tblName.replace('news_', '') , check_col = 'news_source' ).table_string.tolist()

df_sql = news_connectSQL.downloadSQLQuery(var_tblName, date_col = 'created_at', date_from = '2024-07-01' )

mask = df_sql['news_url'].apply(lambda x: not any(substring in x for substring in list_strExcl))

df_sql = df_sql[ 
    (pd.isna(df_sql.news_text) | (df_sql.news_text.isna() )) & 
    mask
    ]

df_sql['news_url'] = df_sql.news_url.str.split('?').str[0]
df_sql.drop_duplicates(subset = ['news_url'], keep = 'first', inplace = True, ignore_index = True)

df_sql.sort_values(by = 'headline_date' , ascending = False, ignore_index = True, inplace = True)

# downloading urls to scrape for MKW
list_strExcl_1 = news_connectSQL.downloadSQLQuery('news_exclusions', check_value = var_tblName_1.replace('news_', '') , check_col = 'news_source' ).table_string.tolist()

df_sql_1 = news_connectSQL.downloadSQLQuery(var_tblName_1, date_col = 'created_at', date_from = var_dtFrAnalysis )

mask_1 = df_sql_1['news_url'].apply(lambda x: not any(substring in x for substring in list_strExcl_1))

df_sql_1 = df_sql_1[ 
    (pd.isna(df_sql_1.news_text) | (df_sql_1.news_text.isna() )) & 
    mask_1
    ]

df_sql_1['news_url'] = df_sql_1.news_url.str.split('?').str[0]
df_sql_1.drop_duplicates(subset = ['news_url'], keep = 'first', inplace = True, ignore_index = True)

df_sql_1.sort_values(by = 'headline_date' , ascending = False, ignore_index = True, inplace = True)


df_sql = pd.concat([df_sql.iloc[ : 350], df_sql_1.iloc[ : 350] ], ignore_index = True)

list_urlDwld = df_sql.news_url.unique()
print(var_tblName, ' and ', var_tblName_1, len(list_urlDwld) , ' to analyse')


var_i = 0
var_len = len(list_urlDwld)

for var_url in list_urlDwld:


    var_i = var_i + 1
    var_counterStr = str(var_i) + ' of ' + str(var_len) + ' '

    var_outputStr = var_counterStr + var_url + ' '
        
    try:

        var_rspn, var_outputStrZenrows = getZenrowsResponse(var_url, 2)
        var_outputStr += var_outputStrZenrows

        if var_rspn.status_code == 200:
            var_fullText = getResponceToTextWsj(var_rspn)

            if 'https://www.wsj.com/' in var_url:
                var_tblUpld = var_tblName
            elif'https://www.marketwatch.com/' in var_url:
                var_tblUpld = var_tblName_1
                        
            if var_fullText != '' and var_fullText is not None:
                var_outputStr += news_connectSQL.replaceSQLQuery(var_tblUpld, 'news_url', var_url, ['news_text'], [var_fullText], upload_to_sql = var_upldOk)
                var_outputStr += ' PRINTED'

        else:
            var_outputStr += ' response NOT 200'


    except:
        var_outputStr += ' with error'

    print(var_outputStr)