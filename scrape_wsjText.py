from news_tools import *


var_tblName = 'news_wsj'
var_tblName_1 = 'news_mkw'


var_upldOk = 'y'

list_kywd = ['/business/', '/finance/', '/tech/', '/economy/']

# Function to check if the URL contains any of the keywords
def checkUrlPriority(var_url):
  if any(keyword in var_url for keyword in list_kywd):
    return 1
  else:
    return 0

# function to extract list of url to download

def getUrlList():
    list_strExcl = news_connectSQL.downloadSQLQuery('news_exclusions', check_value = var_tblName.replace('news_', '') , check_col = 'news_source' ).table_string.tolist()

    df_sql = news_connectSQL.downloadSQLQuery(var_tblName, date_col = 'created_at', date_from = var_dtFrAnalysis )

    mask = df_sql['news_url'].apply(lambda x: not any(substring in x for substring in list_strExcl))

    df_sql = df_sql[ 
        (pd.isna(df_sql.news_text) | (df_sql.news_text.isna() )) & 
        mask
        ]

    df_sql['news_url'] = df_sql.news_url.str.split('?').str[0]
    df_sql.drop_duplicates(subset = ['news_url'], keep = 'first', inplace = True, ignore_index = True)
    # Apply the function to the 'news_url' column and create a new column 'score'
    df_sql['priority_score'] = df_sql['news_url'].apply(checkUrlPriority)
    df_sql.sort_values(by = ['priority_score', 'headline_date'] , ascending = False, ignore_index = True, inplace = True)
    df_sql.drop(columns = ['priority_score'], inplace = True)

    list_urlDwld = df_sql.news_url.unique()

    return list_urlDwld;


list_urlDwldTtl = getUrlList()
var_maxLoop = math.ceil(len(list_urlDwldTtl) / 50)

var_iLoop = 0
var_iStop = 0

print(var_tblName, len(list_urlDwldTtl) , ' to analyse')


var_titleEmail = f"""LOCAL wsj scrape on {datetime.now().strftime('%Y-%m-%d')}"""

var_outputStr = 'wsj news scrape start:\n'
var_outputStr += f"""****** wsj news_scrape start on {datetime.now().strftime('%Y-%m-%d %H:%M')}******\n"""

var_i = 0
var_len = len(list_urlDwldTtl)

while (var_maxLoop >= var_iLoop) and (var_iStop == 0):

    list_urlDwld = getUrlList()[ : 100]

    var_iLoop += 1



    var_iSccs = 0
    var_iEmty = 0
    var_iN200 = 0
    var_iFail = 0

    for var_url in list_urlDwld:
        var_i = var_i + 1
        var_counterStr = str(var_i) + ' of ' + str(var_len) + ' '

        var_str_1 = var_counterStr + var_url + ' '

        df_var = news_connectSQL.downloadSQLQuery(var_tblName, check_col = 'news_url', check_value = var_url)
        df_var = df_var.loc[(df_var.news_text == None) | pd.isna(df_var.news_text) | (df_var.news_text == 'None') | (df_var.news_text == '')  ]

        if df_var.empty == False:

            var_rspn, var_str_1Zenrows = getZenrowsResponse(var_url, 2)
            var_str_1 += var_str_1Zenrows

            if var_rspn.status_code == 200:
                var_fullText = getResponceToTextWsj(var_rspn)

                if 'https://www.wsj.com/' in var_url:
                    var_tblUpld = var_tblName
                elif'https://www.marketwatch.com/' in var_url:
                    var_tblUpld = var_tblName_1
                            
                if var_fullText != '' and var_fullText is not None:
                    var_str_1 += news_connectSQL.replaceSQLQuery(var_tblUpld, 'news_url', var_url, ['news_text'], [var_fullText], upload_to_sql = var_upldOk)
                    var_str_1 += ' PRINTED'
                    var_iSccs += 1
                else:
                    var_str_1 += ' EMPTY'
                    var_iEmty += 1

            else:
                var_str_1 += ' response NOT 200'
                var_iN200 += 1

        else: 
            var_str_1 += ' already available'
            var_iFail += 1
        print(var_str_1)

    
    var_outputStr += f"""news text found: {str(var_iSccs)}\nnews text empty {str(var_iEmty)}\nresponse not 200 {str(var_iN200)}\nnews available {str(var_iFail)}\n"""
    var_outputStr += f"""\n****** wsj scrape {var_iLoop} of {var_maxLoop} END on {datetime.now().strftime('%Y-%m-%d %H:%M')}******\n\n"""

    var_titleEmail_1 = 'checkpoint ' + var_titleEmail
    var_outputStr_1 = '************************** checkpoint update **************************\n' + var_outputStr
    sendEmail(var_receiverEmail, var_titleEmail_1, var_outputStr_1 )
    print(var_outputStr)