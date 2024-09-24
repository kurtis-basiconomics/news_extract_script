from news_tools import *

var_tblName = 'news_mkw'

var_upldOk = 'y'

var_splitPattern = r'(?<=[a-z])(?=[A-Z])'

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537'}

list_url = ['https://www.marketwatch.com/', 'https://www.marketwatch.com/markets', 'https://www.marketwatch.com/investing', 'https://www.marketwatch.com/economy-politics']


df_hdlnNew = pd.DataFrame(columns = ['news_url', 'news_title', 'headline_date'])

print('\n\n', var_tblName, 'downloading headlines')
for var_baseUrl in list_url:
    # response = requests.get(var_baseUrl, headers=headers)
    print(var_baseUrl, ' start headline extract')
    response, var_outputStrZenrows = getZenrowsResponse(var_baseUrl, 0, use_chatgpt = 'zenrows-api')


    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')

        headline_elements = soup.find_all(attrs={"class": "element element--article"})

        for var_element in headline_elements:
            try:            
                # Extract the href attribute
                var_url = var_element.find('a')['href']

                var_url = var_url.split('?')[0]
                
                # Extract the text, stripping extra whitespace
                var_title = var_element.get_text(strip=True)
                try:
                    var_title = re.split(var_splitPattern, var_title)[0]
                except:
                    pass
                
                var_dt = pd.NaT
                if var_element.get('data-timestamp'):
                    try:
                        # Extract the data-timestamp attribute from the parent div element
                        var_dt = var_element.get('data-timestamp')
                        var_dt = int(var_dt) / 1000
                        var_dt = datetime.fromtimestamp(var_dt)
                        # print(var_dt)
                    except:
                        pass

                df_var = pd.DataFrame([[var_url, var_title, var_dt]], columns = df_hdlnNew.columns)

                if (var_url not in df_hdlnNew.news_url.unique()):
                    df_hdlnNew = pd.concat([df_hdlnNew, df_var], ignore_index = True )

                # Upload Headline to SQL
                if news_connectSQL.checkNewsUrlInTbl(var_tblName, var_url):
                    df_var['created_at'] = datetime.now()
                    news_connectSQL.uploadSQLQuery(df_var, var_tblName)
                    print(var_baseUrl, var_url, var_title)
            
            except Exception as e:
                print(var_baseUrl, var_element, 'with error:  ', e, '\n\n')
    else:
        print(var_baseUrl, 'not 200 repsonse')

    print(var_baseUrl, var_tblName, 'headline download finished \n\n')


    # Downloading News Text
    print('\n\ntext download for ', var_tblName, ' start !!')


    # downloading urls to scrape for MKW
    list_strExcl = news_connectSQL.downloadSQLQuery('news_exclusions', check_value = var_tblName.replace('news_', '') , check_col = 'news_source' ).table_string.tolist()

    df_sql = news_connectSQL.downloadSQLQuery(var_tblName, date_col = 'created_at', date_from = var_dtFrAnalysis )
    mask = df_sql['news_url'].apply(lambda x: not any(substring in x for substring in list_strExcl))
    df_sql = df_sql[ 
        (pd.isna(df_sql.news_text) | (df_sql.news_text.isna() )) & 
        mask
        ]
    df_sql['news_url'] = df_sql.news_url.str.split('?').str[0]
    df_sql.drop_duplicates(subset = ['news_url'], keep = 'first', inplace = True, ignore_index = True)
    df_sql.sort_values(by = 'headline_date' , ascending = False, ignore_index = True, inplace = True)


    list_urlDwld = df_sql.news_url.unique()
    print( var_tblName, len(list_urlDwld) , ' to analyse')

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

                if 'https://www.marketwatch.com/' in var_url:
                    var_tblUpld = var_tblName
                            
                if var_fullText != '' and var_fullText is not None:
                    var_outputStr += news_connectSQL.replaceSQLQuery(var_tblUpld, 'news_url', var_url, ['news_text'], [var_fullText], upload_to_sql = var_upldOk)
                    var_outputStr += ' PRINTED'
                else:
                    var_outputStr += ' EMPTY'

            else:
                var_outputStr += ' response NOT 200'

        except:
            var_outputStr += ' with error'

        print(var_outputStr)


try:
    uploadGithubBackup(df_hdlnNew, var_tblName, hdln_backup = 'y')
except Exception as e:
    print('\n\n', var_tblName, 'export to csv with error:  ', e)

print('\n\n', getDataInfo(var_tblName) )
