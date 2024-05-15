from news_tools import *


client = ZenRowsClient(open(news_variables.path_zenrowssKeykcgmail, 'r').read().strip('\n'))


var_tblName = 'news_glb'



headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537'}

var_baseUrl = 'https://www.theglobeandmail.com/' # 2024/01/01'


# Download Headlines 

print('\n\n', var_baseUrl, 'downloading headlines')
response = requests.get(var_baseUrl, headers=headers)

if response.status_code == 200:
    soup = BeautifulSoup(response.content, 'html.parser')

    # Regex to match class names
    class_regex = re.compile(r'CardLink__StyledCard')

    df_hdlnNew = pd.DataFrame(columns = ['news_title', 'news_url'])

    # Find all <a> tags with a class that matches the regex
    matching_links = soup.find_all('a', class_=lambda x: x and class_regex.search(x))


    # Print the matching <a> tags
    for link in matching_links:
        try:
            var_href = link['href']
            if ('https' not in var_href):
                var_url = var_baseUrl[ : -1] + var_href
                var_title = link.text
                df_var = pd.DataFrame([[var_title, var_url]], columns = df_hdlnNew.columns)
                
                if (var_url not in df_hdlnNew.news_url.unique()):
                    df_hdlnNew = pd.concat([df_hdlnNew, df_var], ignore_index = True )

                # Upload Headline to SQL
                if news_connectSQL.checkNewsUrlInTbl(var_tblName, var_url):
                    df_var['created_at'] = datetime.now()
                    news_connectSQL.uploadSQLQuery(df_var, var_tblName)
                    print(var_url, var_title)
        except:
            pass

    print(var_baseUrl, 'headline download finished \n\n')





    # Downloading News Text
    print('\n\ntext download for ', var_tblName, ' start !!')


    df_sql = news_connectSQL.downloadSQLQuery(var_tblName, date_col = 'created_at', date_from = ( (datetime.today() - timedelta(days = 3)).strftime('%Y-%m-%d') ))

    df_sql = df_sql[ 
        (pd.isna(df_sql.news_text) | (df_sql.news_text.isna() ) | (df_sql.news_text == '' )) &
        (~df_sql.news_url.str.contains('/video/') ) 
        ]

    list_urlDwld = df_sql.news_url.unique()

    var_i = 0
    var_len = len(list_urlDwld)
    for var_url in list_urlDwld:

        var_i += 1
        var_countStr = str(var_i) + ' of ' + str(var_len)
        try:
            response = requests.get(var_url, headers=headers)

            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')

                # DOwnload News Text
                try:
                    var_output = ''
                    list_articles = soup.find_all('p')
                    for var_article in list_articles:
                        try:
                            # print(var_article.text)
                            var_output += var_article.text
                        except:
                            pass
                    df_hdlnNew.loc[df_hdlnNew.news_url == var_url, 'news_text'] = var_output
                except Exception as e:
                    print(var_url, var_countStr, 'with text error:  ', e)

                # Download News Date
                try:
                    var_dt = soup.find_all('time')[0]['datetime']
                    var_dt = pd.to_datetime(var_dt)
                except Exception as e:
                    var_dt = ''
                    print(var_url, var_countStr, 'with time error:  ', e)
                df_hdlnNew.loc[df_hdlnNew.news_url == var_url, 'headline_date'] = pd.NaT


                try:
                    news_connectSQL.replaceSQLQuery(var_tblName, 'news_url', var_url, ['news_text', 'headline_date'], [str(var_output), pd.to_datetime(var_dt)])
                except Exception as e:
                    print(var_url, ' upload to databse with error:  ', e)


                print(var_url, var_countStr, ' finished')

            else:
                print(var_url, var_countStr, ' failed extract')
        except:
            print(var_url, var_countStr, 'failed extract')

    try:
        uploadGithubBackup(df_hdlnNew, var_tblName)
    except Exception as e:
        print('\n\n', var_tblName, 'export to csv with error:  ', e)

    print('\n\n', getDataInfo(var_tblName, date_from = (datetime.today() - timedelta(days = 5)).strftime('%Y-%m-%d') ) )


else: 
    print('\n\n', var_tblName, ' headline extract failed!!')