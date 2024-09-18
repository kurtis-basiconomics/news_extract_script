from news_tools import *


var_tblName = 'news_fp'



headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537'}

var_baseUrl = 'https://financialpost.com/' # 2024/01/01'


# Download Headlines 

df_hdlnNew = pd.DataFrame(columns = ['news_title', 'news_url'])
print('\n\n', var_baseUrl, 'downloading headlines')
response = requests.get(var_baseUrl, headers=headers)

if response.status_code == 200:
    soup = BeautifulSoup(response.content, 'html.parser')

    headline_elements = soup.find_all('a', class_='article-card__link')

    for a_element in headline_elements:
        try:
            var_href = a_element.get('href')
            var_url = var_baseUrl + var_href[1: ]

            var_title = a_element.get('aria-label')

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
        var_counterStr = str(var_i) + ' of ' + str(var_len)

        try:
            response = requests.get(var_url, headers=headers)

            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')

                # Get article body
                try:
                    list_articles = soup.find_all('p')
                    var_output = ''
                    for var_article in list_articles:
                        if ('class_' not in var_article.attrs) & (('class' not in var_article.attrs)):
                            if (var_article.text):
                                if 'save this article by registerin' not in var_article.text:
                                    var_output += var_article.text
                    df_hdlnNew.loc[df_hdlnNew.news_url == var_url, 'news_text'] = var_output
                except Exception as e:
                    print('FP', var_url, ' article text with error:  ', e)

                # Get article publish time
                try: 
                    script_tag = soup.find('script', type='application/ld+json')
                    if script_tag:
                        data = json.loads(script_tag.string)  # Load JSON data from the script tag
                        date_published = data.get('datePublished')
                        var_dt = pd.to_datetime(date_published)
                        df_hdlnNew.loc[df_hdlnNew.news_url == var_url, 'headline_date'] = var_dt
                except Exception as e:
                    print('FP', var_url, ' publish date with error:  ', e)
                    var_dt = ''

                try:
                    news_connectSQL.replaceSQLQuery(var_tblName, 'news_url', var_url, ['news_text', 'headline_date'], [str(var_output), var_dt])
                except Exception as e:
                    print(var_url, ' upload to databse with error:  ', e)

                print(var_counterStr, var_url, ' finish')

            else:
                print(var_counterStr, var_url, ' cant connect')

        except Exception as e:
            print(var_counterStr, var_url, ' data extract error:  ', e)

            df_hdlnNew.loc[df_hdlnNew.news_url == var_url, 'news_text' ] = pd.NaT

    try:
        uploadGithubBackup(df_hdlnNew, var_tblName)
    except Exception as e:
        print('\n\n', var_tblName, 'export to csv with error:  ', e)

    print('\n\n', getDataInfo(var_tblName ) )


else: 
    print('\n\n', var_tblName, ' headline extract failed!!')

