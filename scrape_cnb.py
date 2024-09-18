from news_tools import *

var_tblName = 'news_cnb'

list_exclUrl = ['/pro/', '/investingclub/']

df_hdlnNew = pd.DataFrame(columns = ['news_url', 'news_title'])

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537'}

var_baseUrl = 'https://www.cnbc.com/' # 2024/01/01'



# Download Headlines 

print('\n\n', var_tblName, 'downloading headlines')
response = requests.get(var_baseUrl, headers=headers)
# response = client.get(var_baseUrl)

if response.status_code == 200:
    soup = BeautifulSoup(response.content, 'html.parser')

    list_soup_1 = soup.find_all('h2', class_="FeaturedCard-packagedCardTitle")
    list_soup_2 = soup.find_all('div', class_= 'RiverHeadline-headline RiverHeadline-hasThumbnail')
    list_soup_3 = soup.find_all('div', class_= 'Card-titleContainer')


    for list_var in [list_soup_1, list_soup_2, list_soup_3]:
        for var_element in list_var:
            var_element = var_element.find('a')

            var_title = var_element.text
            var_url = var_element['href']

            if var_url not in list_exclUrl:

                df_var = pd.DataFrame([[var_url, var_title ]], columns = df_hdlnNew.columns)
                
                if (var_url not in df_hdlnNew.news_url.unique()):
                    df_hdlnNew = pd.concat([df_hdlnNew, df_var], ignore_index = True )

                # Upload Headline to SQL
                if news_connectSQL.checkNewsUrlInTbl(var_tblName, var_url):
                    df_var['created_at'] = datetime.now()
                    news_connectSQL.uploadSQLQuery(df_var, var_tblName)
                    print(var_url, var_title)

    print(var_tblName, 'headline download finished \n\n')



    # Downloading News Text

    df_sql = news_connectSQL.downloadSQLQuery(var_tblName, date_col = 'created_at', date_from = ( (datetime.today() - timedelta(days = 3)).strftime('%Y-%m-%d') ))

    df_sql = df_sql[ 
        (pd.isna(df_sql.news_text) | (df_sql.news_text.isna() ) | (df_sql.news_text == '' )) &
        (~df_sql.news_url.str.contains('/video/') ) 
        ]

    list_urlDwld = df_sql.news_url.unique()

    print('\n\ntext download for ', var_tblName, ' start !!')
    var_i = 0
    var_len = len(list_urlDwld)

    for var_url in list_urlDwld:
        var_i += 1
        var_counterStr = str(var_i) + ' of ' + str(var_len)
        try:
            response = requests.get(var_url, headers=headers)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')

                var_output = ''
                try:
                    list_element = soup.find_all('div', class_ = 'group')
                    for list_element_1 in list_element:

                        if list_element_1.find_all('p'):
                            list_p = list_element_1.find_all('p')

                            for var_element in list_p:
                                var_output = var_output + var_element.text + ' '
                except:
                    pass

                var_time = pd.NaT
                try:
                    list_time = soup.find_all('time')
                    for var_element in list_time:
                        if pd.isna(var_time):
                            try:
                                var_time = var_element.get('datetime')
                                
                            except:
                                pass
                except:
                    pass

                try:
                    news_connectSQL.replaceSQLQuery(var_tblName, 'news_url', var_url, ['news_text', 'headline_date'], [str(var_output), pd.to_datetime(var_time)])
                except Exception as e:
                    print(var_url, ' upload to databse with error:  ', e)

                df_hdlnNew.loc[df_hdlnNew.news_url == var_url, 'headline_date'] = pd.to_datetime(var_time).date()
                df_hdlnNew.loc[df_hdlnNew.news_url == var_url, 'news_text'] = var_output

                print(var_url, var_counterStr, ' success')

            else:
                print(var_url, 'text extract failed')



        except Exception as e:
            print(var_url, var_counterStr, ' failed with error:  ', e)

    try:
        uploadGithubBackup(df_hdlnNew, var_tblName)
    except Exception as e:
        print('\n\n', var_tblName, 'export to csv with error:  ', e)

    print('\n\n', getDataInfo(var_tblName) )


else: 
    print('\n\n', var_tblName, ' headline extract failed!!')
