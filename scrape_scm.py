from news_tools import *


var_tblName = 'news_scm'


headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537'}

# # headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537'}
var_baseUrl = 'https://www.scmp.com/' # 2024/01/01'
var_baseUrl_1 = 'https://www.scmp.com/business'
var_baseUrl_2 = 'https://www.scmp.com/economy'

df_hdlnNew = pd.DataFrame(columns = ['news_url', 'news_title'])



# Downloading Headline
print('\n\n', var_tblName, 'downloading headlines')
response = requests.get(var_baseUrl, headers=headers)

if response.status_code == 200:
    soup = BeautifulSoup(response.content, 'html.parser')

    for var_element in soup.find_all('a'):
        if var_element.find('span', attrs={'data-qa': 'ContentHeadline-Headline'}):
            try:
                var_title = var_element.find('span', attrs={'data-qa': 'ContentHeadline-Headline'}).text 
                var_url = var_element['href']
                if var_baseUrl not in var_url: var_url = var_baseUrl[ : -1] + var_url

                var_url = var_url.split('?')[0]

                df_var = pd.DataFrame([[var_url, var_title ]], columns = df_hdlnNew.columns)
                
                if (var_url not in df_hdlnNew.news_url.unique()) and ('/video/' not in var_url):
                    df_hdlnNew = pd.concat([df_hdlnNew, df_var], ignore_index = True )

                # Upload Headline to SQL
                if news_connectSQL.checkNewsUrlInTbl(var_tblName, var_url):
                    df_var['created_at'] = datetime.now()
                    news_connectSQL.uploadSQLQuery(df_var, var_tblName)

            except:
                pass
    print(var_tblName, 'headline download finished \n\n')


# Downloading Sub Headline

for var_url_1 in [var_baseUrl_1, var_baseUrl_2]:

    print('\n\n', var_url_1, 'downloading Sub headlines')

    response = requests.get(var_baseUrl_1, headers=headers)

    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')

        for var_element in soup.select('h2[class*="article__title"]'):

            try:
                var_title = var_element.text
                var_url = var_element.find('a')['href']
                if var_baseUrl not in var_url: var_url = var_baseUrl[ : -1] + var_url

                var_url = var_url.split('?')[0]

                df_var = pd.DataFrame([[var_url, var_title ]], columns = df_hdlnNew.columns)

                if (var_url not in df_hdlnNew.news_url.unique()) and ('/video/' not in var_url):
                    df_hdlnNew = pd.concat([df_hdlnNew, df_var], ignore_index = True )

                # Upload Headline to SQL
                if news_connectSQL.checkNewsUrlInTbl(var_tblName, var_url):
                    df_var['created_at'] = datetime.now()
                    news_connectSQL.uploadSQLQuery(df_var, var_tblName)


            except Exception as e:
                print(var_url, var_title)
                print(var_element, ' with error:  ', e)
        print(var_url_1, ' Sub headline download finished \n\n')

    else:
        print(var_url_1, ' Sub not access \n\n')


# Download Text Data

df_sql = news_connectSQL.downloadSQLQuery(var_tblName, date_col = 'created_at', date_from = ( (datetime.today() - timedelta(days = 3)).strftime('%Y-%m-%d') ))

df_sql = df_sql[ 
    (pd.isna(df_sql.news_text) | (df_sql.news_text.isna() ) | (df_sql.news_text == '' )) &
    (~df_sql.news_url.str.contains('/video/') ) 
    ]

list_urlDwld = df_sql.news_url.unique()
print('\n\ntext download for ', var_tblName, str(len(list_urlDwld) ),  ' to download start !!')
var_i = 0
var_len = len(df_hdlnNew)

for var_url in list_urlDwld:
    var_i += 1
    var_counterStr = str(var_i) + ' of ' + str(var_len)
    try:
        response = requests.get(var_url, headers=headers)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')

            var_output = ''
            try:
                for var_element in soup.find_all('p'):
                    try:
                        var_output = var_output + var_element.text + ' '
                    except:
                        pass
                    
            except:
                pass


            var_time = pd.NaT
            try:
                for var_element in soup.find_all('time'):
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
    except Exception as e:
        print(var_url, 'text extract failed')
        print


try:
    uploadGithubBackup(df_hdlnNew, var_tblName)
except Exception as e:
    print('\n\n', var_tblName, 'export to csv with error:  ', e)

print('\n\n', getDataInfo(var_tblName ) )
