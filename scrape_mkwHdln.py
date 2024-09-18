from news_tools import *

var_tblName = 'news_mkw'

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


try:
    uploadGithubBackup(df_hdlnNew, var_tblName, hdln_backup = 'y')
except Exception as e:
    print('\n\n', var_tblName, 'export to csv with error:  ', e)

print('\n\n', getDataInfo(var_tblName) )
