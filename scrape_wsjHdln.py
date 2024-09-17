from news_tools import *

var_tblName = 'news_wsj'

list_cols = ['news_title', 'news_url', 'news_id', 'headline_date']


headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537'}

var_baseUrl = 'https://www.wsj.com/news/archive/' # 2024/01/01'

var_rootUrl = 'https://www.wsj.com'

list_wsjSumHdln = ['https://www.wsj.com/business', 
            'https://www.wsj.com/finance',  
            'https://www.wsj.com/economy', 
            'https://www.wsj.com/tech'
           ]

# # df_hdln = pd.DataFrame(columns = ['news_title', 'news_url', 'news_id', 'headline_date'])
# df_hdln = pd.read_csv(rt_variables_path.path_newsWSJHeadlines, index_col = 0)
# df_hdln['headline_date'] = pd.to_datetime(df_hdln['headline_date']).dt.date

list_dt = [datetime.today().strftime('%Y-%m-%d')]

print('\n\n', var_tblName, 'downloading headlines')
df_hdlnNew = pd.DataFrame(columns = list_cols)

var_x = 0

for var_dt in list_dt:

    var_x = var_x + 1
    
    var_dt = pd.to_datetime(var_dt).date()

    URL = var_baseUrl + var_dt.strftime('%Y/%m/%d')
    # print(str(var_x), ' of ', str(len(list_dt)) , URL)
    try:
        # response = requests.get(URL, headers=headers)
        response, var_outputStrZenrows = getZenrowsResponse(var_baseUrl, 0, use_chatgpt = 'zenrows-api')

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')

            # Find all elements containing 'WSJTheme--story' in their class
            articles = soup.find_all(lambda tag: tag.name == 'article' and 'WSJTheme--story' in ' '.join(tag.get('class', [])))
            print(articles)

            # Extract data
            news_items = []
            for article in articles:
                try:
                    # Find headline element containing 'WSJTheme--headline' in its class
                    headline_element = article.find(lambda tag: tag.name == 'span' and 'WSJTheme--headline' in ' '.join(tag.get('class', [])))
                    url_element = article.find('a')

                    var_title = headline_element.text
                    var_url = url_element['href']
                    var_dataId = article.get('data-id', None)

                    var_url = var_url.split('?')[0]
                    
                    df_var = pd.DataFrame([[var_title, var_url, var_dataId, var_dt.strftime('%Y-%m-%d')]], columns = list_cols)
                    if var_url not in df_hdlnNew.news_url.unique():
                        df_hdlnNew = pd.concat([df_hdlnNew, df_var], ignore_index=True)

                    # Upload Headline to SQL
                    if news_connectSQL.checkNewsUrlInTbl(var_tblName, var_url):
                        df_var['created_at'] = datetime.now()
                        news_connectSQL.uploadSQLQuery(df_var, var_tblName)  
                        print(var_baseUrl, var_url, var_title)      

                    else:
                        print("Missing headline or URL in an article.")

                except:
                    pass
                        
            print(str(var_x), ' of ', str(len(list_dt)) , ' success ', URL)

            print(var_tblName, 'headline download finished \n\n')

        else:
            print('wsj headlines cant connect')
            pass
    except Exception as e:
        print(str(var_x), ' of ', str(len(list_dt)), ' ', var_dt.strftime('%Y/%m/%d'), ' with error:  ', e)




# Downloading Sub Headline


for var_baseUrl_sub in list_wsjSumHdln:
    response, var_outputStrZenrows = getZenrowsResponse(var_baseUrl_sub, 0, use_chatgpt = 'zenrows-api')
    # response = client.get(var_baseUrl)

    if response.status_code == 200:
        print('\n\n', var_baseUrl_sub, var_tblName, 'downloading Sub headlines')
        soup = BeautifulSoup(response.content, 'html.parser')
        headline_elements = soup.select('[class*="-CardLink"]')

        print(var_baseUrl_sub, len(headline_elements) )

        for a_element in headline_elements:
            try:
                var_url = a_element['href']
                var_title = a_element.text

                if 'livecoverage/' in var_url:
                    var_url = var_rootUrl + var_url

                var_url = var_url.split('?')[0]

                if var_url not in df_hdlnNew.news_url.unique():
                    df_var = pd.DataFrame([[var_url, var_title]] , columns = ['news_url', 'news_title'])
                    df_hdlnNew = pd.concat([df_hdlnNew, df_var], ignore_index = True)

                # Upload Headline to SQL
                if news_connectSQL.checkNewsUrlInTbl(var_tblName, var_url):
                    df_var['created_at'] = datetime.now()
                    news_connectSQL.uploadSQLQuery(df_var, var_tblName) 
                    print(var_baseUrl_sub, var_url, var_title) 

            except Exception as e:
                print(a_element, ' with error:  ', e)
    
        print(var_baseUrl_sub, var_tblName, ' Subheadline download finished \n\n')

    else:
        print(var_baseUrl_sub, 'not 200')



df_hdlnNew.drop_duplicates(subset = ['news_url'], keep = 'first', inplace = True, ignore_index = True )
# # df_hdlnNew.news_id.fillna('', inplace = True)
# df_hdlnNew.headline_date.fillna('', inplace = True)


try:
    uploadGithubBackup(df_hdlnNew, var_tblName, hdln_backup = 'y')
except Exception as e:
    print('\n\n', var_tblName, 'export to csv with error:  ', e)

print('\n\n', getDataInfo(var_tblName, date_from = (datetime.today() - timedelta(days = 5)).strftime('%Y-%m-%d') ) )
