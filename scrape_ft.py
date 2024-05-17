from news_tools import *

var_tblName = 'news_ft'


df_hdlnNew = pd.DataFrame(columns = ['news_title', 'news_url', 'news_text'])


# Set the path of the user data directory and the profile directory
user_data_dir = r"C:\Users\Kurtis\AppData\Local\Google\Chrome\User Data"
profile_directory = "Profile 1"  # us Profile 1 for kgchua.com

chrome_driver_path = "C:/Users/Kurtis/chromedriver_win32/chromedriver.exe"

var_baseUrl = 'https://www.ft.com/'  # Replace this with the actual URL
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537'}


list_url = ['https://www.ft.com/', 'https://www.ft.com/world-uk', 'https://www.ft.com/companies', 'https://www.ft.com/technology', 'https://www.ft.com/markets']

list_cols = ['news_title', 'news_url']


for var_baseUrl_sub in list_url:

    print(var_baseUrl_sub, ' Sub downloading headlines')

    response = requests.get(var_baseUrl_sub, headers=headers)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find all span tags with a class that starts with 'NEWSTheme--headlineText--'
        titles = soup.find_all('a', attrs={'data-trackable': 'heading-link'})

        df_hdlnNew = pd.DataFrame(columns = list_cols)

        for headline in titles:
            try:
                # Extracting the URL
                var_href = headline['href']
                var_url = 'https://www.ft.com' + var_href
                
                # Extracting the text inside the <span> tag within the <a> tag
                var_title = headline.find('span').text if headline.find('span') else None
                
                if (' opinion content' not in var_url) and ('News updates from' not in var_url):

                    df_var = pd.DataFrame([[var_title, var_url]], columns = df_hdlnNew.columns)
                        
                    # if (var_url not in df_hdlnNew.news_url.unique()):
                    #     #df_hdlnNew = pd.concat([df_hdlnNew, df_var], ignore_index = True )

                    # Upload Headline to SQL
                    if news_connectSQL.checkNewsUrlInTbl(var_tblName, var_url):
                        df_var['created_at'] = datetime.now()
                        news_connectSQL.uploadSQLQuery(df_var, var_tblName)
                        print(var_baseUrl_sub, var_url, var_title)
                    
            except Exception as e:
                print(var_baseUrl_sub, var_url, ' with error:  ', e)

    else:
        print(var_baseUrl_sub, var_tblName,  ' SUB Headline not extracted with error:  ', str(response.status_code) )




try:
    uploadGithubBackup(df_hdlnNew, var_tblName)
except Exception as e:
    print('\n\n', var_tblName, 'export to csv with error:  ', e)

print('\n\n', getDataInfo(var_tblName, date_from = (datetime.today() - timedelta(days = 5)).strftime('%Y-%m-%d') ) )

