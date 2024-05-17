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




# # **************************** DOWNLOAD NEWS ****************************

# # Create a Service object
# s=Service(chrome_driver_path)
# # Create an instance of ChromeOptions
# options = webdriver.ChromeOptions()

# # Add the user-data-dir and profile-directory options
# options.add_argument(f"user-data-dir={user_data_dir}")
# options.add_argument(f"profile-directory={profile_directory}")

# options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.3')
# options.add_experimental_option("excludeSwitches", ["enable-automation"])
# options.add_experimental_option('useAutomationExtension', False)
# options.add_argument('--disable-blink-features=AutomationControlled')

# driver = webdriver.Chrome(service=s, options=options)


# # Download Text Data

# df_sql = news_connectSQL.downloadSQLQuery(var_tblName, date_col = 'created_at', date_from = ( (datetime.today() - timedelta(days = 3)).strftime('%Y-%m-%d') ))

# df_sql = df_sql[ 
#     (pd.isna(df_sql.news_text) | (df_sql.news_text.isna() ) | (df_sql.news_text == '' )) &
#     (~df_sql.news_url.str.contains('/video/') ) 
#     ]

# list_urlDwld = df_sql.news_url.unique()

# print('\n\ntext download for ', var_tblName, str(len(list_urlDwld) ),  ' to download start !!')

# df_hdlnNew = pd.DataFrame(list_urlDwld, columns = ['news_url'])


# var_i = 0
# var_len = len(list_urlDwld)

# for var_url in list_urlDwld:
#     var_i += 1
#     var_counterStr = str(var_i) + ' of ' + str(var_len)

#     if 'News updates from' not in var_url:

#         try: 
#             driver.get(var_url)

#             try:   
#                 xpath = '//*[@id="site-content"]/div[3]/div[1]/div/time'
#                 # Wait for the element to be present and retrieve it
#                 wait = WebDriverWait(driver, 10)
#                 var_element = wait.until(EC.presence_of_element_located((By.XPATH, xpath)))
#                 var_element = var_element.text
#                 if 'YESTERDAY' in var_element: var_element = (datetime.today() - timedelta(days = 1) ).strftime('%B %d %Y')
#                 if ('TODAY' in var_element) or ('AGO' in var_element): var_element = (datetime.today() ).tdy.strftime('%B %d %Y')

                
#                 try:
#                     var_element = convert_to_datetime(var_element)
#                 except:
#                     var_element = '' 
#                     pass
#                 df_hdlnNew.loc[df_hdlnNew.news_url == var_url, 'headline_date'] = var_element
#             except:
#                 var_element = '' 


#             # get article text
#             article_text = ""
#             index = 1
#             while True:
#                 try:
#                     # Construct the XPath with the current index
#                     xpath = f'//*[@id="article-body"]/p[{index}]'
#                     paragraph = driver.find_element(By.XPATH, xpath)
#                     article_text += paragraph.text + "\n\n"
#                     index += 1
#                 except NoSuchElementException:
#                     # Exit the loop if no paragraph is found with the current index
#                     break
#             df_hdlnNew.loc[df_hdlnNew.news_url == var_url, 'news_text'] = article_text

#             # Extract list of topics
#             article_topics = []
#             index = 1
#             while True:
#                 try:
#                     xpath = f'//*[@id="site-content"]/div[8]/div/div/ul/li[{index}]/a'
#                     var_topic = driver.find_element(By.XPATH, xpath)
#                     article_topics.append(var_topic.text)
#                     index += 1
#                 except NoSuchElementException:
#                     break
#             df_hdlnNew.loc[df_hdlnNew.news_url == var_url, 'news_topics'] = str(article_topics)

#             # Extract article title
#             try:
#                 var_title = driver.find_element(By.TAG_NAME, 'h1').text
#             except :
#                 var_title = ''
#             df_hdlnNew.loc[df_hdlnNew.news_url == var_url, 'news_title'] = str(var_title)



#             try:
#                 news_connectSQL.replaceSQLQuery(var_tblName, 'news_url', var_url, ['news_text', 'headline_date', 'news_topics', 'news_title'], [str(article_text), pd.to_datetime(var_element), str(article_topics), str(var_title)])
#                 print(var_counterStr, var_url, ' upload complete')
#             except Exception as e:
#                 print(var_url, ' upload to databse with error:  ', e)

#         except Exception as e:
#             print(var_counterStr, var_url, ' error:  ', e)

#     else:
#         print(var_counterStr, var_url, ' not extract')




try:
    uploadGithubBackup(df_hdlnNew, var_tblName)
except Exception as e:
    print('\n\n', var_tblName, 'export to csv with error:  ', e)

print('\n\n', getDataInfo(var_tblName, date_from = (datetime.today() - timedelta(days = 5)).strftime('%Y-%m-%d') ) )

