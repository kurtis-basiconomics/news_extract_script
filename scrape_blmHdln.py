from news_tools import *

var_tblName = 'news_blm'



var_baseUrl = 'https://www.bloomberg.com/' # 2024/01/01'

list_blmSubHdln = ['https://www.bloomberg.com/deals', 
            'https://www.bloomberg.com/markets/fixed-income',    
            'https://www.bloomberg.com/economics', 
            'https://www.bloomberg.com/economics/central-banks',
            'https://www.bloomberg.com/economics/indicators',
            'https://www.bloomberg.com/economics/jobs',
            'https://www.bloomberg.com/economics/trade',
            'https://www.bloomberg.com/economics/inflation-and-prices']



df_hdlnNew = pd.DataFrame(columns = ['news_title', 'news_url', 'headline_date'])



print('\n\n', var_tblName, 'downloading headlines')

response = client.get(var_baseUrl)

if response.status_code == 200:
    soup = BeautifulSoup(response.content, 'html.parser')

    # Define the regex pattern to match class names starting with "StoryBlock_storyLink"
    pattern_soup = re.compile(r'StoryBlock_storyLink.*')

    # Find all <a> elements where class matches the pattern
    matched_elements = [a for a in soup.find_all('a', class_=True) if any(pattern_soup.match(cls) for cls in a['class'])]

    # Iterate over each found <div> to extract the text within the <a> element
    for a_element in matched_elements:
        try:
            var_title = a_element.text  # Text content of the <a> element
            if var_title != '':
                var_href = a_element.get('href')  # Href attribute of the <a> element
                var_url = var_baseUrl[ : -1] + var_href

                var_url = var_url.split('?')[0]

                if '/news/thp' in var_href:
                    var_hdlnDt = var_href.split('/')[4]
                else:
                    var_hdlnDt = var_href.split('/')[3]

                if 'opinion content' not in var_title:
                    df_var = pd.DataFrame([[var_title, var_url, var_hdlnDt]], columns = df_hdlnNew.columns)

                    if (var_url not in df_hdlnNew.news_url.unique()):
                        df_hdlnNew = pd.concat([df_hdlnNew, df_var], ignore_index = True )

                    # Upload Headline to SQL
                    if news_connectSQL.checkNewsUrlInTbl(var_tblName, var_url):
                        df_var['created_at'] = datetime.now()
                        news_connectSQL.uploadSQLQuery(df_var, var_tblName)
                        print(var_baseUrl, var_url, var_title)
        
        except Exception as e:
            print(var_url, ' with error:  ', e)

    print(var_tblName, ' Sub headline download finished \n\n')

else:
    print(var_baseUrl, 'Bloomberg Headline not extracted with error:  ', str(response.status_code) )




# Extract Sub Bloomberg Sites
print('\n\n', var_tblName, ' Sub downloading headlines')

for var_baseUrl_sub in list_blmSubHdln:

    print('\n\n', var_baseUrl_sub, ' Sub downloading headlines')
    
    response = client.get(var_baseUrl_sub)

    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')

        headline_elements = soup.find_all(attrs={"data-component": "headline"})

        for a_element in headline_elements:

            try:
                var_url = a_element.find('a')['href']
                var_url = var_url.split('?')[0]

                if ('/authors/' not in var_url) and ("https://www.bloomberg.com/economics/indicators" not in var_url) and ("https://www.bloomberg.com/economics/central-banks"  not in var_url) and ("https://www.bloomberg.com/economics/jobs" not in var_url) and ("https://www.bloomberg.com/economics/trade" not in var_url) and ("https://www.bloomberg.com/economics/tax-and-spend" not in var_url) and ("https://www.bloomberg.com/economics/inflation-and-prices" not in var_url):
                    var_title = a_element.text
                    if ('/features/' not in var_url) and ('/graphics/' not in var_url):
                        var_hdlnDt = var_url.split('/')[3].replace('/', '-')
                    else:
                        var_hdlnDt = ''
                    var_url = var_baseUrl[ : -1] + var_url

                    df_var = pd.DataFrame([[var_title, var_url, var_hdlnDt]], columns = df_hdlnNew.columns)
                    
                    if (var_url not in df_hdlnNew.news_url.unique()):
                        df_hdlnNew = pd.concat([df_hdlnNew, df_var], ignore_index = True )

                    # Upload Headline to SQL
                    if news_connectSQL.checkNewsUrlInTbl(var_tblName, var_url):
                        df_var['created_at'] = datetime.now()
                        news_connectSQL.uploadSQLQuery(df_var, var_tblName)
                        print(var_baseUrl_sub, var_url, var_title)
            except:
                print(var_baseUrl_sub, a_element)
    print(var_baseUrl_sub, ' Sub headline download finished \n\n')

else:
    print(var_baseUrl_sub, 'Bloomberg SUB Headline not extracted with error:  ', str(response.status_code) )



try:
    uploadGithubBackup(df_hdlnNew, var_tblName, hdln_backup = 'y')
except Exception as e:
    print('\n\n', var_tblName, 'export to csv with error:  ', e)

print('\n\n', getDataInfo(var_tblName, date_from = (datetime.today() - timedelta(days = 5)).strftime('%Y-%m-%d') ) )
