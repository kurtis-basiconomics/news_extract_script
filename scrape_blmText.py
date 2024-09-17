from news_tools import *

var_tblName = 'news_blm'

# Function to recursively find all "type": "text" key-value pairs
def find_all_text_types(data):
    results = []
    if isinstance(data, dict):
        if data.get('type') == 'text':
            results.append(data.get('value'))
        for key, value in data.items():
            results.extend(find_all_text_types(value))
    elif isinstance(data, list):
        for item in data:
            results.extend(find_all_text_types(item))
    return results


def getTextFromScript(script_tags):

    # Iterate over script tags to find JSON data and extract text values
    text_values = []
    for script_tag in script_tags:
        try:
            json_data = json.loads(script_tag.string)
            text_values.extend(find_all_text_types(json_data))
        except (json.JSONDecodeError, TypeError):
            continue

    var_fullText = ''                
    for text_value in text_values:
        try:
            var_paragraph = text_value
            if var_paragraph != '':
                var_fullText += var_paragraph
        except:
            pass
    
    return var_fullText;



var_upldOk = 'y'


list_strExcl = news_connectSQL.downloadSQLQuery('news_exclusions', check_value = var_tblName.replace('news_', '') , check_col = 'news_source' ).table_string.tolist()


# driver = webdriver.Chrome(service=s, options=options)
df_sql = news_connectSQL.downloadSQLQuery(var_tblName, date_col = 'created_at', date_from = '2024-07-01')

mask = df_sql['news_url'].apply(lambda x: not any(substring in x for substring in list_strExcl))

df_sql = df_sql[ 
    (pd.isna(df_sql.news_text) | (df_sql.news_text.isna() )) & 
    mask
    ]
df_sql.sort_values(by = 'headline_date' , ascending = False, ignore_index = True, inplace = True)

list_urlDwld = df_sql.news_url.unique()

# for var_url in list_urlDwld:
#     print(var_url)

var_len = len(list_urlDwld)
var_i = 0


print(str(len(list_urlDwld)), ' articles to scrape' )

for var_url in list_urlDwld:
    var_i += 1
    var_counterStr = f"""{str(var_i)} of {str(var_len)} """

    var_outputStr = var_counterStr + ' ' + var_url + ' '

    try:
        var_rspn, var_outputStrZenrows = getZenrowsResponse(var_url, 0)

        if var_rspn.status_code == 200:
            
            # Assuming var_rspn.content contains the HTML content
            soup = BeautifulSoup(var_rspn.content, 'html.parser')

            # get article text
            var_fullText = ''
            try:
                script_tags = soup.find_all('script')
                var_fullText = getTextFromScript(script_tags)
                if (var_fullText != '') and (var_fullText is not None):
                    var_outputStr += ' text extract success '
                    if (var_upldOk == 'y') or (var_upldOk == 'yes'):
                        try:
                            news_connectSQL.replaceSQLQuery(var_tblName, 'news_url', var_url, 'news_text', var_fullText, upload_to_sql = var_upldOk)
                            var_outputStr += 'PRINTED'
                        except:
                            var_outputStr += 'UPLOAD FAILED'
                else:
                    var_outputStr += ' text extract failed with empty text '
            except:
                var_outputStr += ' text extract FAILED '
                pass
            var_outputStr += '; '

            var_dt = pd.NaT
            try:
                var_dt = pd.to_datetime( soup.find_all('time')[0]['datetime'] )
                if (var_dt != pd.NaT):
                    var_outputStr += ' date extract success; '
                    if (var_upldOk == 'y') or (var_upldOk == 'yes'):
                        try:
                            news_connectSQL.replaceSQLQuery(var_tblName, 'news_url', var_url, 'headline_date', var_dt, upload_to_sql = var_upldOk)
                            var_outputStr += 'PRINTED'
                        except:
                            var_outputStr += 'UPLOAD FAILED '
                else:
                    var_outputStr += ' date extract failed with nan '                            
            except:
                var_outputStr += ' date extract FAILED '
                pass

        else:
            var_outputStr += ' not getting response 200'
    
    except:
        var_outputStr += ' error on process'

    print(var_outputStr)

    