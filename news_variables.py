# File Paths =============================================

var_githubRepo_scrape = 'kurtis-basiconomics/backup_news_scrape'

path_newsDataFldr = 'news_data/'

path_newsAtclFldr = path_newsDataFldr + 'news_articles/'

path_newsWSJHeadlines = path_newsDataFldr + 'rt_newsScrapeWSJHeadlines.csv'

path_newsSummary = path_newsDataFldr + 'rt_newsScrapeSummary.csv'
path_profileSummary = path_newsDataFldr + 'rt_newsProfileSummary.csv'
path_newsWSJSampleText = path_newsDataFldr + 'rt_newsScrapeSampleText.csv'

path_newsCountryList = path_newsDataFldr + 'news_countryList.csv'

path_failedSumm = path_newsDataFldr + 'failed_data/' + 'rt_newsSummaryFailed.csv'
path_failedPrfl = path_newsDataFldr + 'failed_data/' + 'rt_newsProfileFailed.csv'

path_failedUpload = path_newsDataFldr + 'upload_failed/' 


path_apiKeysFldr = 'api_keys/'

path_githubKey = path_apiKeysFldr + 'github_token_news_backup.txt'

path_emailSender = path_apiKeysFldr + 'kgchua_sender.txt'

path_openaiKey_120423 = path_apiKeysFldr + 'api_key_120423.txt'
path_openaiKey_100423 = path_apiKeysFldr + 'api_key_100423.txt'
path_openaiKey_country_code = path_apiKeysFldr + 'openai_apiKey_country_code.txt'
path_openaiKey_news_category = path_apiKeysFldr + 'openai_apiKey_news_category.txt'
path_openaiKey_news_summary = path_apiKeysFldr + 'openai_apiKey_news_summary.txt'
path_openaiKey_news_articleId = path_apiKeysFldr + 'openai_apiKey_news_articleId.txt'
path_openaiKey_news_profile = path_apiKeysFldr + 'openai_apiKey_news_profile.txt'


path_zenrowssKeykcgmail = path_apiKeysFldr + 'zenrows_kurtischua_gmail_com.txt'
path_zenrowssKeykcbasic= path_apiKeysFldr + 'zenrows_kurtis_basiconomics_com.txt'
path_zenrowssKeykctide= path_apiKeysFldr + 'zenrows_kurtis_tide_co.txt'
path_zenrowssKey01kgmail = path_apiKeysFldr + 'zenrows_01kchua_gmail_com.txt'
path_zenrowssKey03kgmail = path_apiKeysFldr + 'zenrows_03kchua_gmail_com.txt'
path_zenrowssKey04kgmail = path_apiKeysFldr + 'zenrows_04kchua_gmail_com.txt'
path_zenrowssKey05kgmail = path_apiKeysFldr + 'zenrows_05kchua_gmail_com.txt'

path_zenrowsKeyMaster = path_zenrowssKey05kgmail

# ********************************* NEWS SCRAPE VARIABLES BELOW *********************************
list_exclNewsTitles = ['Charts that Matter. ',
       'Interview. ',
       'The Big Read. ', 
       ' opinion content.\xa0',
       'FT Magazine. ',
       'Lunch with the FT. ',
       ' opinion content.\xa0On Wall Street. ',
       ' opinion content.\xa0The Long View. ',
       ' opinion content.\xa0The FT View. ',
       ' opinion content.\xa0Instant Insight. ', 'Political Fix. ',
       'FT Weekend podcast. ', 
       'Unhedged podcast. ', 
       'Rachman Review. ',
       'HTSI. ', 
       ' video content.\xa0',
       'FT Investigations. ',
       'ETF Hub. ',
       '‘Start-up Nation’ goes to war',
       'opinion content. Lex.',
       'Explainer.	',
       'FT Alphaville. '] 


path_wsjLogin = 'https://www.wsj.com/client/login?target=https%3A%2F%2Fwww.wsj.com%2Freal-estate%2Famericas-biggest-landlords-cant-find-houses-to-buy-either-ea893213'


list_mainTopics = ['.com/articles/', '.com/business/', '.com/economy/', '.com/us-news/', '.com/real-estate/', '.com/tech/', '.com/real-estate/', '.com/finance/','.com/world/asia/','.com/world/china/','.com/world/europe/', 'www.barrons.com', 'www.marketwatch.com']

# summary of columns for order list summary
list_orderListSummCol = ['current_price', 'lastBuySgnl', 'currBuySgnl', 'lastStd', 'lastMA', 'nextEarnDate', 'extractDate', 'distanceRSI-below1','distanceMA-above1', 'RCT_negNews', 'AVE_negNews', 'rsi', 'days_to_earnings']


# File Paths end =========================================
# ========================================================

