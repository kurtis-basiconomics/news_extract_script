from news_tools import *

import news_articlesFunc


list_newsSource = news_connectSQL.downloadSQLQuery('news_source_list' ).table_name.tolist() #['news_wsj', 'news_blm', 'news_mkw', 'news_ft'] #  

list_actualSrc = list()
list_scrapeOk = list()
for var_tblName in list_newsSource:
    var_scrapeOk = news_articlesFunc.runCtgrAndSummFunc(var_tblName)
    if var_scrapeOk > 0:
        list_actualSrc.append(var_tblName)
        list_scrapeOk.append(var_scrapeOk)

print('\n\n')
var_outputStr = 'see chatgpt run analysis below:\n'
if len(list_scrapeOk) > 0:
    for var_scrapeOk, var_actualSrc in zip(list_scrapeOk, list_actualSrc):
        var_outputStr += f"""table {var_actualSrc} with {str(var_scrapeOk)} successful scrape \n""" 
else: 
    var_outputStr += ' no successful scrape'

var_title = f"""Report on chatgptRun on {datetime.now().strftime('%Y-%m-%d')}"""
sendEmail(var_receiverEmail, var_title, var_outputStr )
print(var_outputStr)


print('\n\n start category split\n')
news_articlesFunc.runCtgrSplitText()
print('\n\n category split END')
