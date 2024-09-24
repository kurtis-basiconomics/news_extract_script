from news_tools import *

import news_articlesFunc

print('\n\n')
var_outputStr = 'see category and summary run analysis below:\n'
var_outputStr += f"""****** category and summary analysis start on {datetime.now().strftime('%Y-%m-%d %H:%M')}******\n"""

list_newsSource = news_connectSQL.downloadSQLQuery('news_source_list' ).table_name.tolist() #['news_wsj', 'news_blm', 'news_mkw', 'news_ft'] #  

list_actualSrc = list()
list_scrapeOk = list()
for var_tblName in list_newsSource:
    var_scrapeOk = news_articlesFunc.runCtgrAndSummFunc(var_tblName)
    if var_scrapeOk > 0:
        list_actualSrc.append(var_tblName)
        list_scrapeOk.append(var_scrapeOk)
var_outputStr += f"""\n****** category and summary analysis END on {datetime.now().strftime('%Y-%m-%d %H:%M')}******\n\n"""

if len(list_scrapeOk) > 0:
    for var_scrapeOk, var_actualSrc in zip(list_scrapeOk, list_actualSrc):
        var_outputStr += f"""table {var_actualSrc} with {str(var_scrapeOk)} successful category \n""" 
else: 
    var_outputStr += ' no successful category'


var_outputStr += '\n\n\n'
var_outputStr += f"""****** split text analysis start on {datetime.now().strftime('%Y-%m-%d %H:%M')}******\n\n"""
print('\n\n start category split\n')
var_outputStrCtgrSplit = news_articlesFunc.runCtgrSplitText()
var_outputStr += var_outputStrCtgrSplit
var_outputStr += f"""\n****** split text analysis END on {datetime.now().strftime('%Y-%m-%d %H:%M')}******\n\n"""
print('\n\n category split END')

var_title = f"""Report on Category Summary on {datetime.now().strftime('%Y-%m-%d')}"""
sendEmail(var_receiverEmail, var_title, var_outputStr )
print(var_outputStr)
