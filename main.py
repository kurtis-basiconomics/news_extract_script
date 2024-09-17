import logging
from news_tools import getDataInfoSumm, getSracpeEmailRprtText, sendEmail


var_receiverEmail = 'kurtis@kgchua.com'

# Configure logging
logging.basicConfig(filename='script.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    logging.info("Script started")

    try:
        import scrape_blmHdln
        print('scrape Bloomberg complete')
    except Exception as e:
        print('Bloomberg scrape with error:  ', e)
        var_text = getSracpeEmailRprtText('scrape_blmHdln')
        sendEmail(var_receiverEmail, var_text, var_text )


    try:
        import scrape_wsjHdln
        print('scrape WSJ complete')
    except Exception as e:
        print('WSJ scrape with error:  ', e)
        var_text = getSracpeEmailRprtText('scrape_wsjHdln')
        sendEmail(var_receiverEmail, var_text, var_text )

    try:
        import scrape_mkwHdln
        print('scrape Marketwatch complete')
    except Exception as e:
        print('Marketwatch scrape with error:  ', e)
        var_text = getSracpeEmailRprtText('scrape_mkwHdln')
        sendEmail(var_receiverEmail, var_text, var_text )
        
    try:
        import scrape_cnb
        print('scrape CNBC complete')
    except Exception as e:
        print('CNBC scrape with error:  ', e)
        var_text = getSracpeEmailRprtText('scrape_cnb')
        sendEmail(var_receiverEmail, var_text, var_text )
        
    try:
        import scrape_ft
        print('scrape FT complete')
    except Exception as e:
        print('FT scrape with error:  ', e)
        var_text = getSracpeEmailRprtText('scrape_ft')
        sendEmail(var_receiverEmail, var_text, var_text )
        
    try:
        import scrape_fp
        print('scrape FP complete')
    except Exception as e:
        print('FP scrape with error:  ', e)
        var_text = getSracpeEmailRprtText('scrape_fp')
        sendEmail(var_receiverEmail, var_text, var_text )
        
    try:
        import scrape_glb
        print('scrape GlobeAndMail complete')
    except Exception as e:
        print('GlobeAndMail scrape with error:  ', e)
        var_text = getSracpeEmailRprtText('scrape_glb')
        sendEmail(var_receiverEmail, var_text, var_text )
        
    try:
        import scrape_scm
        print('scrape South China complete')
    except Exception as e:
        print('South China scrape with error:  ', e)
        var_text = getSracpeEmailRprtText('scrape_scm')
        sendEmail(var_receiverEmail, var_text, var_text )
        
    getDataInfoSumm()

    logging.info("Script finished")

if __name__ == "__main__":
    main()


