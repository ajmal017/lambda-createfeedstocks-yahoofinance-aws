# https://alex.dzyoba.com/blog/python-import/
# https://docs.aws.amazon.com/es_es/serverless-application-model/latest/developerguide/serverless-sam-cli-install-linux.html
# https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-using-debugging.html

# https://github.com/aws/serverless-application-model/blob/master/versions/2016-10-31.md#api

# https://www.sqlshack.com/set-up-a-local-serverless-environment-using-the-aws-sam-cli/
# 1. sam build --use-container 
# 2.sam local invoke "StocksDataFeedsYahooFinanceFunction" --debug --env-vars resources/env.json
#   ó
#   sam local invoke 
# 3. pip freeze > requirements.txt NO HACER, METE MODULOS QUE NO SE PUEDEN INCLUIR DINAMICAMENTE POR LAMBDA 
#       pip install --target ./package csv CSV ES LA LIBRERIA A METER EN EL DIRECTORIO PACKAGE 
#       4. requirements.txt SOLO CON LOS MODULOS QUE SE IMPORTAN
# 5. Variables entorno en el yaml template declaradas 
# 6. sam local start-api
# 7. Accesible by clicking http://127.0.0.1:3000/stocksdatafeedsyahoofinance
# 8. sam package  --s3-bucket  apitradeable
# 9. sam deploy  --stack-name apitradeable --capabilities CAPABILITY_IAM 
#   ó
# 9. sam deploy --guided
# 10 aws cloudformation delete-stack --stack-name apitradeable



# API LOGS 

#
#  Creating a log group
#  aws logs create-log-group --log-group-name api-gateway-lambda-createfeedstock
# You need the Amazon Resource Name (ARN) for your log group to enable logging. The ARN format is arn:aws:logs:region:account-id:log-group:log-group-name.
# arn:aws:logs:eu-central-1:291573578422:log-group:api-gateway-lambda-createfeedstocks:*

# Enabling logging for a stage
# aws apigatewayv2 update-stage --api-id ixkms2a3p9     --stage-name '$default'     --access-log-settings  '{\"DestinationArn\": \"arn:aws:logs:eu-central-1:291573578422:log-group:api-gateway-lambda-createfeedstocks:*\", \"Format\": \"$context.requestId $context.status $context.httpMethod  $context.requestTime] $context.integrationErrorMessage  $context.routeKey $context.protocol $context.responseLength \"}'


# INVOCACION REMOTA y debug 
# aws lambda invoke --function-name StocksDataFeedsYahooFinance  out.json
#  cat .\out.json

# CON UN CAMBIO SIEMPRE,
# 1. BUILD   (OPCIONAL local invoke or start-api )
# 2. PACKAGE 
# 3. DEPLOY 

from ib_insync import *
from  configuration.ibtrader_simple_class_test import  *
from  configuration.ibtrader_functions import *
from  configuration.ibtrader_settings import  * 
from  configuration.ibtrader_stocks import   * 
import os, time
from datetime import datetime, timedelta
import yfinance
import json
import csv
import boto3
import os
import logging
import pandas as pd

from botocore.exceptions import ClientError
import matplotlib.dates as mpl_dates
import pandas_ta as ta

logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger()

from dateutil.relativedelta import relativedelta


s3 = boto3.client('s3')
sqs = boto3.client('sqs')




return_message = "Generating Signals from Yahoo finance (Support/Resistances/Divergences) daily, monthly, weekly:" # local variable referenced before assignment

error_message = "" # local variable referenced before assignment
BUCKET_NAME = "tradeable" # coger de las variables de entorno 
SQS_NAME = "tradeable_sqs" # coger de las variables de entorno 
CSV_SIGNALS_FILE = "IBTRADER_SIGNALS.csv"
# DATOS 
# SYMBOL
# SUPPORT
# RESISTANCE
# LAST_PRICE
# SMA           ¿200?
# PERIOD DAILY WEEKLY O MONTHLY
# DIV_TYPE  BAJISTA O ALCISTA 
# DIV_DATE1
# DIV_DATE2
# DIV_MACD    (S/N)
# DIV_INDICATOR  RSI (S/N)
# MACD_VALUE    (S/N)
# RSI_VALUE    (S/N)

# LO SUBIMOS A S3 SI ESTA EN UNA INSTANCIA DE AMAZON Y TIENE LA INSTANCIA EL ROLE ASOCIADO PARA AUTENTICAR CONTRA S3 CON SU IAM ROLE, NADA DE USUARIOS 


# solo si hay divergencias 
dfIBTrader =  pd.DataFrame(columns=['SYMBOL','SUPPORT','SUPPORT_DATE','RESISTANCE','RESISTANCE_DATE','LAST_PRICE','PRICE_IN_RANGE', 'SMA','PERIOD','DIV_MAC_TYPE','DIV_RSI_TYPE',
'DIV_MACD_DATE1', 'DIV_MACD_DATE2','DIV_RSI_DATE1', 'DIV_RSI_DATE2', 'MACD_VALUE','RSI_VALUE','LINK'])


def process():
    end =  datetime.now()

    # p1 = TestIBTrader("John", 36)
    # p1.myfunc()

    # test_lambda_function()
  
    queue = get_queue(SQS_NAME)
    # exists 

    response = sqs.get_queue_attributes(QueueUrl=queue.url, AttributeNames=['All'])

    logger.info("Getting Sqs   URL: " + queue.url) #  + "QueueArn :" + response['QueueArn']   + ",Policy ;  " + response['Policy']

    # receive messages by pulling queue 
    received_messages = receive_messages(queue,10,0) # 0 means short polling, increaase costs 

    logger.info("Number of received_messages SQS: " + str(len(received_messages)))

    if received_messages:

        logger.info("Polling SQS : New messages available") 

        for contract in ib_trader_contracts: 

            global return_message  # local variable referenced before assignment
            global dfIBTrader

            name = contract.symbol
            logger.info("Contract : " + name)

            return_message += name 
            return_message += ","

            for period in IBTraderTimeFrame.list():   
                # WEEKLY   
                #         

                logger.info("Getting data from  : " + period.lower()  + "/" + contract.symbol  + ".csv")
                dfDataTimeFrame = load_data(BUCKET_NAME,period.lower()  + "/" + contract.symbol  + ".csv")                       
                dfOriginalDataTimeFrame = dfDataTimeFrame.copy()
                print (contract.symbol + "-" + period.lower())
                #print (dfDataTimeFrame)
            
                last_resistance_value  = 0
                last_resistance_date  = 0
                last_support_value  = 0
                last_support_date  =  0

                resistance_levels, support_levels = getSupportResistances(dfDataTimeFrame)
                if len(resistance_levels)>0:
                    last_resistance_value  = dfDataTimeFrame.iloc[resistance_levels[-1][0]][StockDataFields.HIGH.value]
                    last_resistance_date  = dfDataTimeFrame.iloc[resistance_levels[-1][0]][StockDataFields.DATE.value]
                if len(support_levels)>0:
                    last_support_value  = dfDataTimeFrame.iloc[support_levels[-1][0]][StockDataFields.HIGH.value]
                    last_support_date  = dfDataTimeFrame.iloc[support_levels[-1][0]][StockDataFields.DATE.value]

                last_price   = dfOriginalDataTimeFrame[StockDataFields.CLOSE.value].iat[-1]          
                DIV_macd_type = ""   
                DIV_macd_date1 = ""
                DIV_macd_date2 = ""
                DIV_rsi_type = ""   
                DIV_rsi_date1 = ""
                DIV_rsi_date2 = ""
                bDIV_macd = False
                bDIV_rsi  = False
                sma = 0 
                macd_value = 0
                rsi_value = 0
                last_price_between_supp_resist = 0
                if last_price > last_support_value and last_price  < last_resistance_value:
                    last_price_between_supp_resist = 1

                peak_levels_macd  =  []
                valley_levels_macd = []
                dfsma  = pd.DataFrame()
                dfmacd = pd.DataFrame()
                dfrsi  = pd.DataFrame()
                try:
                    dfsma  =  dfDataTimeFrame.ta.sma(length=50, append=True) # 200 periodos en mensual es complicado que haya info, ponemos 50
                    dfmacd =  dfDataTimeFrame.ta.macd(fast=12, slow=26, signal=9, min_periods=None, append=True)
                    dfrsi  =  dfDataTimeFrame.ta.rsi(length=14, append=True)
                    sma = dfsma[dfsma.size-1]            
                    # For example, MACD(fast=12, slow=26, signal=9) will return a DataFrame with columns: ['MACD_12_26_9', 'MACDh_12_26_9', 'MACDs_12_26_9'].
                    macd_value = dfmacd.MACD_12_26_9.iat[-1]  # AQUI SACAMOS EL MACD , DIVERGENCIA POR HISTOGRAMA

                    rsi_value = dfrsi[dfrsi.size-1]

                    peak_levels_macd,valley_levels_macd = getIndicatorPeaksValleys(dfmacd,"MACD_12_26_9")  #  MACDH_12_26_9
                except IndexError as error2:
                    logger.info(error2)
                    logger.info("'Error calculating indicators for  " + contract.symbol + "-" + period)      
                    continue          
                except AssertionError as error:
                    logger.info(error)
                    logger.info("'Error calculating indicators for  " + contract.symbol + "-" + period)                    
                    continue

                # MACD 
                dfDivergenceUpperMACD = getIndexUpperDivergence(dfOriginalDataTimeFrame,support_levels,valley_levels_macd)
                dfDivergenceLowerMACD = getIndexLowerDivergence(dfOriginalDataTimeFrame,resistance_levels,peak_levels_macd)

                if not dfDivergenceUpperMACD.empty:

                        #logger.info("dfDivergenceUpperMACD mo empty")                    
                        #logger.info(dfDivergenceUpperMACD)                    

                        bDIV_macd = True    
                        DIV_macd_type = "UPPER"
                        # NOS QUEDAMOS CON LA ULTIMA DIVERGENCIA        
                        #logger.info(dfDivergenceUpperMACD.DIVERG_DATE1.iat[-1])        

                        mac_date1 =   datetime.strptime(dfDivergenceUpperMACD.DIVERG_DATE1.iat[-1], '%Y-%m-%d')
                        mac_date2 =   datetime.strptime(dfDivergenceUpperMACD.DIVERG_DATE2.iat[-1], '%Y-%m-%d')

                        DIV_macd_date1 = mac_date1.strftime('%d-%m-%Y')   
                        DIV_macd_date2 = mac_date2.strftime('%d-%m-%Y')                   

                if not dfDivergenceLowerMACD.empty:

                        #logger.info("dfDivergenceLowerMACD mo empty")                    
                        #logger.info(dfDivergenceLowerMACD)    

                        DIV_macd_type = "LOWER"
                        bDIV_macd = True     
                        #logger.info(dfDivergenceLowerMACD.DIVERG_DATE1.iat[-1])  
                        #logger.info(dfDivergenceLowerMACD.DIVERG_DATE2.iat[-1])                        

                        mac_date1 =   datetime.strptime(dfDivergenceLowerMACD.DIVERG_DATE1.iat[-1], '%Y-%m-%d')
                        mac_date2 =   datetime.strptime(dfDivergenceLowerMACD.DIVERG_DATE2.iat[-1], '%Y-%m-%d')

                        DIV_macd_date1 = mac_date1.strftime('%d-%m-%Y')   
                        DIV_macd_date2 = mac_date2.strftime('%d-%m-%Y')   
                # RSI 
                # RSI series 
                dfRSI = pd.DataFrame(dfrsi,columns=['RSI_14'])
            
                peak_levels_rsi,valley_levels_rsi = getIndicatorPeaksValleys(dfRSI,"RSI_14")    
                dfDivergenceUpperRSI = getIndexUpperDivergence(dfOriginalDataTimeFrame,support_levels,valley_levels_rsi)
                dfDivergenceLowerRSI = getIndexLowerDivergence(dfOriginalDataTimeFrame,resistance_levels,peak_levels_rsi)

                if not dfDivergenceUpperRSI.empty:
                    bDIV_rsi =  True
                    DIV_rsi_type = "UPPER"   

                    DIV_rsi_date1 = datetime.strptime(dfDivergenceUpperRSI.DIVERG_DATE1.iat[-1], '%Y-%m-%d').strftime('%d-%m-%Y')   
                    DIV_rsi_date2 =  datetime.strptime(dfDivergenceUpperRSI.DIVERG_DATE2.iat[-1], '%Y-%m-%d').strftime('%d-%m-%Y')       
                if not dfDivergenceLowerRSI.empty:
                    bDIV_rsi =  True
                    DIV_rsi_type = "LOWER"            
                    DIV_rsi_date1 = datetime.strptime(dfDivergenceLowerRSI.DIVERG_DATE1.iat[-1], '%Y-%m-%d').strftime('%d-%m-%Y')   
                    DIV_rsi_date2 =  datetime.strptime(dfDivergenceLowerRSI.DIVERG_DATE2.iat[-1], '%Y-%m-%d').strftime('%d-%m-%Y')              

                if bDIV_macd or bDIV_rsi:
                # solo si hay divergencias 
                #dfIBTrader =  pd.DataFrame(columns=['SYMBOL','SUPPORT','RESISTANCE','LAST_PRICE', 'SMA','PERIOD','DIV_MAC_TYPE','DIV_RSI_TYPE',
                #'DIV_MACD_DATE1', 'DIV_MACD_DATE2','DIV_RSI_DATE1', 'DIV_RSI_DATE2', 'MACD_VALUE','RSI_VALUE'])

                    link = "https://es.tradingview.com/chart?symbol=" # NASDAQ-CTXS/
                    link += contract.exchange + "-" + contract.symbol

                    dfIBTrader = dfIBTrader.append({'SYMBOL': contract.symbol, 'SUPPORT': last_support_value, 'SUPPORT_DATE': last_support_date,
                        'RESISTANCE' : last_resistance_value, 'RESISTANCE_DATE' : last_resistance_date , 'LAST_PRICE': last_price, 'PRICE_IN_RANGE' : last_price_between_supp_resist,
                        'SMA' : sma,'PERIOD' :period,'DIV_MAC_TYPE' :DIV_macd_type,'DIV_RSI_TYPE' :DIV_rsi_type
                        ,'DIV_MACD_DATE1' :DIV_macd_date1,'DIV_MACD_DATE2' :DIV_macd_date2,
                        'DIV_RSI_DATE1' :DIV_rsi_date1,'DIV_RSI_DATE2' :DIV_rsi_date2,
                        'MACD_VALUE' :macd_value
                        ,'RSI_VALUE' :rsi_value,'LINK' :link}, ignore_index=True)

        

        # LO SUBIMOS A S3 SI ESTA EN UNA INSTANCIA DE AMAZON Y TIENE LA INSTANCIA EL ROLE ASOCIADO PARA AUTENTICAR CONTRA S3 CON SU IAM ROLE, NADA DE USUARIOS 
        s3 = boto3.client('s3')
        response = s3.list_buckets()
        logger.info ("Verifying if bucket " + BUCKET_NAME  + " exists")
        if len(response)==0:
            bucket_created = create_bucket(BUCKET_NAME)
            logger.info ("Bucket " + BUCKET_NAME  + " created ")
        copy_to_s3(client=s3, df=dfIBTrader, bucket=BUCKET_NAME, filepath=CSV_SIGNALS_FILE)
        logger.info ("CSV IBTRADER_SIGNALS uploaded successfully ") 

        delete_messages(queue, received_messages)
        logger.info("Polling SQS : Deleting messages") 

    else:
        
        logger.info("Polling SQS : No new  messages available, cache provided from previopus file ") 

    
    return_message =  return_message + "https://" + BUCKET_NAME +".s3.amazonaws.com/"+ CSV_SIGNALS_FILE

 
 
    return "it worked"
        


    
def lambda_handler(event, context):

    global BUCKET_NAME 
    error = False
    # ENTORNO VIRTUAL NO ACCEDE AL CONTEXT CON LAS VARIABLES DE ENTORNO 
    
    logger.info('## ENVIRONMENT VARIABLES')
    logger.info(os.environ)
    logger.info('## EVENT')
    logger.info(event)

    try:
        BUCKET_NAME = context.env.BUCKET_NAME 
    except:
        logger.info("Error en context.env.BUCKET_NAME")    
        logger.info  ("Error en context.env.BUCKET_NAME " )
        logger.info(sys.exc_info()[0])
        logger.info(sys.exc_info()[0])    
    #logger.info  (context)
     
    process()

    # logger.info("Return Data : " + return_message )   

    #headers: {
    #    'Content-Type': 'application/json',
    #    },

    return {
        "statusCode": 200,        
        "body": json.dumps({
            "message": "hello SupportResistanceDivergencesFunction ",
            "data": return_message,
            "MB": context.memory_limit_in_mb,
            "log_stream_name": context.log_stream_name,
            "id": context.aws_request_id
        }),
    }



