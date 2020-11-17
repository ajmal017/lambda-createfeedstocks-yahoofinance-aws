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

from botocore.exceptions import ClientError


logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger()

from dateutil.relativedelta import relativedelta


s3 = boto3.client('s3')
sqs = boto3.client('sqs')


return_message = "Getting stocks from Yahoo finance:" # local variable referenced before assignment
error_message = "" # local variable referenced before assignment
BUCKET_NAME = "tradeable" # coger de las variables de entorno 
SQS_NAME = "tradeable_sqs" # coger de las variables de entorno 


def process():
    end =  datetime.now()

    # p1 = TestIBTrader("John", 36)
    # p1.myfunc()

    # test_lambda_function()

    # Create a SQS called tradeable_feeds_messages to push a notification when process is finished
    logger.info("Trying to create sqs queue  : " + SQS_NAME)

    queue = None    
    #queue_attributes={'FifoQueue': 'false' }

    queue_attributes={}
    try:            
        queue = create_queue(
            SQS_NAME
        )
        logger.info("Created queue '%s' with URL=%s", SQS_NAME, queue.url)
    except ClientError as error:
        logger.exception("Couldn't create queue named '%s'.", SQS_NAME)        
        queue = get_queue(SQS_NAME)
        # exists 

    response = sqs.get_queue_attributes(QueueUrl=queue.url, AttributeNames=['All'])

    logger.info("Getting Sqs   URL: " + queue.url) #  + "QueueArn :" + response['QueueArn']   + ",Policy ;  " + response['Policy']

    for contract in ib_trader_contracts: 

        global return_message  # local variable referenced before assignment


        formatted_end = ""


        name = contract.symbol
        logger.info("Contract : " + name)

        return_message += name 
        return_message += ","
        

        ticker = yfinance.Ticker(name)

        formatted_end   = end.strftime('%Y-%m-%d')

        formatted_start = end - relativedelta(years=20)


        #logger.info (formatted_start)
        #logger.info (formatted_end)
        df_d = ticker.history(interval="1d",start=formatted_start ,end  = formatted_end)
        df_w = ticker.history(interval="1wk",start=formatted_start ,end = formatted_end)
        df_m = ticker.history(interval="1mo",start=formatted_start ,end =  formatted_end)

        SETTINGS_REALPATH_STOCK_DATA_DAY = "day/"
        SETTINGS_REALPATH_STOCK_DATA_MONTH = "month/"
        SETTINGS_REALPATH_STOCK_DATA_WEEK = "week/"


        logger.info ("Adding file : " + SETTINGS_REALPATH_STOCK_DATA_DAY   + contract.symbol + '.csv to bucket : ' + BUCKET_NAME)        
        copy_to_s3(client=s3, df=df_d, bucket=BUCKET_NAME, filepath=SETTINGS_REALPATH_STOCK_DATA_DAY   + contract.symbol + '.csv')
        logger.info ("Adding file : " + SETTINGS_REALPATH_STOCK_DATA_MONTH   + contract.symbol + '.csv to bucket : ' + BUCKET_NAME)
        copy_to_s3(client=s3, df=df_w, bucket=BUCKET_NAME, filepath=SETTINGS_REALPATH_STOCK_DATA_MONTH   + contract.symbol + '.csv')
        logger.info ("Adding file : " + SETTINGS_REALPATH_STOCK_DATA_WEEK   + contract.symbol + '.csv to bucket : ' + BUCKET_NAME)
        copy_to_s3(client=s3, df=df_m, bucket=BUCKET_NAME, filepath=SETTINGS_REALPATH_STOCK_DATA_WEEK   + contract.symbol + '.csv')
        
        
        

    # Mandamos un mensaje a la cola para ser capturado por otra lambda function 
    tradeable_message = json.dumps({
        "message": "Download CSV stocks data from " + return_message,
        "MessageGroupId" : 1,
        "date" : end.strftime('%Y-%m-%d')            
    })   

    logger.info("Sending Message  to Sqs " + str(tradeable_message))
     
    send_message(queue, tradeable_message)

    
    return "it worked"
        


def load_data(bucket, key):
    """ 
    Load S3 data as in memory string

    Args:
        bucket (str): S3 bucket name
        key (str): S3 prefix path to CSV file
    Returns:
        CSV string 
    """
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, key)
    return obj.get()['Body'].read().decode('utf-8') 
    
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
            "message": "hello StocksDataFeedsYahooFinanceFunction ",
            "data": return_message,
            "MB": context.memory_limit_in_mb,
            "log_stream_name": context.log_stream_name,
            "id": context.aws_request_id
        }),
    }



#raw_data = load_data(os.environ["BUCKET"], os.environ["KEY"])
