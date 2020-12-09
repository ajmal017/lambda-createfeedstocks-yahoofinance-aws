from ib_insync import *
from  configuration.ibtrader_settings import * 
from  configuration.timeframe_class import * 
from  configuration.stock_data_class import *
import matplotlib.dates as mpl_dates
import pandas as pd
import numpy as np
import glob


import logging
import boto3
from botocore.exceptions import ClientError


logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger()

sqs = boto3.resource('sqs')


try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO


def get_queue(name):
    """
    Gets an SQS queue by name.

    Usage is shown in usage_demo at the end of this module.

    :param name: The name that was used to create the queue.
    :return: A Queue object.
    """
    try:
        queue = sqs.get_queue_by_name(QueueName=name)
        logger.info("Got queue '%s' with URL=%s", name, queue.url)
    except ClientError as error:
        logger.exception("Couldn't get queue named %s.", name)
        raise error
    else:
        return queue


def create_queue(name, attributes=None):
    """
    Creates an Amazon SQS queue.

    Usage is shown in usage_demo at the end of this module.

    :param name: The name of the queue. This is part of the URL assigned to the queue.
    :param attributes: The attributes of the queue, such as maximum message size or
                       whether it's a FIFO queue.
    :return: A Queue object that contains metadata about the queue and that can be used
             to perform queue operations like sending and receiving messages.
    """
    if not attributes:
        attributes = {}

    try:
        queue = sqs.create_queue(
            QueueName=name,
            Attributes=attributes
        )
        logger.info("Created queue '%s' with URL=%s", name, queue.url)
    except ClientError as error:
        logger.exception("Couldn't create queue named '%s'.", name)
        raise error
    else:
        return queue

def send_message(queue, message_body, message_attributes=None):
    """
    Send a message to an Amazon SQS queue.

    Usage is shown in usage_demo at the end of this module.

    :param queue: The queue that receives the message.
    :param message_body: The body text of the message.
    :param message_attributes: Custom attributes of the message. These are key-value
                               pairs that can be whatever you want.
    :return: The response from SQS that contains the assigned message ID.
    """
    if not message_attributes:
        message_attributes = {}

    try:
        response = queue.send_message(
            MessageBody=message_body,
            MessageAttributes=message_attributes
        )
    except ClientError as error:
        logger.exception("Send message failed: %s", message_body)
        raise error
    else:
        return response

def send_messages(queue, messages):
    """
    Send a batch of messages in a single request to an SQS queue.
    This request may return overall success even when some messages were not sent.
    The caller must inspect the Successful and Failed lists in the response and
    resend any failed messages.

    Usage is shown in usage_demo at the end of this module.

    :param queue: The queue to receive the messages.
    :param messages: The messages to send to the queue. These are simplified to
                     contain only the message body and attributes.
    :return: The response from SQS that contains the list of successful and failed
             messages.
    """
    try:
        entries = [{
            'Id': str(ind),
            'MessageBody': msg['body'],
            'MessageAttributes': msg['attributes']
        } for ind, msg in enumerate(messages)]
        response = queue.send_messages(Entries=entries)
        if 'Successful' in response:
            for msg_meta in response['Successful']:
                logger.info(
                    "Message sent: %s: %s",
                    msg_meta['MessageId'],
                    messages[int(msg_meta['Id'])]['body']
                )
        if 'Failed' in response:
            for msg_meta in response['Failed']:
                logger.warning(
                    "Failed to send: %s: %s",
                    msg_meta['MessageId'],
                    messages[int(msg_meta['Id'])]['body']
                )
    except ClientError as error:
        logger.exception("Send messages failed to queue: %s", queue)
        raise error
    else:
        return response

def copy_to_s3(client, df, bucket, filepath):
    csv_buf = StringIO()
    df.to_csv(csv_buf, header=True, index=True)    
    csv_buf.seek(0) 
    #logger.info ("Body:" + csv_buf.getvalue())     
    client.put_object(Bucket=bucket, Body=csv_buf.getvalue(), Key=filepath)
    print(f'Copy {df.shape[0]} rows to S3 Bucket {bucket} at {filepath}, Done!')

def test_lambda_function():
    print  ("Calling test_lambda_function")
    return "Worked"

def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def create_bucket(bucket_name, region=None):
    """Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param bucket_name: Bucket to create
    :param region: String region to create bucket in, e.g., 'us-west-2'
    :return: True if bucket created, else False
    """

    # Create bucket
    try:
        if region is None:
            s3_client = boto3.client('s3')
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client = boto3.client('s3', region_name=region)
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration=location)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def getData(file_data):
    
    dfData = pd.read_csv(file_data)
    dfData.Date = pd.to_datetime(dfData.Date)        
    dfData.columns = dfData.columns.str.lower()
    dfData = dfData[[StockDataFields.DATE.value, StockDataFields.OPEN.value, StockDataFields.HIGH.value, StockDataFields.LOW.value,StockDataFields.CLOSE.value]]
    dfData = dfData[dfData[StockDataFields.CLOSE.value].notnull()]
    dfData.reset_index(inplace=True)
    return dfData
    
def getRTH(Contract):
    return True

def isIndicatorSupport(df,i,keyField):
  support = df[keyField][i] < df[keyField][i-1]  and df[keyField][i] < df[keyField][i+1]  #and df[keyField][i+1] < df[keyField][i+2] and df[keyField][i-1] < df[keyField][i-2]
  #print (str(df[keyField][i-1]) + "-" + str(df[keyField][i]) + "-" + str(df[keyField][i+1]))
  #print ("Support?:" + str(i) + "-" + str(support))
  return support
def isIndicatorResistance(df,i,keyField):
  resistance = df[keyField][i] > df[keyField][i-1]  and df[keyField][i] > df[keyField][i+1] # and df[keyField][i+1] > df[keyField][i+2] and df[keyField][i-1] > df[keyField][i-2]
  return resistance

def isPriceSupport(df,i,keyField):
  support = (df[keyField][i] < df[keyField][i-1]  and df[keyField][i] < df[keyField][i+1]  and df[keyField][i+1] < df[keyField][i+2] and df[keyField][i-1] < df[keyField][i-2])
  #print (str(df[keyField][i-1]) + "-" + str(df[keyField][i]) + "-" + str(df[keyField][i+1]))
  #print ("Support?:" + str(i) + "-" + str(support))
  return support
def isPriceResistance(df,i,keyField):
  resistance = df[keyField][i] > df[keyField][i-1]  and df[keyField][i] > df[keyField][i+1]  and df[keyField][i+1] > df[keyField][i+2] and df[keyField][i-1] > df[keyField][i-2]
  return resistance


#PEAKS_VALLEYS_PRICE_ = "PRICE"
#PEAKS_VALLEYS_INDICATORS_ = "INDICATOR"
def isSupport(df,i,keyField, priceOrIndicator):
  if priceOrIndicator == PEAKS_VALLEYS_PRICE_:
     support = isPriceSupport(df,i,keyField)
  else:
     support = isIndicatorSupport(df,i,keyField)
  return support
def isResistance(df,i,keyField, priceOrIndicator):
  if priceOrIndicator == PEAKS_VALLEYS_PRICE_:
     resistance = isPriceResistance(df,i,keyField)
  else:
     resistance = isIndicatorResistance(df,i,keyField)
  return resistance
def isFarFromLevel(l,levels, mean):
  return np.sum([abs(l-x) < mean  for x in levels]) == 0

# Let’s define a function that, given a price value, returns False if it is near some previously discovered key level.  
# levels = []
#  for i in range(2,df.shape[0]-2):
#    if isSupport(df,i):
#     levels.append((i,df[StockDataFields.LOW.value][i]))
#    elif isResistance(df,i):
#     levels.append((i,df[StockDataFields.HIGH.value][i]))
#    s =  np.mean(df[StockDataFields.HIGH.value] - df[StockDataFields.LOW.value])
# def isFarFromLevel(l, levels):
#    return np.sum([abs(l-x) < s  for x in levels]) == 0


def fileExists(symbol, path):
   mylist = [f for f in glob.glob(symbol + "_*.csv")]
   if len(mylist)>0:
        return True
   else:
        return False

def getFile(symbol, timeFrame):   
   path =  SETTINGS_REALPATH_STOCK_DATA_WEEK
   if timeFrame == str(IBTraderTimeFrame.MONTH.value):
     path =  SETTINGS_REALPATH_STOCK_DATA_MONTH
   elif timeFrame ==  str(IBTraderTimeFrame.DAY.value):
     path =  SETTINGS_REALPATH_STOCK_DATA_DAY  

   mylist = [f for f in glob.glob(path + symbol + "*.csv")]  
   if len(mylist)>0:
        return mylist[0]
   else:
        return ""

# TAMBIEN VALE PARA VALLES Y PICOS DE INDICADORES 
# TAMBIEN VALE PARA VALLES Y PICOS DE INDICADORES 
def getSupportResistances(dfData):

    support_levels = []
    resistance_levels  = []
    logger.info ()
    mean_value =  np.mean(dfData[StockDataFields.HIGH.value] - dfData[StockDataFields.LOW.value])
    for i in range(2,dfData.shape[0]-2):
        # Finally, let’s create a list that will contain the levels we find. Each level is a tuple whose first element is the index of the signal candle and the second element is the price value.
        #print (support_levels)
        if isSupport(dfData,i,StockDataFields.LOW.value,PEAKS_VALLEYS_PRICE_):
            l = dfData[StockDataFields.LOW.value][i]
            #print (isFarFromLevel(l,support_levels,mean_value))
            if isFarFromLevel(l,support_levels,mean_value):            
                support_levels.append((i,l))
            #print (support_levels)
        elif isResistance(dfData,i,StockDataFields.HIGH.value,PEAKS_VALLEYS_PRICE_):
            l = dfData[StockDataFields.HIGH.value][i]
            if isFarFromLevel(l,resistance_levels,mean_value):
                resistance_levels.append((i,l))        
    return resistance_levels,support_levels

# TAMBIEN VALE PARA VALLES Y PICOS DE INDICADORES 
def getIndicatorPeaksValleys(dfData,keyField):
    valleys_levels = []
    peaks_levels   = []    
    for i in range(2,dfData.shape[0]-2):
        # Finally, let’s create a list that will contain the levels we find. Each level is a tuple whose first element is the index of the signal candle and the second element is the price value.
        if isSupport(dfData,i,keyField,PEAKS_VALLEYS_INDICATORS_):
            l = dfData[keyField][i]               
            valleys_levels.append((i,l))                 
        elif isResistance(dfData,i,keyField, PEAKS_VALLEYS_INDICATORS_):
            l = dfData[keyField][i]            
            peaks_levels.append((i,l))        
    return peaks_levels,valleys_levels 

#  DIVERGENCIAS BAJISTAS 
def getIndexLowerDivergence(dfData,lPeaksPrice,lPeaksIndicator):
    
    dfPeaksPrice = pd.DataFrame(lPeaksPrice,columns=['TimeFrameImdex','Price'])
    dfPeaksIndicator = pd.DataFrame(lPeaksIndicator,columns=['TimeFrameImdex','Price'])
    
    dfDivergence =  pd.DataFrame(columns=['DIVERG_DATE1','DIVERG_DATE2','DIVERG_PRCE1','DIVERG_PRCE2', 'DIVERG_INDICATOR1','DIVERG_INDICATOR2'])

    # SOLO HASTA EL VALOR N-1
    for i in range(len(dfPeaksPrice)-1):
    
        # COGEMOS EL PRIMER INDICE ENCONTRADO CON EL PICO DE LA RESISTENCIA
        currentPricePeak        =  dfPeaksPrice.iloc[i,1] # price 
        currentpricePeakindex   =  dfPeaksPrice.iloc[i,0] # TimeFrameImdex
        # COGEMOS EL SIGIOEMTE  INDICE ENCONTRADO CON EL PICO DE LA RESISTENCIA
        nextPricePeak            =  dfPeaksPrice.iloc[i+1,1] # price 
        nextPricePeakIndex       =  dfPeaksPrice.iloc[i+1,0] # TimeFrameImdex
        # VERIFICAMOS QUE AMOBS INDICES ESTEN EN EL DATAFRAME DEL INDICADOR , CON LO QUE PODRIA DAR UN DIVERGENCIA AL COINCIDIR AMBOS PUNTOS 
        currentIndicatorPeakindex = -1
        currentIndicatorPeakPrice = -1
        nextIndicatorPeakindex    = -1
        nextIndicatorPeakPrice    = -1

       
        dfIndicatorPeakindex = dfPeaksIndicator.loc[dfPeaksIndicator['TimeFrameImdex']==currentpricePeakindex]
        dfNextIndicatorPeakindex = dfPeaksIndicator.loc[dfPeaksIndicator['TimeFrameImdex']==nextPricePeakIndex]
        if not dfIndicatorPeakindex.empty:               
                currentIndicatorPeakindex = dfIndicatorPeakindex.values[0][0] # ["TimeFrameImdex"] 
                currentIndicatorPeakPrice = dfIndicatorPeakindex.values[0][1] # ["Price"]
                #print ("Find indexes : currentpricePeakindex and nextPricePeakIndex:" + str(currentpricePeakindex) + "," +  str(nextPricePeakIndex))
                #print ("Found : dfIndicatorPeakindex:" + str(currentIndicatorPeakindex) + "," +  str(currentIndicatorPeakPrice))
        if not dfNextIndicatorPeakindex.empty:               
                nextIndicatorPeakindex =  dfNextIndicatorPeakindex.values[0][0] # ["TimeFrameImdex"] 
                nextIndicatorPeakPrice  = dfNextIndicatorPeakindex.values[0][1] # ["Price"]
                #print ("Find indexes : currentpricePeakindex and nextPricePeakIndex:" + str(currentpricePeakindex) + "," +  str(nextPricePeakIndex))
                #print ("Found : dfNextIndicatorPeakindex:" + str(nextIndicatorPeakindex) + "," +  str(nextIndicatorPeakPrice))

        if (nextPricePeak >  currentPricePeak and  nextIndicatorPeakPrice < currentIndicatorPeakPrice and 
            currentpricePeakindex == currentIndicatorPeakindex and  nextPricePeakIndex == nextIndicatorPeakindex):

                diverg_date1 = dfData[StockDataFields.DATE.value][currentpricePeakindex]
                diverg_date2 = dfData[StockDataFields.DATE.value][nextPricePeakIndex]
                dfDivergence = dfDivergence.append({'DIVERG_DATE1': diverg_date1, 'DIVERG_DATE2': diverg_date2,'DIVERG_PRCE1' : currentPricePeak, 'DIVERG_PRCE2': nextPricePeak,
                 'DIVERG_INDICATOR1' : currentIndicatorPeakPrice,'DIVERG_INDICATOR2' :nextIndicatorPeakPrice }, ignore_index=True)

                

    return dfDivergence

#  DIVERGENCIAS ALCISTAS 
def getIndexUpperDivergence(dfData, lValleysPrice,lValleysIndicator):

    dfValleysPrice = pd.DataFrame(lValleysPrice,columns=['TimeFrameImdex','Price'])
    dfValleysIndicator = pd.DataFrame(lValleysIndicator,columns=['TimeFrameImdex','Price'])
    
    dfDivergence =  pd.DataFrame(columns=['DIVERG_DATE1','DIVERG_DATE2','DIVERG_PRCE1','DIVERG_PRCE2'])

    # SOLO HASTA EL VALOR N-1
    for i in range(len(dfValleysPrice)-1):
    
        # COGEMOS EL PRIMER INDICE ENCONTRADO CON EL PICO DE LA RESISTENCIA
        currentPriceValley        =  dfValleysPrice.iloc[i,1] # price 
        currentpriceValleyindex   =  dfValleysPrice.iloc[i,0] # TimeFrameImdex
        # COGEMOS EL SIGIOEMTE  INDICE ENCONTRADO CON EL PICO DE LA RESISTENCIA
        nextPriceValley            =  dfValleysPrice.iloc[i+1,1] # price 
        nextPriceValleyIndex       =  dfValleysPrice.iloc[i+1,0] # TimeFrameImdex
        # VERIFICAMOS QUE AMOBS INDICES ESTEN EN EL DATAFRAME DEL INDICADOR , CON LO QUE PODRIA DAR UN DIVERGENCIA AL COINCIDIR AMBOS PUNTOS 
        currentIndicatorValleyindex = -1
        currentIndicatorValleyPrice = -1
        nextIndicatorValleyindex   = -1
        nextIndicatorValleyPrice = -1

       
        dfIndicatorValleyindex = dfValleysIndicator.loc[dfValleysIndicator['TimeFrameImdex']==currentpriceValleyindex]
        dfNextIndicatorValleyindex = dfValleysIndicator.loc[dfValleysIndicator['TimeFrameImdex']==nextPriceValleyIndex]
        if not dfIndicatorValleyindex.empty:
                currentIndicatorValleyindex = dfIndicatorValleyindex.values[0][0] # ["TimeFrameImdex"] 
                currentIndicatorValleyPrice = dfIndicatorValleyindex.values[0][1] # ["Price"]
        if not dfNextIndicatorValleyindex.empty:
                nextIndicatorValleyindex =  dfNextIndicatorValleyindex.values[0][0] # ["TimeFrameImdex"] 
                nextIndicatorValleyPrice  = dfNextIndicatorValleyindex.values[0][1] # ["Price"]

       
        if (nextPriceValley <  currentPriceValley and  nextIndicatorValleyPrice > currentIndicatorValleyPrice and 
            currentpriceValleyindex == currentIndicatorValleyindex and  nextPriceValleyIndex == nextIndicatorValleyindex):

                diverg_date1 = dfData[StockDataFields.DATE.value][currentpriceValleyindex]
                diverg_date2 = dfData[StockDataFields.DATE.value][nextPriceValleyIndex]
                dfDivergence = dfDivergence.append({'DIVERG_DATE1': diverg_date1, 'DIVERG_DATE2': diverg_date2,'DIVERG_PRCE1' : currentPriceValley, 'DIVERG_PRCE2': nextPriceValley,
                 'DIVERG_INDICATOR1' : currentIndicatorValleyindex,'DIVERG_INDICATOR2' :nextIndicatorValleyindex }, ignore_index=True)

    return dfDivergence
