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


try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

def copy_to_s3(client, df, bucket, filepath):
    csv_buf = StringIO()
    df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
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

                diverg_date1 = dfData["date"][currentpricePeakindex]
                diverg_date2 = dfData["date"][nextPricePeakIndex]
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

                diverg_date1 = dfData["date"][currentpriceValleyindex]
                diverg_date2 = dfData["date"][nextPriceValleyIndex]
                dfDivergence = dfDivergence.append({'DIVERG_DATE1': diverg_date1, 'DIVERG_DATE2': diverg_date2,'DIVERG_PRCE1' : currentPriceValley, 'DIVERG_PRCE2': nextPriceValley,
                 'DIVERG_INDICATOR1' : currentIndicatorValleyindex,'DIVERG_INDICATOR2' :nextIndicatorValleyindex }, ignore_index=True)

    return dfDivergence
