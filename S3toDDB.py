import boto3
import csv
import sys
import pandas as pd
from botocore.exceptions import ClientError
import logging
from io import StringIO

logger = logging.getLogger(__name__)

#initiate the client/resource
dynamodb = boto3.client('dynamodb', region_name='us-east-1')

#Specify the table name and bucket name here
table_name = 'milestone-table'
bucket_name = '<redacted>'
bucket_prefix = '<redacted>'

#s3 client
s3_client = boto3.client('s3',region_name='us-east-1')

#wait for the table to be created
#waiter = dynamodb.get_waiter('table_exists')
#waiter.wait(TableName='milestone-table')

#put-item function
def put_item_function(data_import):
    for i in range(0,len(data_import)):
        try:
            dynamodb.put_item(
                TableName= table_name,
                Item={
                    'GlobalEventID': {
                        'S': data_import.values[i][0]
                    },
                    'Day': {
                        'S': data_import.values[i][1]
                    },
                    'MonthYear': {
                        'S': data_import.values[i][2]
                    },
                    'Year': {
                        'S': data_import.values[i][3]
                    },
                    'FractionDate': {
                        'S': data_import.values[i][4]
                    },
                    'Actor1Code': {
                        'S': data_import.values[i][5]
                    },
                    'Actor1Name': {
                        'S': data_import.values[i][6]
                    },
                    'Actor1CountryCode': {
                        'S': data_import.values[i][7]
                    },
                    'Actor1KnownGroupCode': {
                        'S': data_import.values[i][8]
                    },
                    'Actor1EthnicCode': {
                        'S': data_import.values[i][9]
                    },
                    'Actor1Religion1Code': {
                        'S': data_import.values[i][10]
                    },
                    'Actor1Religion2Code': {
                        'S': data_import.values[i][11]
                    },
                    'Actor1Type1Code': {
                        'S': data_import.values[i][12]
                    },
                    'Actor1Type2Code': {
                        'S': data_import.values[i][13]
                    },
                    'Actor1Type3Code': {
                        'S': data_import.values[i][14]
                    },
                    'Actor2Code': {
                        'S': data_import.values[i][15]
                    },
                    'Actor2Name': {
                        'S': data_import.values[i][16]
                    },
                    'Actor2CountryCode': {
                        'S': data_import.values[i][17]
                    },
                    'Actor2KnownGroupCode': {
                        'S': data_import.values[i][18]
                    },
                    'Actor2EthnicCode': {
                        'S': data_import.values[i][19]
                    },
                    'Actor2Religion1Code': {
                        'S': data_import.values[i][20]
                    },
                    'Actor2Religion2Code': {
                        'S': data_import.values[i][21]
                    },
                    'Actor2Type1Code': {
                        'S': data_import.values[i][22]
                    },
                    'Actor2Type2Code': {
                        'S': data_import.values[i][23]
                    },
                    'Actor2Type3Code': {
                        'S': data_import.values[i][24]
                    },
                    'IsRootEvent': {
                        'S': data_import.values[i][25]
                    },
                    'EventCode': {
                        'S': data_import.values[i][26]
                    },
                    'EventBaseCode': {
                        'S': data_import.values[i][27]
                    },
                    'EventRootCode': {
                        'S': data_import.values[i][28]
                    },
                    'QuadClass': {
                        'S': data_import.values[i][29]
                    },
                    'GoldsteinScale': {
                        'S': data_import.values[i][30]
                    },
                    'NumMentions': {
                        'S': data_import.values[i][31]
                    },
                    'NumSources': {
                        'S': data_import.values[i][32]
                    },
                    'NumArticles': {
                        'S': data_import.values[i][33]
                    },
                    'AvgTone': {
                        'S': data_import.values[i][34]
                    },
                    'Actor1Geo_Type': {
                        'S': data_import.values[i][35]
                    },
                    'Actor1Geo_Fullname': {
                        'S': data_import.values[i][36]
                    },
                    'Actor1Geo_CountryCode': {
                        'S': data_import.values[i][37]
                    },
                    'Actor1Geo_ADM1Code': {
                        'S': data_import.values[i][38]
                    },
                    'Actor1Geo_Lat': {
                        'S': data_import.values[i][39]
                    },
                    'Actor1Geo_Long': {
                        'S': data_import.values[i][40]
                    },
                    'Actor1Geo_FeatureID': {
                        'S': data_import.values[i][41]
                    },
                    'Actor2Geo_Type': {
                        'S': data_import.values[i][42]
                    },
                    'Actor2Geo_Fullname': {
                        'S': data_import.values[i][43]
                    },
                    'Actor2Geo_CountryCode': {
                        'S': data_import.values[i][44]
                    },
                    'Actor2Geo_ADM1Code': {
                        'S': data_import.values[i][45]
                    },
                    'Actor2Geo_Lat': {
                        'S': data_import.values[i][46]
                    },
                    'Actor2Geo_Long': {
                        'S': data_import.values[i][47]
                    },
                    'Actor2Geo_FeatureID': {
                        'S': data_import.values[i][48]
                    },
                    'ActionGeo_Type': {
                        'S': data_import.values[i][49]
                    },
                    'ActionGeo_Fullname': {
                        'S': data_import.values[i][50]
                    },
                    'ActionGeo_CountryCode': {
                        'S': data_import.values[i][51]
                    },
                    'ActionGeo_ADM1Code': {
                        'S': data_import.values[i][52]
                    },
                    'ActionGeo_Lat': {
                        'S': data_import.values[i][53]
                    },
                    'ActionGeo_Long': {
                        'S': data_import.values[i][54]
                    },
                    'ActionGeo_FeatureID': {
                        'S': data_import.values[i][55]
                    },
                    'DATEADDED': {
                        'S': data_import.values[i][56]
                    }
                }
                    )
        except ClientError as err:
            logger.error("Couldn't add item to table. Here's why:",
                         err.response['Error']['Code'], err.response['Error']['Message'])
            continue
    print('put item loop done')
    return

#list files in bucket prefix and then loop over those files. Call put item function
response = s3_client.list_objects_v2(Bucket = bucket_name, Prefix = bucket_prefix)
files = response.get("Contents")

for file in files:
        file_key = file['Key']
    print('reading file: ',file_key)
        resp = s3_client.get_object(Bucket= bucket_name , Key= file_key)
        body = resp['Body']
        csv_string = body.read().decode('utf-8')
        try:
            data_import = pd.read_csv(StringIO(csv_string), sep='\t', lineterminator='\n',  index_col=False, dtype='unicode')
            data_import.values[data_import.isna()] = 'na'
        except:
            print("unable to read/parse file: ", file_key)
            continue
        put_item_function(data_import)
    print("this file has been done: ", file_key)
