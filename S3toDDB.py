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
                Item={...}
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
