import re
import boto3
import pandas as pd
import numpy as np


videos_path = 'D:\\html\\'
df = pd.read_csv('data1859_Click_PT1_31_05_22.csv')
df1 = df[['htmlLink']].drop_duplicates()

def token(x):
    try:
        x=re.findall(r"\[.*?\]", x)[0]
        x=x[1:-1].split(',')[0].split('/')
        idx = next((i for i, x in enumerate(x) if len(x) == 48), None)
        return x[idx]
    except:
        return np.NaN

df1['Token'] = df['htmlLink'].apply(lambda x:token(x))

########## Unique Tokens ###################
list_searchToken=list(df1['Token'].unique())
list_searchToken = [x for x in list_searchToken if str(x) != 'nan']   

############## Connect to s3 using boto ##########################
s3 = boto3.resource(
service_name='s3',
region_name='us-east-1',
aws_access_key_id='AKIAWFPZMNZHVZTOQ5X2',
aws_secret_access_key='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')
   
##################### Download the mp4 or wav files of unique tokens ###################
for bucket in s3.buckets.all():
    if bucket.name == 'prod-osgsearchmedia':
        for obj in bucket.objects.all():
            if (obj.key.split('.')[-1]=='html'):
                if (obj.key.split("/")[0] in list_searchToken):
                    bucket.download_file(obj.key, videos_path+obj.key.replace("/","_").replace(':','-'))
                
                elif (obj.key.split("/")[1] in list_searchToken):
                    bucket.download_file(obj.key, videos_path+obj.key[7:].replace("/","_").replace(':','-'))
                
##########################################################################
