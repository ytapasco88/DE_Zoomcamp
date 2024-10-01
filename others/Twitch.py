import pandas as pd
from datetime import datetime, timedelta
import boto3
import json
import re
import logging
import io

import warnings
warnings.filterwarnings("ignore")



def lambda_handler(event, context):
    bucket = "project-olimpo-logos"
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket('project-olimpo-logos')
    
    prefix = 'Twitch_Top100/raw_data/to_processed/'
    
    files = [my_bucket_object.key for my_bucket_object in my_bucket.objects.filter(Prefix=prefix)]
    r = re.compile(f".json")
    newlist_nodos = list(filter(r.search, files))
    
    df_fin = pd.DataFrame()
    for n in newlist_nodos:
        s3 = boto3.client('s3', region_name = 'us-east-1')
        obj = s3.get_object(Bucket=bucket, Key=n)
        try:    
            df_aux = pd.read_json(io.BytesIO(obj['Body'].read()))
            df_aux["name_json"] = n
            df_fin = pd.concat([df_fin, df_aux], ignore_index=True)
    
        except:
            df_aux = wr.s3.read_json(path='s3://'+bucket+'/'+n)
            df_aux["name_json"] = n
            df_fin = pd.concat([df_fin, df_aux], ignore_index=True)
    
    print(df_fin.columns)
    print(len(df_fin))
    
    
    dict_df_fin = df_fin.to_dict(orient='records')
    
    filename = "grouped_twitch_raw_top100_" + str(datetime.now()) + ".json"
    
    
    s3_client.put_object(
        Bucket=bucket,
        Key="Twitch_Top100/raw_data/grouped_data/" + filename,
        Body=json.dumps(dict_df_fin)
        )
        