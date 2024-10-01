import pandas as pd
import datetime
import boto3
import time
import random
import json
import requests
import time

from datetime import datetime
from io import StringIO # python3; python2: BytesIO 



def lambda_handler(event, context):
    
    ## Definiendo Region de AWS
    AWS_REGION = "us-east-1"
    
    ## Leer las credenciales para conexión a reddit desde ssm agent
    ssm_client = boto3.client("ssm", region_name=AWS_REGION)
    client_id = ssm_client.get_parameter(Name='client_id_twitch', WithDecryption=True)['Parameter'].get("Value")
    client_secret = ssm_client.get_parameter(Name='client_secret_twitch', WithDecryption=True)['Parameter'].get("Value")
    bearer_token = ssm_client.get_parameter(Name='bearer_token_twitch', WithDecryption=True)['Parameter'].get("Value")
    
    ## Definiendo servicio de S3
    s3_client = boto3.client("s3", region_name=AWS_REGION)
    mybucket = "s3://project-olimpo-logos"
    bucket = "project-olimpo-logos"
    
    client = boto3.resource('dynamodb')
    table = client.Table('Twitch_Top100_Games')
    
    client = boto3.client('s3')
    
    
    headers = {
    'Authorization': f'Bearer {bearer_token}',
    'Client-ID': client_id,
    }
    
    params = (
        ('first', '100'),
    )
    
    response2 = requests.get('https://api.twitch.tv/helix/games/top', headers=headers, params = params)
    
    
    if response2.status_code == 401:
        response = requests.post(f'https://id.twitch.tv/oauth2/token?client_id={client_id}&client_secret={client_secret}&grant_type=client_credentials')
        bearer_token = response.json()['access_token']
        headers = {
            'Authorization': f'Bearer {bearer_token}',
            'Client-ID': client_id,
            }
        response2 = requests.get('https://api.twitch.tv/helix/games/top', headers=headers, params = params)
    
    games_list = []
    
    for ans in response2.json()['data']:
        games_list.append(ans)
    
    params = (
        ('first', '100'),
        ('after', response2.json()['pagination']['cursor'])
    )
    
    full_list = []
    i = 1
    for game in games_list:
        try:
            #print(str(i)+ ' '+ game['name'])
            i += 1
            time.sleep(0.6)
            params = (
                ('first', '100'),
                ('game_id',game['id'])
            )
            checker = '-1'
            while True:
                try: 
                    response2 = requests.get('https://api.twitch.tv/helix/streams', headers=headers, params = params)
                except:
                    try: 
                        response2 = requests.get('https://api.twitch.tv/helix/streams', headers=headers, params = params)
                    except:
                        break
                resp_data = response2.json()['data']
                if resp_data[0]['id'] == checker:
                    break
                checker = resp_data[0]['id']
                full_list.extend(resp_data)
                if 'cursor' in response2.json()['pagination'].keys():
                    params = (
                        ('first', '100'),
                        ('game_id',game['id']),
                        ('next', response2.json()['pagination']['cursor'])
                    )
                else:
                    break
        except:
            pass
    games = pd.DataFrame(games_list).rename(columns={'id':'game_id', 'name':'game_name'})
    streams = pd.DataFrame(full_list).merge(games)
    streams = streams.drop(columns=['tag_ids'])
    cur_date = pd.Timestamp.now()
    streams['retrieval_date'] = cur_date
    streams['retrieval_date'] = streams['retrieval_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
    

    dict_stream = streams.to_dict(orient='records')
    
    filename = "twitch_raw_top100_" + str(datetime.now()) + ".json"
    
    
    s3_client.put_object(
        Bucket=bucket,
        Key="data_extraction/Twitch_Top100/raw_data/" + filename,
        Body=json.dumps(dict_stream)
        )
       
       
  
    #wr.dynamodb.put_df(df=streams, table_name='Twitch_Top100_Games')

    # table.put_item(Item = {
    #     'id': streams.id,
    #     'user_id': streams.user_id,
    #     'user_login': streams.user_login,
    #     'user_name': streams.user_name,
    #     'game_id': streams.game_id,
    #     'game_name': streams.game_name,
    #     'type': streams.type,
    #     'title': streams.title,
    #     'viewer_count': streams.viewer_count,
    #     'started_at': streams.started_at,
    #     'language': streams.language,
    #     'thumbnail_url': streams.thumbnail_url,
    #     'tags': streams.tags,
    #     'is_mature': streams.is_mature,
    #     'box_art_url': streams.box_art_url,
    #     'igdb_id': streams.igdb_id,
    #     'retrieval_date': streams.retrieval_date
    #     })

   
   
   

   
   