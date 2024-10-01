import requests
import sys, os
import json
import time
import dateutil.parser
import unicodedata
import boto3
import re 
from itertools import compress
import pandas as pd
import datetime as dt
from datetime import datetime, timedelta, timezone
from uuid import uuid4

# Request



today = dt.datetime.today() - dt.timedelta(hours=5)
yesterday = today - dt.timedelta(hours=24)


def connect_to_endpoint(keyword,bearer_lista,bearer_token, start_date, end_date, next_token=None,tiempo=10): #Sumar next token hasta que no se actualice más
    
    #no hay más tokens
    #print(bearer_lista)
    #if bearer_token not in bearer_lista:
        #raise Exception(" lista token vacia, ningun token funciona o se acabo la capacidad.")
    
    # params object received from create_url function
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    url = "https://api.twitter.com/2/tweets/search/recent"
    
    key='(from:"'+keyword+'" OR "'+keyword+'" OR "@'+keyword+'" OR "#'+keyword+'")'
    key += " lang:es"
    params = {'query': key,
                'start_time': start_date,
                'end_time': end_date,
                'max_results': 100,
                'expansions': 'author_id,referenced_tweets.id,referenced_tweets.id.author_id,entities.mentions.username,attachments.poll_ids,attachments.media_keys,in_reply_to_user_id,geo.place_id',
                'tweet.fields': 'id,author_id,in_reply_to_user_id,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source,text,entities,context_annotations',
                'user.fields': 'id,location,name,username,created_at,description,public_metrics,verified,protected',
                'place.fields': 'contained_within,country,country_code,full_name,geo,id,name,place_type', 
                'next_token': next_token}
    
    
    #print(url,'\n',headers,'\n',params)
    response = requests.get(url, headers=headers, params=params)
    #print('url',response.url)
    #print(response.status_code)
    #if response.status_code != 200:
    #    raise Exception(response.status_code, response.text) # Error
    response=response.json()
    
    #change token if does not work
    if 'title' in response:
        if response['title']=='UsageCapExceeded' or response['title']=='Unauthorized':
            #print(bearer_lista)
            print('token '+bearer_token +' no sirvió. Borrando...')
            bearer_lista=bearer_lista[1:]
            
            if bearer_lista:
                bearer_token=bearer_lista[0]
                time.sleep(1)
                
                response,bearer_lista = connect_to_endpoint(keyword,bearer_lista,bearer_token, start_date, end_date, next_token,tiempo)

                #if next_token== None: #De dónde saca esto??
                    
                #    response,bearer_token = changue_token(json,bearer_lista,bearer_token,keyword, start_time, end_time, max_results)
                #else:
                #    response = connect_to_endpoint(url[0], headers, url[1],next_token)
                #    response,bearer_token = changue_token(response,bearer_lista,bearer_token,keyword, start_time, end_time, max_results,next_token)

            else:
                bearer_lista = bearer_lista_2
                bearer_token=bearer_lista[0]
                response,bearer_lista = connect_to_endpoint(keyword,bearer_lista,bearer_token, start_date, end_date, next_token,tiempo)
                #raise Exception(" lista token vacia, ningun token funciona o se acabo la capacidad.")
            
        #if too many requests, wait
        elif (response['title']=='Too Many Requests' or response['title']=='Service Unavailable'):
            print('muchas peticiones. waiting...')
            time.sleep(min(tiempo,90))
            tiempo*=2
            response,bearer_lista = connect_to_endpoint(keyword,bearer_lista,bearer_token, start_date, end_date, next_token,tiempo)
    
    return response,bearer_lista
    
def json_to_pandas(json_response,topic_id):
    
    df = pd.json_normalize(json_response, 'data',['includes'])
    #print(df.shape)
    #print(df.head())

    df['topic_id'] = topic_id
    df["type_tweet"] = None
    df["tweet_count"] = None
    #df["id_tweet"] = None
    df["user_id"] = None
    df["username"] = None
    df["followers_count"] = None
    df["following_count"] = None
    df["verified"] = None
    df["user_description"] = ''
    df["user_protected"] = True
    df["fullname"] = ''
    df["user_verified"] = ''
    #print(df)
    print(df['includes'][0]['users'])
    for i in range(len(df)):
        if str(pd.isna(df['referenced_tweets'][i])) != 'True':
            df['type_tweet'][i] = df['referenced_tweets'][i][0]['type'] 
    
        if str(pd.isna(df['includes'][0])) != 'True':
            #print('user',i,':',df['includes'][0]['users'][i])
            
            try:
                #dio=False
                for user_details in df['includes'][0]['users']:
                    if str(user_details['id'])==str(df['author_id'][i]):
                        #dio=True
                        df['followers_count'][i] = user_details['public_metrics']['followers_count']
                        df['following_count'][i] = user_details['public_metrics']['following_count']
                        df['tweet_count'][i] = user_details['public_metrics']['tweet_count']
                
                        df['username'][i] = user_details['username']
                        df['user_id'][i] = user_details['id']
                        df['user_description'][i] = user_details['description']
                        df["user_protected"][i] = user_details['protected']
                        df['user_verified'][i] = user_details['verified']
                        df['fullname'][i] = user_details['name']
                        break
                    
            except Exception as e:
                print(e)
                
            #print(dio)
    
    df["followers_count"] = df["followers_count"].astype('int64')
    df['public_metrics.retweet_count'] = df['public_metrics.retweet_count'].astype("int64")
    df['following_count'] = df["following_count"].astype('int64')
    df['tweet_count'] = df['tweet_count'].astype("int64")
    df["user_verified"] = df["user_verified"].apply(lambda x: 'True' if x == 'True' else 'False')
    df["user_favourites_count"] = df["public_metrics.like_count"].astype('int64')
    df['following_count'] = df['following_count'].astype('int64')
    df["retweet"] = df["type_tweet"].apply(lambda x: 'True' if x == 'retweeted' else 'False')
    df["profile_validation"] = df["user_description"].apply(lambda x: 'False' if x == '' else 'True')
    df["follower_rate"] = (df['followers_count'].astype('float64')/df['following_count'].astype('float64'))
    
    return df


def perfilamiento(df_ingesta):
    
    #INFLUENCER??
    
    df_ingesta['influencers_popularity_score'] = df_ingesta['public_metrics.retweet_count'].astype("float64") + df_ingesta['user_favourites_count'].astype("float64")
    #calculating the reach score
    df_ingesta['influencers_reach_score'] = df_ingesta['followers_count'].astype("float64") - df_ingesta['following_count'].astype("float64")
    
    df_ingesta["Influencer"] = ""
    df_ingesta["Puntuacion_Influencer"] = None
    pd.options.mode.chained_assignment = None
    i = 0
    
    lista = df_ingesta["username"]
    cuentas  = []
    puntuacion = []
    lista_influencers = [] 

    for i in range(len(lista)):
        cuenta = 0
        influencer = False

        try:
            if df_ingesta["follower_rate"][i]>2000:
                cuenta+= 2
        except:
            cuenta = cuenta
        if df_ingesta["followers_count"][i] >= 50000:
            cuenta+=0.5
        if df_ingesta["followers_count"][i] >= 60000:
            cuenta+=4
        if df_ingesta["followers_count"][i] >= 200000:
            cuenta+=4
        if df_ingesta["followers_count"][i] >= 100000:
            cuenta+=2
        if df_ingesta["user_verified"][i] == 'True':
            cuenta+=5
        if  df_ingesta["influencers_popularity_score"][i]>df_ingesta['influencers_popularity_score'].mean():
            cuenta +=0.5
        if  df_ingesta["influencers_reach_score"][i]>df_ingesta['influencers_reach_score'].mean():
            cuenta +=0.5
            
            
        if cuenta >= 3.9:
            influencer = True
            df_ingesta["Influencer"][i] = "Mundial"
        elif 2.9 < cuenta:
            influencer = True
            df_ingesta["Influencer"][i] = "Local"
        else:
            influencer = False
            df_ingesta["Influencer"][i] = "No"

        df_ingesta["Puntuacion_Influencer"][i] = cuenta
        cuentas.append(str(df_ingesta["username"][i]))
        puntuacion.append(cuenta)
        i += 1
    
    #BOTKE??
    
    now = dt.datetime.now()
    year = now.strftime("%Y")
    df_ingesta['age_currently_created'] = df_ingesta['created_at'].apply(lambda x: x[26:30]) == year  
    
    pd.options.mode.chained_assignment = None
    df_ingesta["Botke"] = False
    df_ingesta["Puntuacion_Botke"] = None
    
    cuentas  = []
    puntuacion = []
    lista_botke = [] 
    
    for i in range(len(lista)):
        cuenta = 0
        botke = False
        try:
            if df_ingesta["follower_rate"][i] < 0.5:
                cuenta -= 0.5
                #print("su tasa de seguidores es menor a 2000")
        except:
            cuenta = cuenta
        if df_ingesta["profile_validation"][i] == True:
            cuenta += 0.8
            #print("descripcion aprobada")
        if df_ingesta["user_protected"][i] == 'False':
            cuenta += 0.3  #Muchos usuarios normales tienen privado simplemente por "mantener su vida oculta"
            #print("no es privado")
        if df_ingesta["age_currently_created"][i] == False:
            cuenta += 0.9
            #print("no es de este año reciente")
        if  df_ingesta["followers_count"][i]>300 :
            cuenta += 0.8
            #print("tiene likes superiores a 300 en toda su cuenta")
        if  df_ingesta["followers_count"][i]>=50 : # cambiado 1
            cuenta += 0.8  #Vale el doble porque es fundamental ese parámetro
            #print("tiene mas de 50 seguidores")
        if cuenta <= 1.3:
            botke= True #Por ahora el umbral que define un botke es 1.5,
            #print("entro la cuenta es",cuenta)

            df_ingesta["Botke"][i] = True
        df_ingesta["Puntuacion_Botke"][i] = cuenta
        lista_botke.append(botke)
        #print("la cuenta de", df_ingesta["user_screen_name"][i],"es", cuenta)
        #print("-----------")
        cuentas.append(str(df_ingesta["username"][i]))
        puntuacion.append(cuenta)
        i += 1
        df_ingesta['tipo_usuario']='Regular'
        for i in range(len(lista)):
            if df_ingesta["Influencer"][i]=='No':
                if df_ingesta["Botke"][i]==True:
                    df_ingesta["tipo_usuario"][i]='Botke'
            else:
                df_ingesta["tipo_usuario"][i]='Influencer'
                
    return cuentas,puntuacion,df_ingesta


def datos(keyword,topic_id):
    start = time.time()
    
    
    
    bearer_token = "AAAAAAAAAAAAAAAAAAAAAGf6ZAEAAAAAoGtS74l5GvDg4y4%2BEPbWZa2hJ0s%3DkGnHwrUXppmZlwU9kTQNqo1EgAylDd1y32GBpHEdksDeQHvwAg" #anclopezg
    bearer_token_2 = "AAAAAAAAAAAAAAAAAAAAAGB1VQEAAAAA%2FM%2Bj150T25xAY5vrkT5kvoZASjM%3DBlOaChICMUciatLdwfOZdC3OsgAGubxuqJtgj9yJGvArG8ST1i"#esteban
    bearer_token_3 = "AAAAAAAAAAAAAAAAAAAAAGf6ZAEAAAAALPxcFkiiHhUpVlnMLTQrf5yOpzI%3Dbf3gWgPZGYxVkR3u0d9FLq84j8VYW1L7s2HkoAZ6kpEHBA6buf" #anclopez
    bearer_token_4 = "AAAAAAAAAAAAAAAAAAAAAKf2ZQEAAAAASL5M%2FVfao9G6%2FkaX4EtTzEZqQTQ%3DAhAjZ75LdsxEWim2ZWrNTOsU3Gy4O3GqGBUk4AHaDH0QQbOtYc" #Lau
    bearer_token_5 = "AAAAAAAAAAAAAAAAAAAAAMD%2BegEAAAAA2PCyaNXXgYDZvMmMBayo2PlpQr0%3D4DlXZU5H2mYk2Inf7KtsVl4v6MZajK0nwAiespYUKn0qEZ3Yiu" #Adan
    bearer_token_6 = "AAAAAAAAAAAAAAAAAAAAAHGEbQEAAAAAks2hzSIPL2241jQ%2F72k%2BdMiUsEI%3Dlu3lcmD9ls3U6WX7HIFObt2M2rKww61AB8yrBNZPOX2MHW6Ig0" #Charlie
    bearer_token_7 = "AAAAAAAAAAAAAAAAAAAAALjTfAEAAAAAEv0CzD5OJgaHcXNlbkxn9pwT050%3DcxX5TGcEq2Uqfio2qkkwcgHFnzCRaLwZQnX4Zd2ebvD07Rna3a" #Rigo
    
    bearer_lista = [bearer_token,bearer_token_2, bearer_token_3, bearer_token_4, bearer_token_5, bearer_token_6, bearer_token_7]
    
    global bearer_lista_2
    
    bearer_lista_2 = bearer_lista.copy()
    
    start_time = yesterday.isoformat()[:-3]+'Z'
    end_time = today.isoformat()[:-3]+'Z'
    try:
        ## CARGAR JSON CON DATOS
        
        json_response,bearer_lista = connect_to_endpoint(keyword,bearer_lista,bearer_token, start_time, end_time)
        if 'title' in json_response:
            if json_response['title'] == 'Invalid Request':
                print('Invalid Request')
                raise Exception('hubo un problema')
        json_response['ID'] = topic_id#Topic_ID json y api tiene el nombre diferente
        json_response['date'] = start_time
        request_count = 1
        #print(json_response)
        
        if 'meta' in  json_response:
            while 'next_token' in json_response['meta'].keys() and request_count<3:
                #print(request_count)
                next_token = json_response['meta']['next_token']
                #print('\n',next_token)
                if next_token==None:
                    print('token is none')
                    break
    
                json_response_next_token,bearer_lista = connect_to_endpoint(keyword,bearer_lista,bearer_token, start_time, end_time, next_token)                
                #print('\n',json_response_next_token)
                if 'data' in json_response_next_token:
                    #print('\n','data in response')
                    json_response['data'] += json_response_next_token['data']
    
                    if 'includes' in json_response_next_token:
                        if 'tweets' in  json_response_next_token['includes']:
                            json_response['includes']['tweets'] += json_response_next_token['includes']['tweets']
    
                        if 'users' in  json_response_next_token['includes']:
                            json_response['includes']['users'] += json_response_next_token['includes']['users']
    
                        if 'meta' in  json_response_next_token:
                            json_response['meta'] = json_response_next_token['meta']
                else:
                    print('no trae data')
                    break
    
                print(request_count,'cont****',end="\r")
                request_count +=1
                time.sleep(0.25)
            
        ## PERFILAR Y PROCESAR DATOS
        
        df = json_to_pandas(json_response,topic_id)
        cuentas,puntuacion,df = perfilamiento(df)
        
        
        df['_id']=0
        df['_id']=df['_id'].map(lambda x: str(uuid4()))
        df['sentimiento']=None
        df['probabilidad_sentimiento']=None
        df['rrss']='twitter'
        df['suma_likes_post']=None
        
        df.rename(columns={'id':'id_pub', 'text':'texto', 'public_metrics.like_count':'likes_count',
                          'public_metrics.retweet_count':'retweet_count',
                          'followers_count':'seguidores'}, inplace=True)
        df['created_at'] = pd.to_datetime(df['created_at'])
        df['tiempo'] = df['created_at'].map(lambda x: x.replace(tzinfo=timezone.utc).timestamp())
        df = df[['_id','id_pub','texto','tiempo','likes_count','retweet_count','username','user_id','seguidores','fullname','topic_id',
                'tipo_usuario','sentimiento','probabilidad_sentimiento','rrss']]
                
        
        comentarios_name = '#'+keyword+'-date-'+str(dt.date.today())+'.csv' ###
        
        df.to_csv('/tmp/'+comentarios_name, encoding="utf-8")
        s3 = boto3.client('s3')
        bucket = "project-olimpo-horus"
        s3.upload_file(Filename='/tmp/'+comentarios_name, Bucket=bucket,Key="Narrativas/Twitter/data/clean/"+comentarios_name)
        lista_dynamo=[]
        lista_dynamo.append("Narrativas/Twitter/data/clean/"+comentarios_name)
        end = time.time()
        total = round((end-start)/60)
        
        return total, lista_dynamo
    
    except Exception as e:
        raise e
    
    