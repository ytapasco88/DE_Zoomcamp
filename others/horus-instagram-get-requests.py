import json
import requests
import re

def lambda_handler(event, context):
    # TODO implement
    espera=3
    while True:
        try:
            response = requests.get('http://horusdocker-env.eba-qybs9ycc.us-east-1.elasticbeanstalk.com/v1/api/key-parse')
            response = response.json()
            print(response)
        except ValueError:
            if attempt < 4:
                time.sleep(espera)
                espera*=2
                continue
            else:
                raise
        break
    
    json_final={}
    json_final['msg']=response['msg']
    json_final['data']=[]
    
    for i in response['data']:
        x=i['keyword'][0]
        x= re.sub(' OR ',',',x)
        x = re.sub(r'[(]','',x)
        x=re.split(r'[)]',x)
        i['transversal']=re.sub('\"','',x[1]).strip()
        i['keyword']=re.split(',',re.sub('"','',x[0]).strip())#'['+.strip()+']'
        
        if i['keyword']!=[""]:
            json_final['data'].append(i)

    return json_final
#