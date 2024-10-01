import pandas as pd
import praw
import datetime
import boto3
import time
import random

client = boto3.resource('dynamodb')
table = client.Table('AppiReddit')

# Función para determinar el tiempo que dirará el item en la tabla de dynamo
def expiration_time():
    return int(time.time()) + 7776000 # Tiempo en segundos


def lambda_handler(event, context):
    
    ## Leer las credenciales para conexión a reddit desde ssm agent
    AWS_REGION = "us-east-1"
    ssm_client = boto3.client("ssm", region_name=AWS_REGION)
    client_id = ssm_client.get_parameter(Name='cliend_id_reddit', WithDecryption=True)['Parameter'].get("Value")
    client_secret = ssm_client.get_parameter(Name='client_secret_api_reddit', WithDecryption=True)['Parameter'].get("Value")
    
    # Conectarse con reddit
    reddit = praw.Reddit(client_id = client_id,
    client_secret = client_secret,
    user_agent = "ProjectAPIv1.0"
    )
    
    # Definir la categorías y los subreddits que pertenecen a ellas
    categorias = {
        "sports": ["nba", "nfl", "sports", "soccer", "worldcup"],
        "tech": ["technology", "learnprogramming", "computing", "programming", "analytics"],
        "educational": ["education", "languagelearning", "lectures", "HowTo", "suggestmeabook"],
        "business": ["marketing", "ecommerce", "startup", "investing", "entrepreneur"],
        "media": ["boxoffice", "marvelstudios", "movies", "news", "starwars"],
        "lifestyle": ["travel", "digitalnomad", "food", "diy", "lifehacks"],
        "finances": ["finance", "financialplanning", "economics", "investing", "cryptocurrency"],
        "productivity": ["productivity", "todayilearned", "selfimprovement", "getdisciplined", "lifeprotips"],
        "science": ["science", "datascience", "sciencefacts", "compsci", "physics"]
    }
    #Definir banco de imagenes y función para obtener link
    
 
    business = ['Business/1059088660.jpg','Business/1136358204.jpg','Business/1146500452.jpg','Business/1207287952.jpg','Business/1210536568.jpg','Business/1211322739.jpg','Business/1281882590.jpg','Business/1305308934.jpeg','Business/1353983621.jpg','Business/1458604.jpg','Business/175138169.jpg','Business/184949844.jpg','Business/2913425.jpg','Business/3003763.jpg','Business/519472146.jpg','Business/556565295.jpg','Business/631390457.jpeg','Business/740534869.jpg','Business/925171024.jpg','Business/991160798.jpg']
    educational = ['Educational/1066324992.jpg','Educational/1084170908.jpg','Educational/1096013734.jpg','Educational/1124686964.jpg','Educational/1128917940.jpg','Educational/11315.jpg','Educational/11648.jpg','Educational/11656.jpg','Educational/1167824392.jpg','Educational/1193062841.jpg','Educational/1198419911.jpeg','Educational/1200911287.jpg','Educational/1240129511.jpg','Educational/1444061.jpg','Educational/202122.jpg','Educational/305989.jpg','Educational/3445867.jpg','Educational/639407632.jpg','Educational/653876434.jpg','Educational/908952856.jpg']
    finances = ['Finances/10664.jpg','Finances/1129767810.jpg','Finances/1131724309.jpg','Finances/11335.jpg','Finances/1159551106.jpg','Finances/11617.jpg','Finances/11645.jpg','Finances/11675.jpg','Finances/1208820789.jpg','Finances/1248192315.jpg','Finances/1324869840.jpg','Finances/168445829.jpg','Finances/1899030.jpg','Finances/609179183.jpg','Finances/909482746.jpg','Finances/945957634.jpg','Finances/966192478.jpg','Finances/970158226.jpg','Finances/970533914.jpg','Finances/990909558.jpg']
    lifestyle = ['Lifestyle/1077203902.jpg','Lifestyle/1090694184.jpg','Lifestyle/1129469.jpg','Lifestyle/1143754799.jpg','Lifestyle/1168593610.jpg','Lifestyle/1321340.jpg','Lifestyle/1812471.jpg','Lifestyle/2083030.jpg','Lifestyle/2178417.jpg','Lifestyle/574227.jpg','Lifestyle/585859171.jpg','Lifestyle/640322114.jpg','Lifestyle/641416240.jpg','Lifestyle/780643.jpg','Lifestyle/868622860.jpg','Lifestyle/875332502.jpg','Lifestyle/900238342.jpg','Lifestyle/900250204.jpg','Lifestyle/994521488.jpg','Lifestyle/996020584.jpg']
    media = ['Media/11234.jpg','Media/1129802429.jpg','Media/1140252133.jpg','Media/11410.jpg','Media/1145040476.jpg','Media/1145166058.jpg','Media/11683.jpg','Media/11706.jpg','Media/1198709549.jpg','Media/1205964283.jpg','Media/1207391933.jpg','Media/1276464274.jpeg','Media/1302325694.jpeg','Media/1320277912.jpeg','Media/1911002.jpg','Media/514410157.jpg','Media/670884337.jpg','Media/814065.jpg','Media/886066.jpg','Media/902700050.jpg']
    productivity = ['Productivity/103017741.jpg','Productivity/1085079292.jpeg','Productivity/1127292924.jpg','Productivity/1147524.jpg','Productivity/1164884183.jpg','Productivity/1168878478.jpg','Productivity/1180594315.jpg','Productivity/1278738473.jpeg','Productivity/1291773135.jpg','Productivity/129179210.jpg','Productivity/1307705211.jpg','Productivity/1310818458.jpg','Productivity/1341970867.jpg','Productivity/2787921.jpg','Productivity/3248557.jpg','Productivity/507243777.jpg','Productivity/507829277.jpg','Productivity/577237157.jpg','Productivity/594381929.jpg','Productivity/926846200.jpg']
    science = ['Science/1036194352.jpg','Science/11309.jpg','Science/11327.jpg','Science/11618.jpg','Science/11654.jpg','Science/11673.jpg','Science/11727.jpg','Science/11736.jpg','Science/11737.jpg','Science/1261588129.jpg','Science/1282126173.jpeg','Science/1285620923.jpg','Science/1292392034.jpg','Science/1294891292.jpg','Science/2016606.jpg','Science/2097718.jpg','Science/558255329.jpg','Science/623973971.jpg','Science/652508383.jpg','Science/Microscope1.jpg']
    sports = ['Sports/10472.jpg','Sports/1062308570.jpeg','Sports/10676.jpg','Sports/1130580876.jpg','Sports/11433.jpg','Sports/11635.jpg','Sports/11638.jpg','Sports/1305631211.jpeg','Sports/2770997.jpg','Sports/2874065.jpg','Sports/485592116.jpg','Sports/498071405.jpg','Sports/498071413.jpg','Sports/521690223.jpg','Sports/556416645.jpg','Sports/756846.jpg','Sports/78776625.jpg','Sports/Alpine Skiing 1.jpg','Sports/Surfing15.jpg','Sports/Water_aerobics1.jpg']
    tech = ['Tech/1022182536.jpg','Tech/11343.jpg','Tech/11585.jpg','Tech/11601.jpg','Tech/11613.jpg','Tech/11685.jpg','Tech/11707.jpg','Tech/1186091451.jpg','Tech/1200281779.jpg','Tech/1201506516.jpg','Tech/1212653217.jpg','Tech/1218585444.jpg','Tech/1273518964.jpg','Tech/1277377092.jpg','Tech/1323554686.jpg','Tech/1345967858.jpeg','Tech/1814757.jpg','Tech/202101.jpg','Tech/3333129.jpg','Tech/876551248.jpg']
    base = "https://project-olimpo-epistemos-banco-imagenes.s3.amazonaws.com/Epistemo_Images/"
    
    def get_link(category):
        if category == "business":
            link = base + random.choice(business)
        elif category == "educational":
            link = base + random.choice(educational)
        elif category == "finances":
            link = base + random.choice(finances)
        elif category == "lifestyle":
            link = base + random.choice(lifestyle)
        elif category == "media":
            link = base + random.choice(media)
        elif category == "productivity":
            link = base + random.choice(productivity)
        elif category == "science":
            link = base + random.choice(science)
        elif category == "sports":
            link = base + random.choice(sports)
        elif category == "tech":
            link = base + random.choice(tech)
        elif category == "base":
            link = base + random.choice(base)
        return link
        
        
    ## Loop para traer má 5 hot posts que tengan más de 200 caracteres en el body

    for key in categorias.keys(): # Itera sobre cada categoría
    
        for subreddit_name in categorias.get(key): #Itera cada subreddit de todas las categorias definidas
            
            subreddit = reddit.subreddit(subreddit_name) # Traer el subreddit

            hot_reddit = subreddit.hot(limit= 100) #Traer los 100 primeros hot posts de ese subreddit
            
            print(f"Procesando categoria: {key}. Subreddit: {subreddit_name}")
            
            contador = 0 # Para contar los posts que cumplen más de 200 caracteres
            
            # Loop para traer las variables de cada post
            
            for submission in hot_reddit:
                
                if (contador  <= 1) and (len(submission.selftext) >= 250): #Chequear que hayan menos de 2 posts y que el post en cuestión tenga más de 250 caracteres
                        
                    # Fecha de creación
                    date_time = datetime.datetime.fromtimestamp(submission.created_utc)
                    date_time = date_time.strftime('%Y-%m-%d %H:%M:%S')
                    
                    ### Para traer la url de la imagen que acompaña el post
                    
                    url_image = '' # Iniciar con una url en blanco

                    if 'media_metadata' in dir(submission): #Si el atributo existe, se toma la información de la media

                        for elem in submission.media_metadata.values(): #Se revisan los elementos de la metadata
                
                            media = [elem2 for elem2 in elem.values()] #Se hace una lista con cada una de las calidades de imagen
                
                            break #Solo quiero revisar el primero, entonces se aborta la ejecución en la primera iteración
                
                        if media[1]=='Image':
                            dict_image = media[-2] #Tomar la mayor calidad disponible
                            list_image = [elem for elem in dict_image.values()] #Convertir a lista
                            url_image = list_image[2] #Tomar la url
        
                    if url_image == '':
                        url_image = get_link(key)
                    
                    # Escribir el item en la tabla de dynamo
                    
                    table.put_item(Item = {
                        'id_post': submission.id,
                        'title': submission.title,
                        'body': submission.selftext,
                        'created_post': date_time,
                        'ups': submission.ups,
                        'downs': submission.downs,
                        'num_comments': submission.num_comments,
                        'category': key,
                        'subreddit': subreddit_name,
                        'url_imagen': url_image,
                        'url_post': submission.url,
                        'ExpirationTime': expiration_time()        
                    })
                    
                    contador += 1
                    if contador > 1: #Para solo tener en cuenta 5 posts
                        break
                    
