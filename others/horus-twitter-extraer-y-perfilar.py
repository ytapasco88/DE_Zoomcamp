import twitter

def lambda_handler(event, context):
    hashtag = event['keyword'].lower().split()
    hashtag=''.join(hashtag)
    topic_id = event['topic'].lower().split()
    topic_id=''.join(topic_id)
    try:
        total, lista_dynamo = twitter.datos(hashtag,topic_id)
        return {'hashtag':hashtag,'lista_dynamo':lista_dynamo, 'minutos': total}
    except Exception as e:
        raise e
