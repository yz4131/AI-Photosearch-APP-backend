import math
import dateutil.parser
import datetime
import time
import boto3
import json
import requests
from requests_aws4auth import AWS4Auth
import os


def searchFromES(keywords):
    photos = []
    region = 'us-east-1'
    service = 'es'
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

    host = 'https://search-photosearch-keg5p47cv2pwbkvyvalwgnv7lq.us-east-1.es.amazonaws.com'
    for key in keywords:
        index = 'photo'
        url = host + '/' + index + '/_search'
        query = {
            "size": 1000,
            "query": {
                "query_string": {
                "default_field": "labels",
                "query": key
                }
            },
            "sort":{
            "_script":{
                "script":"Math.random()",
                "type":"number",
                "order":"asc"
            }
        }
        }
        headers = { "Content-Type": "application/json" }

        r = requests.get(url, auth=awsauth, headers=headers, data=json.dumps(query))

        response = {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": '*'
            },
        "isBase64Encoded": False
        }

        response['body'] = r.text
        # print(response['body'])
        for item in json.loads(response['body'])['hits']['hits']:
            if item['_source']['objectKey'] not in photos:
                photos.append(item['_source']['objectKey'])
    # print(photos)
    return photos



def lambda_handler(event, context):
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    lex = boto3.client('lex-runtime')
    # print(event)
    keywords = []
    lex_res = lex.post_text(botName='photosearch',
                                botAlias='photosearch',
                                userId='testuser',
                                inputText=event['queryStringParameters']['q'])
    for item in lex_res['slots'].values():
        if item:
            if item.lower() not in keywords:
                keywords.append(item.lower())
    # print(keywords)
    photos = searchFromES(keywords)

    # to_urls
    urls = []
    for photo in photos:
        url = 'https://b2photostorage.s3.amazonaws.com/' + photo
        urls.append(url)
    res = {
        "isBase64Encoded": False,
        "statusCode": 200,
        "headers": {'Access-Control-Allow-Origin': '*'},
        "body": json.dumps(urls)
        }
    print(res)
    return res
