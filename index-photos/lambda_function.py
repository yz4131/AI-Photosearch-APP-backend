from requests_aws4auth import AWS4Auth
import json
import urllib.parse
import boto3
import requests

#   hello!!!!!!!!!!!!!!!!!!!!!!!!!
s3 = boto3.client('s3')
client=boto3.client('rekognition')


def lambda_handler(event, context):
    # print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    labels=[]
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        temp = s3.head_object(Bucket=bucket, Key=key)
        # print(temp['x-amz-meta-customLabels'])
        # print("CONTENT TYPE: " + response['x-amz-meta-customLabels'])
        predict = client.detect_labels(Image={'S3Object':{'Bucket':bucket,'Name':key}}, MaxLabels=4, MinConfidence=50)
        for label in predict['Labels']:
            labels.append(label['Name'])
        # print(labels)
        res = {
            'objectKey': key,
            'bucket': bucket,
            'createdTimestamp': temp['ResponseMetadata']['HTTPHeaders']['date'],
            'labels': labels
        }
        # res = json.dumps(res)
        # print(res)

        # post to es
        region = 'us-east-1'
        service = 'es'
        credentials = boto3.Session().get_credentials()
        awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
        host = 'https://search-photosearch-keg5p47cv2pwbkvyvalwgnv7lq.us-east-1.es.amazonaws.com'
        index = 'photo'
        type = 'lambda-type'
        url = host + "/" + index + "/" + type
        headers = { "Content-Type": "application/json" }
        r = requests.post(url, auth=awsauth, json=res, headers=headers)
        print(r.text)
    except Exception as e:
        print(e)
        raise e
