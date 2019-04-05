import json
import os
import boto3
from urllib.parse import unquote_plus
from wand.image import Image

QUALITY = 80
BUCKET_NAME = 's3.bucket.name'  # TODO. 변경필요
UPLOAD_PREFIX = 'origin'
TARGET_PREFIX = 'optimized'
ACCESS_KEY = os.environ.get('ACCESS_KEY')
SECRET_KEY = os.environ.get('SECRET_KEY')
s3 = boto3.resource(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)


def download_image_from_s3(key, local_file_path):
    s3.meta.client.download_file(BUCKET_NAME, key, local_file_path)


def upload_image_from_s3(key, local_file_path):
    s3.meta.client.upload_file(local_file_path, BUCKET_NAME, key)


def image_optimize(filename, quality):
    with Image(filename=filename) as img:
        img.compression_quality = quality
        img.save(filename='{}.output'.format(filename))


def lambda_handler(event, context):
    quality = QUALITY
    records = event['Records']
    for record in records:
        bucket = record['s3']['bucket']['name']
        assert bucket == BUCKET_NAME
        key = unquote_plus(record['s3']['object']['key'])
        assert key.startswith('origin/')
        paths = key.split('/')

        origin_path_depth = len(UPLOAD_PREFIX.split('/'))
        new_file_name = key.replace('/', '.')
        local_file_path = '/tmp/{}'.format(new_file_name)
        compress_file_path = '{}/{}'.format(TARGET_PREFIX, '/'.join(paths[origin_path_depth:]))

        download_image_from_s3(key=key, local_file_path=local_file_path)
        image_optimize(filename=local_file_path, quality=quality)
        upload_image_from_s3(key=compress_file_path, local_file_path='{}.output'.format(local_file_path))

    return {
        'statusCode': 200,
        'body': json.dumps('Success')
    }
