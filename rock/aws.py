import os
import json
import boto3
import base64
import secrets
import collections

from botocore.exceptions import ClientError


_errors = [
    'DecryptionFailureException',
    'InvalidParameterException',
    'InvalidRequestException',
    'ResourceNotFoundException'
]


def get_client(service, use_session, region):
    session = boto3
    if use_session == True:
        session = boto3.session.Session()

    client = session.client(
        service_name=service, region_name=region
    )
    if service == 'sns':
        topics = collections.OrderedDict()
        groups = client.list_topics()['Topics']
        if groups:
            for topic in groups:
                arn = topic['TopicArn']
                name = arn.split(':')[-1]
                topics[name] = arn
        return client, topics
    return client


def create_token_secrets(secret_name, use_session=False, region='us-east-2'):
    client = get_client('secretsmanager', use_session, region)
    keys = {
        'LOGIN': secrets.token_urlsafe(24),
        'VERIFY': secrets.token_urlsafe(24),
        'RESET': secrets.token_urlsafe(24),
    }
    try:
        client.create_secret(
            Name=secret_name,
            SecretString=json.dumps(keys)
        )
    except ClientError as e:
        raise e


def get_secret(secret_name, use_session=False, region='us-east-2'):
    client = get_client('secretsmanager', use_session, region)
    try:
        res = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        if e.response['Error']['Code'] in _errors:
            raise e
    else:
        if 'SecretString' in res:
            secret = json.loads(res['SecretString'])
        else:
            secret = base64.b64decode(res['SecretBinary'])
        return secret


def get_token_secrets():
    prod = os.environ.get('PROD_ENV', False)
    name = 'PROD_TOKEN_SECRETS' if prod == True else 'DEV_TOKEN_SECRETS'
    secrets = get_secret(name)
    return secrets


def get_db_secret(db=None):
    prod = os.environ.get('PROD_ENV', False)
    name = 'PROD_DB' if prod == True else 'DEV_DBS'
    dsn = get_secret(name)
    if db:
        dsn = dsn[db]
    return dsn


def get_cache_secret(cache):
    prod = os.environ.get('PROD_ENV', False)
    name = 'PROD_CACHES' if prod == True else 'DEV_CACHES'
    dsn = get_secret(name)[db]
    return dsn
