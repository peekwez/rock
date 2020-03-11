import boto3
import base64
import secrets

from botocore.exceptions import ClientError

import rock as rk


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
            SecretString=rk.msg.dumps(keys)
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
            secret = rk.msg.loads(res['SecretString'])
        else:
            secret = base64.b64decode(res['SecretBinary'])
        return secret
