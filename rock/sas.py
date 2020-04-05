import yaml
import boto3
import collections
import functools


class AWSProvider(object):

    def __init__(self, filename, stage):
        self._stage = stage
        with open(filename, 'r') as c:
            conf = yaml.safe_load(c)[stage]
        self._provider = functools.partial(
            boto3.client,
            aws_access_key_id=conf['aws_access_key_id'],
            aws_secret_access_key=conf['aws_secret_access_key'],
        )

    def get_client(self, service, region):
        client = self._provider(service_name=service, region_name=region)
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

    def get_service_secret(self, service, bucket):
        client = self._provider('s3')
        filename = f'secrets/{self._stage}/{service}.yml'
        try:
            response = client.get_object(Bucket=bucket, Key=filename)
            conf = yaml.safe_load(response['Body'])
        except:
            conf = None
        finally:
            return conf
