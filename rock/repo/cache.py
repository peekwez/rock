import re
import zlib
import msgpack

from redis import Redis
from pymemcache.client.base import Client
from pymemcache.client.hash import HashClient

CLIENTS = {
    'redis': Redis,
    'memcached': Client,
    'memhashed': HashClient
}


def serialize(data):
    return zlib.compress(
        msgpack.packb(data, use_bin_type=True)
    )


def deserialize(data):
    return msgpack.unpackb(
        zlib.decompress(data), raw=False
    )


def parse(dsn):
    parts = re.split(':|//|@', dsn)
    params = [('driver', parts[0])]
    keys = ('port', 'host', 'password', 'user')
    # reverse the order for the parts
    for k, value in enumerate(parts[-1:1:-1]):
        params.append((keys[k], value))
    return dict(params)


def cache_factory(dsn):
    kwargs = parse(dsn)
    driver = kwargs.pop('driver')
    if driver == 'memcached':
        kwargs = {'server': [(kwargs['host'], kwargs['port'])]}
    return CLIENTS[driver](**kwargs)


class CacheLayer(object):
    def __init__(self, dsn):
        self._client = cache_factory(dsn)

    def set(self, key, value):
        if type(value) != str:
            value = serialize(value)
        return self._client.set(key, value)

    def get(self, key):
        value = self._client.get(key)
        try:
            return value.decode('utf-8')
        except UnicodeDecodeError:
            return deserialize(value)

    def delete(self, key):
        return self._client.delete(key)

    def close(self):
        self._client.close()
