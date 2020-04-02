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


class CacheLayer(object):
    def __init__(self, client, *args, **kwargs):
        cls = CLIENTS[client]
        self._client = cls(*args, **kwargs)

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
