import msgpack
import collections

RequestParser = collections.namedtuple(
    'RequestParser', ('method', 'args')
)


def pack(data):
    return msgpack.packb(data)


def unpack(data):
    return msgpack.unpackb(data, raw=False)


def parse(message):
    return RequestParser(**unpack(message))


def prepare(method, args):
    return pack({'method': method, 'args': args})
