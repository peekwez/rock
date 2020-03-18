import json
import msgpack
import collections

Request = collections.namedtuple(
    'Request', ('method', 'args')
)


def mpack(data):
    return msgpack.packb(data)


def munpack(data):
    return msgpack.unpackb(data, raw=False)


def dumps(data):
    return json.dumps(data)


def loads(data):
    return json.loads(data)


def parse(message):
    return Request(**message)


def unpack(message):
    return parse(munpack(message))


def pack(method, args):
    return mpack({'method': method, 'args': args})
