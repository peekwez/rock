import msgpack


def pack(data):
    return msgpack.packb(data)


def unpack(data):
    return msgpack.unpackb(data, raw=False)
