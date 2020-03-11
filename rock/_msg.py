import json
import msgpack


def mpack(data):
    return msgpack.packb(data)


def munpack(data):
    return msgpack.unpackb(data, raw=False)


def dumps(data):
    return json.dumps(data)


def loads(data):
    return json.loads(data)


class Base(object):
    header = None
    pack = None
    unpack = None

    @property
    def msg(self):
        return [self.header]

    def _recv(self, socket):
        msg = socket.recv_multipart()
        return msg[:-1], self.unpack(msg[-1])

    def _send(self, socket, data, identity):
        encoded = [self.pack(data)]
        socket.send_multipart(identity+encoded)

    def recv(self, socket):
        return self._recv(socket)

    def send(self, socket, data, identity=[]):
        self._send(socket, data, identity)


class Client(Base):
    def __init__(self, header):
        if header == b'json':
            self.pack = dumps
            self.unpack = loads
        elif header == b'mpack':
            self.pack = mpack
            self.unpack = munpack
