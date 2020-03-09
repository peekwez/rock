import abc

from . import serializer


class Message(object):
    header = None
    _slz = None

    @property
    def msg(self):
        return [self.header]

    def decode(self, msg):
        return self._slz.decode(msg)

    def _recv(self, socket):
        msg = socket.recv_multipart()
        return msg[:-1], self.decode(msg[-1])

    def _send(self, socket, data, identity):
        encoded = [self._slz.encode(data)]
        socket.send_multipart(identity+encoded)

    def recv(self, socket):
        return self._recv(socket)

    def send(self, socket, data, identity=[]):
        self._send(socket, data, identity)


class JSONMessage(Message):
    header = b'json'
    _slz = serializer.JSON()


class MPACKMessage(Message):
    header = b'mpack'
    _slz = serializer.MPACK()
