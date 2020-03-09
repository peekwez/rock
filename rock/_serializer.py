import abc
import json
import msgpack


class ABC(abc.ABC):
    @abc.abstractmethod
    def encode(self, data):
        pass

    @abc.abstractmethod
    def decode(self, data):
        pass


class JSON(ABC):
    def encode(self, data):
        return json.dumps(data)

    def decode(self, data):
        return json.loads(data)


class MPACK(ABC):
    def encode(self, data):
        return msgpack.packb(data)

    def decode(self, data):
        return msgpack.unpackb(data, raw=False)
