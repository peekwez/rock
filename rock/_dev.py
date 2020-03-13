import zmq

import rock as rk

DEVICES = {
    'queue': (zmq.QUEUE, zmq.ROUTER, zmq.DEALER),
    'forwarder': (zmq.FORWARDER, zmq.SUB, zmq.PUB),
    'streamer': (zmq.STREAMER, zmq.PULL, zmq.PUSH)
}


class Manager(object):
    def __init__(self, service, device, faddr, baddr):
        self._ctx = zmq.Context(1)
        self._name = device
        self._faddr = faddr
        self._baddr = baddr
        self._dev = DEVICES[device][0]
        self._log = rk.utils.logger(f'{service}.{device}', 'INFO')
        self.setup(device, faddr, baddr)

    def start(self):
        try:
            self._log.info(f'ZMQ {self._name} device started....')
            self._log.info(f'Frontend listening at {self._faddr} ...')
            self._log.info(f'Backend listening at {self._baddr} ...')
            zmq.device(self._dev, self._fend, self._bend)
        except Exception as err:
            self._log.error(err)
            self._log.info('Bringing ZMQ device down')

    def setup(self, device, faddr, baddr):
        self._log.info('Setting up ZMQ device...')
        self.__create(*DEVICES[device])
        self.__bind(faddr, baddr)
        self.__setopts(device)

    def __setopts(self, device):
        if device == 'queue':
            self._fend.setsockopt(zmq.IDENTITY, b'ROUTER')
            self._bend.setsockopt(zmq.IDENTITY, b'DEALER')
        elif device == 'forwarder':
            self._fend.setsockopt(zmq.SUBSCRIBE, b'')

    def __create(self, type, ftype, btype):
        self._fend = self._ctx.socket(ftype)
        self._bend = self._ctx.socket(btype)

    def __bind(self, faddr, baddr):
        self._fend.bind(faddr)
        self._bend.bind(baddr)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self._fend.close()
        self._bend.close()
        self._ctx.term()
