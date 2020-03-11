import logging
import collections

from time import time


FORMAT = "[%(name)s-%(process)d][%(levelname)1.1s %(asctime)s %(module)s:%(lineno)d] %(message)s"
logging.basicConfig(format=FORMAT)


Request = collections.namedtuple(
    'Request', ['method', 'args']
)


def parse(message):
    return Request(**message)


def error(err):
    return {
        'ok': False,
        'error': type(err).__name__,
        'detail': str(err)
    }


def split(msgobj, message):
    return message[:-1], parse(msgobj.unpack(message[-1]))


def handle(rpc, sock, msgobj, message, log=None):
    start = time() if log else 0
    identity, req = split(msgobj, message)
    try:
        func = rpc[req.method]
        res = func(**req.args)
    except Exception as err:
        res = error(err)
        log.exception(err)
    else:
        res['ok'] = True
    msgobj.send(sock, res, identity)

    if log:
        elapsed = int(1e6*(time()-start))
        log.info(f'{req.method} {elapsed}\u03BCs')


def prep(method, args):
    return {'method': method, 'args': args}


def logger(name, loglevel):
    log = logging.getLogger(name)
    log.setLevel(getattr(logging, loglevel))

    ch = logging.StreamHandler()
    ch.setLevel(loglevel)
    ch.setFormatter(FORMAT)
    log.addHandler(ch)

    return log


class RPC(collections.OrderedDict):
    def register(self, endpoint):
        def wrapper(func):
            self.update({endpoint: func})
            return func
        return wrapper
