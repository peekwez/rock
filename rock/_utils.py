import logging
import collections
import yaml

from time import time
from jinja2 import Environment, PackageLoader, select_autoescape

FORMAT = "[%(name)s-%(process)d][%(levelname)1.1s %(asctime)s %(module)s:%(lineno)d] %(message)s"
logging.basicConfig(format=FORMAT)


Request = collections.namedtuple(
    'Request', ('method', 'args')
)
Email = collections.namedtuple(
    'Email', ('emails', 'subject', 'html', 'text')
)
SMS = collections.namedtuple(
    'SMS', ('action', 'topic', 'number', 'message')
)
SMS.__new__.__defaults__ = ('send', None, None, None)
parsers = {
    'request': Request,
    'email': Email,
    'sms': SMS
}


def parse(message, mtype):
    return parsers[mtype](**message)


def error(err):
    return {
        'ok': False,
        'error': type(err).__name__,
        'detail': str(err)
    }


def unpack(proto, message, mtype='request'):
    return message[:-1], parse(proto.unpack(message[-1]), mtype)


def prep(method, args):
    return {'method': method, 'args': args}


def logger(name, loglevel):
    log = logging.getLogger(name)
    log.setLevel(getattr(logging, loglevel))
    return log


class RPC(collections.OrderedDict):
    def register(self, endpoint):
        def wrapper(func):
            self.update({endpoint: func})
            return func
        return wrapper


def loader(package, template):
    return Environment(
        loader=PackageLoader(package, template),
        autoescape=select_autoescape(
            ['html', 'xml']
        )
    )


def render(loader, filename, context):
    template = loader.get_template(filename)
    return template.render(context)


def parse_config(section):
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument(
        '-c', '--config', dest='config',
        help='service end point',
        default='config.yml'
    )
    options = parser.parse_args()
    with open(options.config, 'r') as conf:
        cfg = yaml.safe_load(conf)
    return cfg[section]


def handle_rpc(message, rpc, proto, sock, log):
    # message : incoming request
    # rpc: dictionary rpc method to endpoints
    # proto: message protocol for unpacking
    # log: logger for the service handling request
    # start timer

    ibeg = time()

    # handle incoming request and send response
    sid, req = unpack(_proto, message, 'request')
    try:
        func = rpc[req.method]
        res = func(**req.args)
    except Exception as err:
        res = rk.utils.error(err)
        log.exception(err)
    else:
        res['ok'] = True
    finally:
        proto.send(sock, res, sid)

        # finish timer
        iend = rk.utils.time()
        elapsed = 1000*(iend-ibeg)
        _log.info(f'{req.method} >> {func.__name__} {elapsed:0.2f}ms')
