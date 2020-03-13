import logging
import collections

from time import time
from jinja2 import Environment, PackageLoader, select_autoescape

FORMAT = "[%(name)s-%(process)d][%(levelname)1.1s %(asctime)s %(module)s:%(lineno)d] %(message)s"
logging.basicConfig(format=FORMAT)


Request = collections.namedtuple(
    'Request', ['method', 'args']
)
Email = collections.namedtuple(
    'Email', ['emails', 'subject', 'html', 'text']
)

parsers = {
    'request': Request,
    'email': Email
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
