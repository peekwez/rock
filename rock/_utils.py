import coloredlogs
import logging
import collections
import yaml

from time import time
from jinja2 import (
    Environment, PackageLoader, select_autoescape
)

import schemaless as sm

from . import _mdp
from . import _aws
from . import _msg


coloredlogs.install()
FORMAT = "[%(name)s-%(process)d][%(levelname)1.1s %(asctime)s %(module)s:%(lineno)d] %(message)s"
logging.basicConfig(
    format=FORMAT, level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S"
)

DB = {
    'schemaless': sm.client.PGClient,
    'postgres': None,
    'mongo': None,
    'mysql': None
}

CACHE = {
    'memcached': None,
    'redis': None
}


class InvalidRPC(Exception):
    pass


def error(err):
    return {
        'ok': False,
        'error': type(err).__name__,
        'detail': str(err)
    }


def logger(name):
    log = logging.getLogger(name)
    return log


def loader(package, template):
    return Environment(
        loader=PackageLoader(package, template),
        autoescape=select_autoescape(
            ['html', 'xml', 'txt']
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
    return cfg.get(section, None)


def is_private(method):
    return method.startswith('_')


def log_metrics(cls, req):
    if is_private(req.method) == True:
        raise InvalidRPC("'{req.method}' rpc endpoint does not exist")

    start = time()
    f = getattr(cls, req.method)
    results = f(**req.args)
    elapsed = 1000.*(time() - start)
    cls.log.info(f'{req.method} {elapsed:0.2f}ms')

    return results


class BaseService(object):
    __slots__ = ('_db', '_cache')
    name = None
    version = None
    log = None

    def __init__(self, broker, db=None, cache=None, verbose=False):
        self.__setup(db, cache)
        self.worker = _mdp.mdwrkapi.MajorDomoWorker(
            broker, self.name, verbose
        )

    def __setup(self, db, cache):
        self._db = None
        self._cache = None
        if db:
            dsn = _aws.get_db_secret(db['name'])
            self._db = DB[db['client']](dsn)

        if cache:
            dsn = _aws.get_cache_secret(self.name)
            self._cache = CACHE[cache['client']](dsn)

    def __call__(self):
        reply = None
        while True:
            message = self.worker.recv(reply)
            request = _msg.unpack(message[-1])
            if request is None:
                break
            try:
                data = log_metrics(self, request)
            except Exception as err:
                data = error(err)
            else:
                data['ok'] = True
            response = [_msg.mpack(data)]
            reply = response

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if self._db:
            self._db.close()
        if self._cache:
            self._cache.close()
