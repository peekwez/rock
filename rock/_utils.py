import os
import coloredlogs
import logging
import signal
import collections
import yaml

from time import time
from jinja2 import (
    Environment, PackageLoader,
    select_autoescape
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


class RPCError(Exception):
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


def read_config(config):
    with open(config, 'r') as c:
        conf = yaml.safe_load(c)
    return conf


def parse_config(section):
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument(
        '-c', '--config', dest='config',
        help='service end point',
        default='config.yml'
    )
    options = parser.parse_args()
    conf = read_config(options.config)
    return conf.get(section, None)


def log_metrics(cls, req):
    start = time()
    f = cls._rpc[req.method]
    results = f(cls, **req.args)
    elapsed = 1000.*(time() - start)
    cls._log.info(f'{req.method} {elapsed:0.2f}ms')
    return results


class BaseService(object):
    __slots__ = (
        '_worker', '_db', '_cache', '_clients', '_ctx'
        '_ipc', '_unix', '_producer', '_consumers'
    )
    _name = None
    _version = None
    _rpc = collections.OrderedDict()

    def __new__(cls, *args, **kwargs):
        inst = super(BaseService, cls).__new__(cls)
        for rpc in dir(cls):
            if not rpc.startswith('_'):
                inst._rpc[rpc] = cls.__dict__[rpc]
        return inst

    def __init__(self, brokers, conf, verbose):
        self._db = None
        self._cache = None
        self.__setup(brokers, conf, verbose)

    def __setup(self, brokers, conf, verbose):
        name = self._name.decode('utf-8')
        self._log = logger(f'{name}.service')

        broker = brokers[name]
        self._worker = _mdp.worker.MajorDomoWorker(
            broker, self._name, verbose
        )
        self.__clients(brokers, conf.get('clients'), verbose)
        self.__db(name, conf.get('db'))
        self.__cache(name, conf.get('cache'))
        # self.__handle_signals()

        self._log.info('service initialized...')

    def __handle_signals(self):
        signal.signal(signal.SIGINT, self.__cleanup)
        signal.signal(signal.SIGTERM, self.__cleanup)

    def __clients(self, brokers, clients, verbose):
        if clients:
            self._clients = collections.OrderedDict()
            for client in clients:
                name = client.encode('utf-8')
                self._clients[client] = _mdp.client.MajorDomoClient(
                    brokers[client], name, verbose
                )

    def __db(self, name, client=None):
        if client:
            dsn = _aws.get_db_secret(name)
            self._db = DB[client](dsn)

    def __cache(self, name, client=None):
        if client:
            dsn = _aws.get_cache_secret(self._name)
            self._cache = CACHE[client](dsn)

    def _setup_ipc(self):
        self._unix = f'/tmp/mail.{os.getpid()}.sock'
        self._ipc = f'ipc://{self._unix}'

    def __reply(self, request):
        try:
            data = log_metrics(self, request)
        except Exception as err:
            data = error(err)
        else:
            data['ok'] = True
        return [_msg.pack(data)]

    def __start_consumers(self):
        if hasattr(self, '_consumers'):
            if self._consumers:
                for worker in self._consumers:
                    worker.start()

    def __call__(self):
        self.__start_consumers()
        self._log.info('service is ready for requests...')

        reply = None
        while True:
            request = self._worker.recv(reply)
            if request is None:
                break
            reply = self.__reply(
                _msg.parse(request[-1])
            )

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.__cleanup()

    def __cleanup(self, *arg, **kwargs):
        if self._db:
            self._db.close()

        if self._cache:
            self._cache.close()

        if hasattr(self, '_producer'):
            if self._producer:
                self._log.info('terminating producer...')
                self._producer.close()
                self._ctx.term()
                if os.path.exists(self._unix):
                    os.remove(self._unix)
                    self._log.info(f'{self._unix} removed...')

        if hasattr(self, '_consumers'):
            if self._consumers:
                self._log.info('terminating consumers...')
                for worker in self._consumers:
                    worker.terminate()

        self._log.info('service terminated...')
