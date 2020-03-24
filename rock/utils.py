import os
import coloredlogs
import logging
import signal
import collections
import yaml
import functools

from multiprocessing import Process
from time import time, sleep
from jinja2 import (
    Environment, PackageLoader,
    select_autoescape
)
from tornado import ioloop

import schemaless as sm

from . import mdp
from . import aws
from . import msg
from . import zkit

coloredlogs.install()
FORMAT = "[%(name)s-%(process)d][%(levelname)1.1s %(asctime)s %(module)s:%(lineno)d] %(message)s"
logging.basicConfig(
    format=FORMAT, level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S"
)

DB = dict(
    schemaless=sm.client.PGClient,
    postgres=None, mong=None, mysql=None
)

CACHE = dict(memcached=None, redis=None)


class RPCError(Exception):
    pass


def error(err):
    return dict(
        ok=False, error=type(err).__name__, detail=str(err)
    )


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
        help='services configuration file',
        default='services.yml'
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


def create_socket_fd(name):
    pid = os.getpid()
    fd = f'/tmp/{name}.{pid}.sock'
    addr = f'ipc://{fd}'
    return addr, fd


def consumer_factory(cls, *args, **kwargs):
    with cls(*args, **kwargs) as consumer:
        consumer()


def producer_factory(cls, *args, **kwargs):
    with cls(*args, **kwargs) as producer:
        producer()


def send_request(client, service, method, data):
    req = msg.prepare(method, data)
    rep = msg.unpack(client.send(service, req)[-1])
    return rep


class BaseConsumer(object):
    __slots__ = (
        '_log', '_ctx', '_sock',
        '_sns', '_ses', '_topics', '_db'
    )
    _events = collections.OrderedDict()

    def __new__(cls, *args, **kwargs):
        inst = super(BaseConsumer, cls).__new__(cls)
        for event in dir(cls):
            if not event.startswith('_'):
                inst._events[event] = cls.__dict__[event]
        return inst

    def __init__(self, addr, name):
        self._db = None
        self._log = logger(f'{name}.service')
        self._ctx, self._sock = zkit.consumer(addr, self._handler)

    def _handler(self, message):
        try:
            payload = msg.unpack(message[-1])
            event = payload['event']
            data = payload.get('data', {})
            func = self._events[event]
            code = func(self, data)
        except Exception as err:
            self._log.exception(err)
            suffix = 'failed...'
        else:
            suffix = 'passed...'
        finally:
            self._log.info(f'`{event}` request {suffix}')

    def __call__(self):
        try:
            self._log.info('consumer started...')
            ioloop.IOLoop.instance().start()
        except KeyboardInterrupt:
            self._log.info('consumer interrupted...')

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.__clean()
        self._log.info('consumer terminated...')

    def __clean(self):
        if self._sock:
            self._sock.close()

        if self._ctx:
            self._ctx.term()

        if self._db:
            self._db.close()


class BaseProducer(object):
    __slots__ = ('_ctx', '_sock', '_log')
    PERIODIC = False

    def __init__(self, addr, name):
        self._ctx, self._sock = zkit.producer(addr)
        self._log = logger(f'{name}.service')
        self._log.info('producer started...')

    def push(self, payload):
        self._sock.send(msg.pack(payload))
        event = payload['event']
        res = dict(ok=True, details=f"{event} event sent...")
        return res

    def close(self):
        self.__clean()
        self._log.info('producer terminated...')

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.__clean()

    def __clean(self):
        if self._sock:
            self._sock.close()

        if self._ctx:
            self._ctx.term()


class EventsManager(object):
    __slots__ = (
        '_producer', '_consumer', '_periodic', '_pool', '_fd'
    )

    def __init__(self, name, prod, cons, workers):
        self._pool = []
        self.__setup(name, prod, cons, workers)

    def __setup(self, name, prod, cons, workers):
        addr, self._fd = create_socket_fd(name)
        self.__setup_producer(prod, addr, name)
        self.__setup_consumer(cons, addr, name, workers)

    def __setup_producer(self, prod, addr, name):
        cls, args = prod[0], prod[1:]
        if cls.PERIODIC == True:
            self._producer = functools.partial(
                producer_factory, cls, *args, addr, name,
            )
            self._pool = [
                Process(target=self._producer, args=())
            ]
        else:
            self._producer = cls(*args, addr, name)

    def __setup_consumer(self, cons, addr, name, workers):
        cls, args = cons[0], cons[1:]
        self._consumer = functools.partial(
            consumer_factory, cls, *args, addr, name
        )
        for num in range(workers):
            self._pool.append(
                Process(target=self._consumer, args=())
            )

    @property
    def producer(self):
        if type(self._producer) == 'functools.partial':
            return None
        return self._producer

    def start(self):
        [proc.start() for proc in self._pool]

    def close(self):
        for proc in self._pool:
            try:
                proc.close()
                proc.terminate()
            except AttributeError:
                pass

        if self._fd and os.path.exists(self._fd):
            os.remove(self._fd)


class BaseService(object):
    __slots__ = (
        '_worker', '_db', '_cache', '_clients',
        '_log', '_mem', '_sns', '_topics', '_ses',
        '_events'
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
        self._log = logger(f'{self}.service')
        self._worker = mdp.worker.MajorDomoWorker(
            brokers[str(self)], self._name, verbose
        )
        self.__clients(brokers, conf.get('clients'), verbose)
        self.__db(conf.get('db'))
        self.__cache(conf.get('cache'))
        self._log.info('service initialized...')

    def __clients(self, brokers, clients, verbose):
        if clients:
            self._clients = collections.OrderedDict()
            for client in clients:
                name = client.encode('utf-8')
                self._clients[name] = mdp.client.MajorDomoClient(
                    brokers[client], name, verbose
                )

    def __db(self, client=None):
        if client:
            dsn = aws.get_db_secret(str(self))
            self._db = DB[client](dsn)

    def __cache(self, client=None):
        if client:
            dsn = aws.get_cache_secret(str(self))
            self._cache = CACHE[client](dsn)

    def __reply(self, request):
        try:
            data = log_metrics(self, request)
        except Exception as err:
            data = error(err)
        else:
            data['ok'] = True
        return [msg.pack(data)]

    def __call__(self):
        if hasattr(self, '_events'):
            self._start_events()

        self._log.info('service is ready...')
        reply = None
        while True:
            request = self._worker.recv(reply)
            if request is None:
                break
            reply = self.__reply(
                msg.parse(request[-1])
            )

    def __str__(self):
        return self._name.decode('utf-8')

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.__clean()

    def __clean(self, *args, **kwargs):
        if self._db:
            self._db.close()

        if self._cache:
            self._cache.close()

        if hasattr(self, '_events'):
            if self._events:
                self._log.info('terminating events manager')
                self._events.close()
        self._log.info('service terminated...')

    def _setup_events(self, prod, cons, workers):
        self._events = EventsManager(
            str(self), prod, cons, workers
        )

    def _start_events(self):
        if self._events:
            self._events.start()
            sleep(3)

    def _send(self, client, method, data):
        req = msg.prepare(method, data)
        rep = self._clients[client].send(client, req)
        return msg.unpack(rep[-1])

    def _emit(self, event, data):
        payload = dict(event=event, data=data)
        return self._events.producer.push(payload)
