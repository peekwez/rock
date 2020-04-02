import os
import logging
import signal
import collections
import yaml
import functools
import time

from multiprocessing import Process
from jinja2 import Environment, PackageLoader, select_autoescape
from tornado import ioloop

import schemaless as sm

from . import mdp, aws, msg, zkit, rpc


FORMAT = "[%(name)s-%(process)d][%(levelname)1.1s %(asctime)s %(module)s:%(lineno)d] %(message)s"
logging.getLogger('botocore.credentials').setLevel(logging.CRITICAL)
logging.basicConfig(
    format=FORMAT, level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S"
)

DB = dict(
    schemaless=sm.client.PGClient,
    postgres=None, mong=None, mysql=None
)

CACHE = dict(memcached=None, redis=None)

RequestParser = collections.namedtuple(
    'RequestParser', ('method', 'args')
)
EventParser = collections.namedtuple(
    'EventParser', ('event', 'data')
)


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
        autoescape=select_autoescape(['html', 'xml', 'txt'])
    )


def render(loader, filename, context):
    template = loader.get_template(filename)
    return template.render(context)


def read_config(config):
    with open(config, 'r') as c:
        conf = yaml.safe_load(c)
    return conf


def parse_config(section=None):
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument(
        '-c', '--config', dest='config',
        help='service configuration file',
        default='config.yml'
    )
    options = parser.parse_args()
    conf = read_config(options.config)
    if section:
        return conf.get(section, None)
    return conf


def log_metrics(cls, req):
    start = time.time()
    try:
        f = cls._rpc[req.method]
    except KeyError:
        raise rpc.RpcError(f'`{req.method}` rpc endpoint does not exist')
    results = f(cls, **req.args)
    elapsed = 1000.*(time.time() - start)
    cls._log.info(f'{req.method} {elapsed:0.2f}ms')
    return results


def create_socket_fd(name):
    pid = os.getpid()
    fd = f'/tmp/{name}-{pid}.sock'
    addr = f'ipc://{fd}'
    return addr, fd


def socket_factory(cls, *args, **kwargs):
    with cls(*args, **kwargs) as socket:
        socket()


class BaseConsumer(object):
    __slots__ = (
        '_log', '_ctx', '_sock', '_db'
        '_sns', '_ses', '_topics'
    )
    _events = collections.OrderedDict()

    def __new__(cls, *args, **kwargs):
        inst = super(BaseConsumer, cls).__new__(cls)
        for event in dir(cls):
            if not event.startswith('_'):
                name = event.replace('_', ':')
                inst._events[name] = cls.__dict__[event]
        return inst

    def __init__(self, addr, name):
        self._db = None
        self._log = logger(f'{name}.service')
        self._ctx, self._sock = zkit.consumer(addr, self._handler)
        signal.signal(signal.SIGTERM, self._close)

    def _handler(self, message):
        try:
            req = self._parse(message[-1])
            func = self._events[req.event]
            code = func(self, req.data)
        except Exception as err:
            self._log.exception(err)
            suffix = 'failed...'
        else:
            suffix = 'passed...'
        finally:
            self._log.info(f'`{req.event}` event handling {suffix}')

    def _parse(self, message):
        return EventParser(**msg.unpack(message))

    def __call__(self):
        try:
            self._log.info(f'consumer started...')
            ioloop.IOLoop.instance().start()
        except KeyboardInterrupt:
            self._log.info(f'consumer interrupted...')

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self._close()

    def _close(self, *args, **kwargs):
        if self._sock:
            self._sock.close()

        if self._ctx:
            self._ctx.term()

        if self._db:
            self._db.close()

        self._log.info(f'consumer terminated...')


class BaseProducer(object):
    __slots__ = ('_ctx', '_sock', '_log')
    PERIODIC = False

    def __init__(self, addr, name):
        self._ctx, self._sock = zkit.producer(addr)
        self._log = logger(f'{name}.service')
        self._log.info(f'producer started...')
        signal.signal(signal.SIGTERM, self._close)

    def push(self, payload):
        self._sock.send(msg.pack(payload))
        event = payload['event']
        res = dict(ok=True, details=f"{event} event sent...")
        return res

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self._close()

    def _close(self, *args, **kwargs):
        if self._sock:
            self._sock.close()

        if self._ctx:
            self._ctx.term()

        self._log.info(f'producer terminated...')


class EventsManager(object):
    __slots__ = (
        '_producer', '_consumer', '_log',
        '_periodic', '_pool', '_fd', '_term'
    )

    def __init__(self, name, prod, cons, workers):
        self._pool = []
        self._term = False
        self._log = logger(f'{name}.service')
        self._setup(name, prod, cons, workers)

    def _setup(self, name, prod, cons, workers):
        addr, self._fd = create_socket_fd(name)
        self._setup_producer(prod, addr, name)
        self._setup_consumer(cons, addr, name, workers)

    def _setup_producer(self, prod, addr, name):
        self._producer = None
        if isinstance(prod, functools.partial):
            self._pool.append(Process(
                target=prod, args=(addr, name))
            )
        elif issubclass(prod, BaseProducer):
            self._producer = prod(addr, name)

    def _setup_consumer(self, cons, addr, name, workers):
        for num in range(workers):
            self._pool.append(
                Process(target=cons, args=(addr, name))
            )

    @property
    def producer(self):
        return self._producer

    def start(self):
        [
            proc.start() for proc in self._pool
        ]

    def close(self):
        if self._term == True:
            return

        for proc in self._pool:
            try:
                proc.terminate()
            except AttributeError:
                pass
        self._log.info('processes terminated...')

        if self._producer:
            self._producer._close()

        if self._fd:
            if os.path.exists(self._fd):
                os.remove(self._fd)
                self._log.info('removing socket file descriptor...')

        self._term = True


class BaseService(object):
    __slots__ = (
        '_worker', '_db', '_cache', '_log', '_events'
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

    def __init__(self, cfg):
        broker = cfg['broker']
        conf = cfg[self._name]
        verbose = cfg.get('verbose') == None
        self._log = logger(f'{self._name}.service')
        self._setup(broker, conf, verbose)

    def _setup(self, broker, conf, verbose):
        self._worker = mdp.worker.MajorDomoWorker(
            broker, self._name, verbose
        )
        self._setup_clients(broker, verbose)
        self._setup_db(conf.get('db'))
        self._setup_cache(conf.get('cache'))
        self._log.info('service initialized...')
        signal.signal(signal.SIGTERM, self._close)

    def _reply(self, request):
        try:
            data = log_metrics(self, request)
        except Exception as err:
            data = error(err)
        else:
            data['ok'] = True
        return [msg.pack(data)]

    def __call__(self):
        if hasattr(self, '_events'):
            self._start_event_system()

        self._log.info('service is ready...')
        reply = None
        while True:
            request = self._worker.recv(reply)
            if request is None:
                break
            reply = self._reply(self._parse(request[-1]))

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self._close()

    def _parse(self, message):
        return RequestParser(**msg.unpack(message))

    def _cleanup(self):
        clients = (
            ('_db', 'database'),
            ('_cache', 'cache'),
            ('_events', 'event system')
        )
        for attr, client in clients:
            if hasattr(self, attr):
                obj = getattr(self, attr)
                obj.close()
                self._log.info(f'cleaning up {client} connection...')
        self._log.info('service terminated...')

    def _close(self, *args, **kwargs):
        self._cleanup()

    def _setup_clients(self, broker, verbose):
        pass

    def _setup_db(self, client=None):
        if client:
            dsn = aws.get_db_secret(self._name)
            self._db = DB[client](dsn)

    def _setup_cache(self, client=None):
        if client:
            dsn = aws.get_cache_secret(self._name)
            self._cache = CACHE[client](dsn)

    def _setup_event_system(self, prod, cons, workers):
        self._events = EventsManager(self._name, prod, cons, workers)

    def _start_event_system(self):
        if self._events:
            self._events.start()
            time.sleep(3)

    def _emit(self, event, data):
        payload = dict(event=event, data=data)
        return self._events.producer.push(payload)
