import sys
import platform
import signal
import functools
import inspect
import collections
import multiprocessing
import subprocess

from . import utils, mdp, sas, msg, repo

RequestParser = collections.namedtuple(
    'RequestParser', ('method', 'args')
)


GIT_INFO = dict(
    branch=subprocess.check_output(["git", "branch"]).strip()[1:],
    commit=subprocess.check_output(["git", "rev-parse", "HEAD"]).strip()
)
SYSTEM_INFO = dict(
    arch=platform.machine(),
    system=platform.system(),
    dist=' '.join(platform.dist())
)

PYTHON_INFO = dict(
    version=platform.python_version(),
    build=' '.join(platform.python_build()),
    compiler=platform.python_compiler()
)


class TaskError(Exception):
    pass


class Consumer(multiprocessing.Process):
    def __init__(self, tasks, queue, log):
        super(Consumer, self).__init__()
        self._tasks = tasks
        self._queue = queue
        self._log = log

    def _handler(self, cls, event, data):
        suffix = 'failed'
        try:
            task = cls._events.get(event, None)
            if task is None:
                raise TaskError(
                    f'no task exist for {event} event'
                )
            task(cls, data)
            suffix = 'passed'
        except Exception as err:
            self._log.exception(err)
        finally:
            self._log.info(
                f'`{task.__name__}` task {suffix}...'
            )

    def _recv(self):
        try:
            event, data = self._queue.get()
        except KeyboardInterrupt:
            event, data = (None, None)
        finally:
            return event, data

    def run(self):
        proc_name = self.name
        while True:
            event, data = self._recv()
            if event is None:
                # poision pill arrived
                self._log.info(f'{proc_name.lower()} poisoned, exiting...')
                break
            self._handler(self._tasks, event, data)
        return


class BaseTasks(object):
    _events = dict()

    def __new__(cls, *args, **kwargs):
        inst = super(BaseTasks, cls).__new__(cls)
        for task in dir(cls):
            if not task.startswith('_'):
                name = task.replace('_', ':')
                inst._events[name] = getattr(cls, task)
        return inst

    def test(self, data=None):
        pass


class BaseService(object):
    __slots__ = (
        '_worker', '_repo', '_cache',
        '_log', '_events', '_sas',
        '_queue', '_consumers'
    )

    _name = None
    _version = None
    _rpc = dict()
    _info = dict()

    def __new__(cls, *args, **kwargs):
        inst = super(BaseService, cls).__new__(cls)
        inst._info['service'] = inst._name
        inst._info['version'] = inst._version
        inst._info['git'] = GIT_INFO
        inst._info['python'] = PYTHON_INFO
        inst._info['system'] = SYSTEM_INFO
        inst._info['copyright'] = 'Mybnbaid, Inc 2019-2020'
        inst._info['rpc'] = []
        for rpc in dir(cls):
            if not rpc.startswith('_'):
                f = getattr(cls, rpc)
                inst._rpc[rpc] = f
                if rpc != 'info':
                    name = f.__name__
                    args = inspect.getargspec(f)[0][1:]
                    inst._info['rpc'].append(dict(method=name, args=args))
        return inst

    def __init__(self, conf):
        self._log = utils.logger(f'{self._name}.service')
        self._setup(conf)

    def _setup(self, conf):
        self._setup_cloud(conf['credentials'], conf['stage'])
        self._setup_worker(conf['broker'], conf['verbose'])
        self._setup_clients(conf['broker'], conf['verbose'])
        self._setup_service(conf['bucket'])
        self._log.info('service initialized...')
        signal.signal(signal.SIGTERM, self._close)

    def _setup_worker(self, broker, verbose=False):
        self._worker = mdp.worker.MajorDomoWorker(
            broker, self._name, verbose
        )

    def _setup_cloud(self, credentials, stage):
        self._sas = sas.AWSProvider(credentials, stage)

    def _setup_service(self, bucket):
        conf = self._sas.get_service_secret(self._name, bucket)
        if not conf:
            return

        if conf.get('repository', None):
            self._repo = repo.layers.SchemalessLayer(
                self._name, conf['repository']
            )

        if conf.get('authentication', None):
            self._setup_auth(conf['authentication'])

    def _setup_clients(self, broker, verbose):
        pass

    def _setup_tasks(self, tasks, max_workers=2):
        self._queue = multiprocessing.Queue()
        self._consumers = [
            Consumer(tasks, self._queue, self._log)
            for k in range(max_workers)
        ]

    def _start_consumers(self):
        for w in self._consumers:
            w.start()
        self._queue.put(('test', None))
        self._log.info('task workers up and running...')

    def _emit(self, event, data):
        self._queue.put((event, data))
        return dict(ok=True, details=f"{event} event emitted...")

    def _reply(self, request):
        try:
            data = utils.log_metrics(self, request)
        except Exception as err:
            data = utils.error(err)
        else:
            data['ok'] = True
        return [msg.pack(data)]

    def __call__(self):
        if hasattr(self, '_consumers'):
            self._start_consumers()

        self._log.info('service is ready...')
        reply = None
        while True:
            try:
                request = self._worker.recv(reply)
                if request is None:
                    break
            except KeyboardInterrupt:
                self._close()
            else:
                reply = self._reply(self._parse(request[-1]))

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self._close()

    def _parse(self, message):
        return RequestParser(**msg.unpack(message))

    def _cleanup(self):
        if hasattr(self, '_repo'):
            self._repo.close()
            self._log.info('persistent layer connetion closed...')

        if hasattr(self, '_consumers'):
            for proc in self._consumers:
                self._queue.put((None, None))
            self._log.info('consumers killed...')

        self._log.info('service terminated...')

    def _close(self, *args, **kwargs):
        self._cleanup()

    def info(self):
        return self._info
