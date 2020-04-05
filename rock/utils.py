import os
import logging
import collections
import yaml
import time
import secrets
import bcrypt

from branca import Branca
from jinja2 import Environment, PackageLoader, select_autoescape

from . import rpc


FORMAT = "[%(name)s-%(process)d][%(levelname)1.1s %(asctime)s %(module)s:%(lineno)d] %(message)s"
logging.getLogger('botocore.credentials').setLevel(logging.CRITICAL)
logging.basicConfig(
    format=FORMAT, level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S"
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


def create_secret_keys(size=24):
    return secrets.token_urlsafe(size)


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


def parse_response(response, extras=None):
    meta = response.get('ResponseMetadata')
    code = (meta.get('HTTPStatusCode'),)
    if not extras:
        return code[0]

    for key in extras:
        code += (response.get(key, None),)
    return code


class AuthManager(object):
    __slots__ = ('_clients',)

    def __init__(self, secrets):
        items = []
        for key in secrets:
            items.append((key, Branca(secrets[key])))
        self._clients = dict(items)

    def create_token(self, key, uid):
        return self._clients[key].encode(str(uid))

    def verify_token(self, key, token, ttl=7200):
        uid = self._clients[key].decode(token, ttl)
        return int(uid)

    def encrypt_password(self, password, salt=None):
        if isinstance(password, str):
            password = password.encode('utf-8')
        if not salt:
            salt = bcrypt.gensalt(6)
        hashed = bcrypt.hashpw(password, salt)
        return salt, hashed
