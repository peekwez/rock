import functools

from . import msg
from . import mdp


class RpcError(Exception):
    pass


class MethodProxy:
    def __init__(self, method, service, client):
        self._service = service
        self._method = method
        self._client = client

    def _send_reply(self, reply):
        if not reply:
            error = dict(
                ok=False, error='ResponseTimedOut',
                detail='service server may not be running'
            )
            return error
        return msg.unpack(reply[-1])

    def _prepare(self, args):
        return msg.pack(dict(method=self._method, args=args))

    def _send_request(self, **kwargs):
        request = self._prepare(kwargs)
        return self._client.send(self._service, request)

    def __call__(self, **kwargs):
        reply = self._send_request(**kwargs)
        return self._send_reply(reply)


class AsyncMethodProxy(MethodProxy):
    def _send_request(self, **kwargs):
        request = self._prepare(kwargs)
        self._client.send(self._service, request)
        return self._client.recv()


class BaseProxy(object):
    def __init__(self, client, method, broker, service, verbose=False):
        self._client = client(broker, service, verbose)
        self._service = service
        if type(service) == str:
            self._service = bytes(service, 'utf-8')
        self._method_cls = method

    def __getattr__(self, attr):
        return self._method_cls(
            attr, self._service, self._client
        )


def rpc_proxy_factory(client, method):
    return functools.partial(BaseProxy, client, method)


RpcProxy = rpc_proxy_factory(
    mdp.client.MajorDomoClient,
    MethodProxy
)
AsyncRpcProxy = rpc_proxy_factory(
    mdp.aclient.MajorDomoClient,
    AsyncMethodProxy
)
