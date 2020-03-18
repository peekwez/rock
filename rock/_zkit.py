import zmq
from zmq.eventloop.zmqstream import ZMQStream


# def context(*args):
#    return zmq.Context(*args)


# def reply(ctx, addr, handler):
#     sock = ctx.socket(zmq.REP)
#     sock.connect(addr)
#     sock.linger = 0
#     sock = ZMQStream(sock)
#     sock.on_recv(handler)
#     return sock


# def request(ctx, addr):
#     sock = ctx.socket(zmq.REQ)
#     sock.connect(addr)
#     sock.linger = 0
#     return sock


# def router(ctx, addr, handler):
#     sock = ctx.socket(zmq.ROUTER)
#     sock.connect(addr)
#     sock.linger = 0
#     sock = ZMQStream(sock)
#     sock.on_recv(handler)
#     return sock


# def dealer(ctx, addr):
#     sock = ctx.socket(zmq.DEALER)
#     sock = s.connect(addr)
#     sock.linger = 0
#     return sock


def producer(ctx, addr):
    sock = ctx.socket(zmq.PUSH)
    sock.connect(addr)
    return sock


def consumer(ctx, addr, handler):
    sock = ctx.socket(zmq.PULL)
    sock.connect(addr)
    sock = ZMQStream(sock)
    sock.on_recv(handler)
    return sock


def publisher(ctx, addr):
    sock = ctx.socket(zmq.PUB)
    sock.connect(addr)
    return sock


def subscriber(ctx, addr, handler, topic=b''):
    sock = ctx.socket(zmq.SUB)
    sock.connect(addr)
    sock.setsockopt(zmq.SUBSCRIBE, topic)
    sock = ZMQStream(sock)
    sock.on_recv(handler)
    return sock
