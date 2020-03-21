import zmq
from zmq.eventloop.zmqstream import ZMQStream


def producer(addr):
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PUSH)
    sock.bind(addr)
    return ctx, sock


def consumer(addr, handler):
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PULL)
    sock.connect(addr)
    sock.linger = 0
    sock = ZMQStream(sock)
    sock.on_recv(handler)
    return ctx, sock
