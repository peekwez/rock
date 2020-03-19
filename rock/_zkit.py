# import zmq
# from zmq.eventloop.zmqstream import ZMQStream

# brokers = {
#     'queue': (zmq.QUEUE, zmq.ROUTER, zmq.DEALER),
#     'forwarder': (zmq.FORWARDER, zmq.SUB, zmq.PUB),
#     'streamer': (zmq.STREAMER, zmq.PULL, zmq.PUSH)
# }


# def context(*args):
#     return zmq.Context(*args)


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


# # def broker(ctx, device, faddr, baddr):
# #     dtype, ftype, btype = brokers[device]
# #     frontend = ctx.socket(ftype)
# #     backend = ctx.socket(btype)
# #     if device == 'queue':
# #         frontend.setsockopt(zmq.IDENTITY, b'ROUTER')
# #         backend.setsockopt(zmq.IDENTITY, b'DEALER')
# #     elif device == 'forwarder':
# #         frontend.setsockopt(zmq.SUBSCRIBE, b'')
# #     frontend.bind(faddr)
# #     backend.bind(baddr)
# #     return frontend zmq.device(dtype, frontend, backend)


# def producer(ctx, addr):
#     sock = ctx.socket(zmq.PUSH)
#     sock.connect(addr)
#     return sock


# def consumer(ctx, addr, handler):
#     sock = ctx.socket(zmq.PULL)
#     sock.connect(addr)
#     sock = ZMQStream(sock)
#     sock.on_recv(handler)
#     return sock


# def publisher(ctx, addr):
#     sock = ctx.socket(zmq.PUB)
#     sock.connect(addr)
#     return sock


# def subscriber(ctx, addr, handler, topic=b''):
#     sock = ctx.socket(zmq.SUB)
#     sock.connect(addr)
#     sock.setsockopt(zmq.SUBSCRIBE, topic)
#     sock = ZMQStream(sock)
#     sock.on_recv(handler)
#     return sock
