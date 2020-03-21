import textwrap

from . import utils


def static():
    return f"""
    [unix_http_server]
    file=/tmp/supervisor.sock

    [inet_http_server]
    port=localhost:9999

    [supervisord]

    [supervisord]
    logfile=%(here)s/logs/supervisor.log
    logfile_maxbytes=50MB
    logfile_backups=5
    loglevel=info
    pidfile=%(here)s/logs/supervisor.pid
    nodaemon=false
    minfds=4096
    minprocs=128

    [rpcinterface:supervisor]
    supervisor.rpcinterface_factory=supervisor.rpcinterface:make_main_rpcinterface

    [supervisorctl]
    serverurl=unix:///tmp/supervisor.sock
    """


def group(name, members):
    return f"""
    [group:{name}]
    programs={', '.join(members)}
    """


def broker(service, addr, verbose=''):
    return f"""
    [program:{service}.broker]
    command=rock.broker -s {service} -a {addr} {verbose}
    directory=%(here)s
    process_name=broker
    numprocs=1
    priority=1
    autostart=true
    autorestart=true
    startsecs=15
    stopwaitsecs=20
    killasgroup=true
    stdout_logfile=%(here)s/logs/{service}.broker.log
    stderr_logfile=%(here)s/logs/{service}.broker.log
    """


def service(name, workers=2):
    workers = min(workers, 8)
    return f"""
    [group:{name}]
    programs={name}.service,  {name}.broker

    [program:{name}.service]
    command=mybnbaid.{name}
    directory=%(here)s
    process_name=service-[%(process_num)02x]
    numprocs={workers}
    priority=2
    autostart=true
    autorestart=true
    startsecs=10
    stopwaitsecs=10
    killasgroup=true
    stdout_logfile=%(here)s/logs/{name}.service.log
    stderr_logfile=%(here)s/logs/{name}.service.log
    """


def gateway(workers=4):
    workers = min(workers, 10)
    return f"""
    [program:gateway.service]
    command=mybnbaid.gateway --port=88%(process_num)02d
    directory=%(here)s
    process_name=gateway.service-[%(process_num)02x]
    numprocs={workers}
    priority=3
    autostart=true
    autorestart=true
    startsecs=10
    stopwaitsecs=10
    killasgroup=true
    stdout_logfile=%(here)s/logs/gateway.service.log
    stderr_logfile=%(here)s/logs/gateway.service.log
    """


def supervisor(config='services.yml'):

    brokers = _utils.parse_config('brokers')
    verbose = '-v' if _utils.parse_config('verbose') == True else ''
    conf = _utils.parse_config('services')
    if conf:
        writer = (static(),)
        tmp = ()
        for name in conf:
            addr = brokers[name]
            workers = conf[name]['workers']
            writer += (broker(name, addr, verbose),)
            tmp += (service(name, workers),)
        writer += tmp

    conf = _utils.parse_config('gateway')
    if conf:
        writer += (gateway(conf['workers']),)

    if writer:
        with open('supervisord.conf', 'w') as file:
            for line in writer:
                file.write(textwrap.dedent(line))
