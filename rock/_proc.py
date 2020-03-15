import textwrap

from . import _utils


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


def device(service, name, fend, bend):
    return f"""
    [program:{service}.{name}]
    command=rock.device -s {service} -d {name} -f {fend} -b {bend}
    directory=%(here)s
    process_name={service}.{name}
    numprocs=1
    priority=1
    autostart=true
    autorestart=true
    startsecs=10
    stopwaitsecs=10
    killasgroup=true
    stdout_logfile=%(here)s/logs/{service}.{name}.log
    stderr_logfile=%(here)s/logs/{service}.{name}.log
    """


def service(name, numprocs=2):
    return f"""
    [program:{name}.service]
    command=mybnbaid.{name}
    directory=%(here)s
    process_name={name}.service-[%(process_num)02d]
    numprocs={numprocs}
    priority=2
    autostart=true
    autorestart=true
    startsecs=10
    stopwaitsecs=10
    killasgroup=true
    stdout_logfile=%(here)s/logs/{name}.service.log
    stderr_logfile=%(here)s/logs/{name}.service.log
    """


def gateway(numprocs=4):
    numprocs = min(numprocs, 10)
    return f"""
    [program:gateway.service]
    command=mybnbaid.gateway --ports=888%(process_num)01d
    directory=%(here)s
    process_name=gateway.service-[%(process_num)02d]
    numprocs=1{numprocs}
    priority=3
    autostart=true
    autorestart=true
    startsecs=10
    stopwaitsecs=10
    killasgroup=true
    stdout_logfile=%(here)s/logs/gateway.service.log
    stderr_logfile=%(here)s/logs/gateway.service.log
    """


def get_devices(devices):
    blk = ()
    mems = []
    for svc in devices:
        name, fend, bend = devices[svc]
        blk += (device(svc, name, fend, bend),)
        mems.append(f"{svc}.{name}")
    grps = (group('devices', mems),)
    return grps + blk


def get_services(services):
    blk = ()
    mems = []
    for name in services:
        blk += (service(name),)
        mems.append(f"{name}.service")
    grps = (group('services', mems),)
    return grps + blk


def get_gateway():
    blk = (gateway(),)
    grps = (group('gateway', ['gateway.service']),)
    return grps + blk


def supervisor(config='config.yml'):
    devices = _utils.parse_config('devices')
    services = _utils.parse_config('services')
    gateway = _utils.parse_config('gateway')

    writer = (static(),)
    writer += get_devices(devices)
    writer += get_services(services)
    writer += get_gateway()

    with open('supervisord.conf', 'w') as sup:
        for line in writer:
            sup.write(textwrap.dedent(line))
