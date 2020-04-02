import textwrap
import rock as rk


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


def broker(name, addr, verbose=''):
    return f"""
    [group:broker]
    programs={name}

    [program:{name}]
    command=rock.broker -n {name} -a {addr} {verbose}
    directory=%(here)s
    process_name=broker
    numprocs=1
    priority=1
    autostart=true
    autorestart=true
    startsecs=15
    stopwaitsecs=20
    killasgroup=true
    stdout_logfile=%(here)s/logs/broker.log
    stderr_logfile=%(here)s/logs/broker.log
    """


def service(name, workers=2):
    workers = min(workers, 8)
    return f"""
    [group:{name}]
    programs={name}

    [program:{name}]
    command=mybnbaid.{name}
    directory=%(here)s
    process_name=worker[%(process_num)02d]
    numprocs={workers}
    priority=2
    autostart=true
    autorestart=true
    startsecs=10
    stopwaitsecs=10
    killasgroup=true
    stdout_logfile=%(here)s/logs/{name}.log
    stderr_logfile=%(here)s/logs/{name}.log
    """


def gateway(workers=4):
    workers = min(workers, 10)
    return f"""
    [group:gateway]
    programs=worker

    [program:worker]
    command=mybnbaid.gateway --port=88%(process_num)02d
    directory=%(here)s
    process_name=worker[%(process_num)02d]
    numprocs={workers}
    priority=3
    autostart=true
    autorestart=true
    startsecs=10
    stopwaitsecs=10
    killasgroup=true
    stdout_logfile=%(here)s/logs/gateway.log
    stderr_logfile=%(here)s/logs/gateway.log
    """


def supervisor(config='config.yml'):
    cfg = rk.utils.parse_config()
    if cfg:
        addr = cfg['broker']
        verbose = '-v' if cfg.get('verbose', None) == True else ''
        writer = (static(), broker('mybnbaid', addr, verbose))
        for name in cfg:
            if name not in ('verbose', 'broker', 'gateway'):
                workers = cfg[name].get('workers', 1)
                writer += (service(name, workers),)
        writer += (gateway(cfg.get('gateway')['workers']),)

    if writer:
        with open('supervisord.conf', 'w') as file:
            for line in writer:
                file.write(textwrap.dedent(line))


def main():
    from argparse import ArgumentParser
    parser = ArgumentParser()

    parser.add_argument(
        '-c', '--config', dest='config',
        help='Configuration that connects to device',
        required=True
    )
    options = parser.parse_args()
    supervisor(options.config)


if __name__ == "__main__":
    main()
