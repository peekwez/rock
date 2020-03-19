import rock as rk


def main():
    """create and start new broker"""
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument(
        '-s', '--service', dest='service',
        help='service name', required=True
    )
    parser.add_argument(
        '-a', '--addr', dest='addr',
        help='broker port', default='tcp://*:5555'
    )
    parser.add_argument(
        '-v', '--verbose', dest='verbose',
        help='verbose logging', action='store_true'
    )
    options = parser.parse_args()
    broker = rk.mdp.broker.MajorDomoBroker(
        options.service, options.verbose
    )
    broker.bind(options.addr)
    broker.mediate()


if __name__ == "__main__":
    main()
