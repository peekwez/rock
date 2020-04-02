#import rock as rk
from .. import mdp

def main():
    """create and start new broker"""
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument(
        '-n', '--name', dest='name',
        help='broker name', required=True
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
    broker = mdp.broker.MajorDomoBroker(
        options.name, options.verbose
    )
    broker.bind(options.addr)
    broker.mediate()


if __name__ == "__main__":
    main()
