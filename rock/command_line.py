import rock as rk


def addparser():

    from argparse import ArgumentParser
    parser = ArgumentParser()

    parser.add_argument(
        '-s', '--service', dest='service',
        help='Service that connects to device'
    )

    parser.add_argument(
        '-d', '--device', dest='device',
        help='ZMQ device type',
        choices=('queue', 'forwarder', 'streamer')
    )
    parser.add_argument(
        '-f', '--faddr', dest='faddr',
        help='ZMQ device frontend address'
    )
    parser.add_argument(
        '-b', '--baddr', dest='baddr',
        help='ZMQ device backend address'
    )
    return parser


def main():
    # parse arguments
    parser = addparser()
    options = parser.parse_args()

    # run appropriate command
    args = (
        options.service, options.device,
        options.faddr, options.baddr
    )
    with rk.dev.Manager(*args) as dev:
        dev.start()


if __name__ == "__main__":
    main()
