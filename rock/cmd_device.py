import rock as rk


def main():
    # parse arguments
    from argparse import ArgumentParser
    parser = ArgumentParser()

    parser.add_argument(
        '-s', '--service', dest='service',
        help='Service that connects to device',
        required=True
    )

    parser.add_argument(
        '-d', '--device', dest='device',
        help='ZMQ device type',
        choices=('queue', 'forwarder', 'streamer'),
        required=True
    )
    parser.add_argument(
        '-f', '--faddr', dest='faddr',
        help='ZMQ device frontend address',
        required=True
    )
    parser.add_argument(
        '-b', '--baddr', dest='baddr',
        help='ZMQ device backend address',
        required=True
    )

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
