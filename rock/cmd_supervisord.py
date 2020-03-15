import rock as rk


def main():
    from argparse import ArgumentParser
    parser = ArgumentParser()

    parser.add_argument(
        '-c', '--config', dest='config',
        help='Configuration that connects to device'
    )
    options = parser.parser_args()
    rk.proc.supervisor(options.config)


if __name__ == "__main__":
    main()
