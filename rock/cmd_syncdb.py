import rock as rk


def main():
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument(
        '-c', '--config', dest='config',
        help='datastore config file',
        default='datastore.yml'
    )
    options = parser.parse_args()
    rk.db.syncdb(options.config)


if __name__ == "__main__":
    main()
