import rock as rk
import schemaless as sm


def syncdb(config='datastore.yml'):
    stores = rk.utils.read_config(config)
    dsn = rk.aws.get_db_secret()
    _, topics = rk.aws.get_client('sns', False, 'us-east-1')

    for key in stores:
        db = stores[key]
        recreate = db.get('recreate', None) == True
        with sm.utils.SchemalessManager(dsn[key], recreate=recreate) as client:

            # configure database
            client.run('config')

            # allocate schemas
            schemas = []
            if db.get('shard', None):
                schemas = topics.keys()

            if not schemas:
                schemas = [key]

            # create schema
            for schema in schemas:
                client.run('schema', name=schema.lower())

            # create tables
            for schema in schemas:
                tables = db.get('tables', [])
                for table in tables:
                    client.run('table', schema=schema.lower(), name=table)

            # create index
            for schema in schemas:
                indexes = db.get('indexes', [])
                for table in indexes:
                    for field, datatype in indexes[table]:
                        client.run(
                            'index', schema=schema.lower(), table=table,
                            field=field, dtype=datatype
                        )


def main():
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument(
        '-c', '--config', dest='config',
        help='datastore config file',
        default='datastore.yml'
    )
    options = parser.parse_args()
    syncdb(options.config)


if __name__ == "__main__":
    main()
