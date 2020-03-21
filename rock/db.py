import schemaless as sm

from . import aws
from . import utils


def syncdb(config='datastore.yml'):
    stores = utils.read_config(config)
    dsn = aws.get_db_secret()
    _, topics = aws.get_client('sns', False, 'us-east-1')

    for key in stores:
        db = stores[key]
        recreate = db.get('recreate', None) == True
        with sm.utils.SchemalessManager(
                dsn[key], 'create', recreate) as client:

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
                tables = db.get('tables')
                for table in tables:
                    client.run('table', schema=schema.lower(), name=table)

            # create index
            for schema in schemas:
                indexes = db.get('indexes')
                for table in indexes:
                    for field, datatype in indexes[table]:
                        client.run(
                            'index', schema=schema.lower(), table=table,
                            field=field, dtype=datatype
                        )
