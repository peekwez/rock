from abc import ABCMeta, abstractmethod, abstractproperty
from schemaless.client import PGClient

from . import cache as mem


class PersistentLayer(object):
    __metaclass__ = ABCMeta
    _db = None
    _cache = None

    @abstractmethod
    def put(self, shard, table, data):
        pass

    @abstractmethod
    def get(self, shard, table, pk):
        pass

    @abstractmethod
    def filter(self, shard, table, params, **kwargs):
        pass

    @abstractmethod
    def edit(self, shard, table, pk, data):
        pass

    @abstractmethod
    def drop(self, shard, table, pk):
        pass

    @abstractmethod
    def close(self):
        pass

    @property
    def db(self):
        return self._db

    @db.setter
    @abstractmethod
    def db(self, val):
        self._db = val

    @property
    def cache(self):
        return self._cache

    @cache.setter
    @abstractmethod
    def cache(self, val):
        self._cache = val


class SchemalessLayer(PersistentLayer):

    def __init__(self, service, conf):
        self.db = PGClient(
            '{0}/{1}'.format(conf['database'], service)
        )
        self.cache = mem.CacheLayer(conf['cache'])

    def put(self, shard, table, data):
        return self.db.create(
            schema=shard, table=table,
            data=data
        )

    def get(self, shard, table, pk):
        return self.db.get(
            schema=shard, table=table,
            pk=pk
        )

    def filter(self, shard, table, params, **kwargs):
        return self.db.filter(
            schema=shard, table=table,
            params=params, **kwargs
        )

    def edit(self, shard, table, pk, data):
        return self.db.update(
            schema=shard, table=table,
            pk=pk, data=data
        )

    def drop(self, shard, table, pk):
        return self.db.delete(
            schema=shard, table=table,
            pk=pk
        )

    def close(self):
        self.db.close()
        self.cache.close()


class MongoLayer(PersistentLayer):
    pass


class MySQLLayer(PersistentLayer):
    pass


class FirebaseLayer(PersistentLayer):
    pass


class DynamoDBLayer(PersistentLayer):
    pass


class SpannerLayer(PersistentLayer):
    pass


class CosmosLayer(PersistentLayer):
    pass
