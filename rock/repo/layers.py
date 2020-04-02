from abc import ABCMeta, abstractmethod, abstractproperty
from schemaless.client import PGlient

from . import cache


class PersistentLayer(object):
    __metaclass__ = ABCMeta

    # def __init__(self, db, cache):
    #    pass

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

    @abstractproperty
    def cache(self):
        pass


class SchemalessLayer(PersistentLayer):
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
