from cassandra.ttypes import (KsDef,)
from .datastructures import (OrderedDict,)
from .util import (CASPATHSEP,
                   CrossModelCache,
                  )
from . import connection

cmcache = CrossModelCache()

class InventoryType(type):
    """This keeps inventory of the models created, and prepares the
       limited amount of metaclass magic that we do. keep this small!"""
    def __new__(cls, name, bases, attrs):
        parents = [b for b in bases if isinstance(b, InventoryType)]
        new_cls = super(InventoryType, cls).__new__(cls, name, bases, attrs)
                
        if '__abstract__' in new_cls.__dict__:
            return new_cls
        
        new_cls._init_class()
        new_cls._keyspace.register_model(getattr(new_cls, '_column_family', name), new_cls)

        return new_cls

class Cluster(object):
    def __init__(self, name):
        self.keyspaces = OrderedDict()
        self.name = name
        
        cmcache.append('clusters', self)
    
    def setclient(self, client):
        self._client = client
    
    def registerKeyspace(self, name, keyspc):
        self.keyspaces[name] = keyspc
        
    def __str__(self):
        return self.name

class Keyspace(object):
    def __init__(self, name, cluster):
        self.models = OrderedDict()
        self.name = name
        self.cluster = cluster
        self._client = None
        cluster.registerKeyspace(self.name, self)
        
        self.strategy_class = 'org.apache.cassandra.locator.RackUnawareStrategy'
        self.replication_factor = 1
        
        cmcache.append('keyspaces', self)

    def connect(self, auto_create_model=False, *args, **kwargs):
        self._client = connection.connect(*args, **kwargs)
        if auto_create_model:
            self.verify_datamodel(auto_create_model=auto_create_model)
        if not self._client._keyspace_set:
            self._client.set_keyspace(self.name)

    def getclient(self):
        assert self._client, "Keyspace doesn't have a connection."
        return self._client

    def path(self):
        return u'%s%s%s' % (self.cluster.name, CASPATHSEP, self.name)

    def register_model(self, name, model):
        self.models[name] = model

    def __str__(self):
        return self.name

    def register_keyspace_with_cassandra(self):
        ksdef = KsDef(name=self.name, strategy_class=self.strategy_class,
                      replication_factor=self.replication_factor,
                      cf_defs=[x.asCfDef() for x in self.models.values()], # XXX: create columns at the same time
                     )
        self.getclient().system_add_keyspace(ksdef)

    def verify_datamodel(self, auto_create_model=False):
        for model in self.models.values():
            self.verify_datamodel_for_model(model, auto_create_model=auto_create_model)
    
    @classmethod
    def verify_datamodel_for_model(cls, model, auto_create_model=False):
        cls.verify_keyspace_for_model(model, auto_create_model)
        cls.verify_columnfamilies_for_model(model, auto_create_model)
        
    @classmethod
    def verify_keyspace_for_model(cls, model, auto_create_model=False):
        client = model.getclient()
        allkeyspaces = client.describe_keyspaces()
        if not model._keyspace.name in allkeyspaces:
            print "Cassandra doesn't know about keyspace %s (only %s)" % (model._keyspace, allkeyspaces)
            if auto_create_model:
                print 'Creating...'
                model._keyspace.register_keyspace_with_cassandra()
    
    @classmethod
    def verify_columnfamilies_for_model(cls, model, auto_create_model=False):
        client = model.getclient()
        if not client._keyspace_set:
            client.set_keyspace(model._keyspace.name)
            
        mykeyspace = client.describe_keyspace(model._keyspace.name)            
        if not model._column_family in mykeyspace.keys():
            print "Cassandra doesn't know about ColumnFamily '%s'." % (model._column_family,)
            if auto_create_model:
                print 'Creating...'
                model.register_columnfamiliy_with_cassandra()
                mykeyspace = client.describe_keyspace(model._keyspace.name)            
        mycf = mykeyspace[model._column_family]
        assert model._column_type == mycf['Type'], "Cassandra expects Column Type '%s' for ColumnFamily %s. Tragedy thinks it is '%s'." % (mycf['Type'], model._column_family, model._column_type)
        remotecw = mycf['CompareWith'].rsplit('.',1)[1]
        assert model._default_field.compare_with == remotecw, "Cassandra thinks ColumnFamily '%s' is sorted by '%s'. Tragedy thinks it is '%s'." % (model._column_family, remotecw, model._default_field.compare_with)
