from cassandra.ttypes import (KsDef,)
from .datastructures import (OrderedDict,)
from .util import (CASPATHSEP,
                   CrossModelCache,
                   popmulti,
                  )
from . import connection

cmcache = CrossModelCache()

possible_validate_args = (
                          ('auto_create_models', False),
                          ('auto_drop_keyspace', False),
                          ('auto_drop_columnfamilies', False),
                         )

class InventoryType(type):
    """This keeps inventory of the models created, and prepares the
       limited amount of metaclass magic that we do. keep this small!"""
    def __new__(cls, name, bases, attrs):
        parents = [b for b in bases if isinstance(b, InventoryType)]
        new_cls = super(InventoryType, cls).__new__(cls, name, bases, attrs)
                
        if '__abstract__' in new_cls.__dict__: 
            return new_cls
        else: # we're not abstract -> we're user defined and stored in the db
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
        self._first_iteration_in_this_cycle = False
        cluster.registerKeyspace(self.name, self)
        
        self.strategy_class = 'org.apache.cassandra.locator.RackUnawareStrategy'
        self.replication_factor = 1
        
        cmcache.append('keyspaces', self)

    def connect(self, *args, **kwargs):
        newkwargs = popmulti(kwargs, *possible_validate_args )
        self._client = connection.connect(*args, **kwargs)
        if newkwargs['auto_create_models']:
            self.verify_datamodel(**newkwargs)
            
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

    def verify_datamodel(self, **kwargs):
        self._first_iteration_in_this_cycle = True
        for model in self.models.values():
            self.verify_datamodel_for_model(model=model, **kwargs)
    
    @classmethod
    def verify_datamodel_for_model(cls, model, **kwargs):
        cls.verify_keyspace_for_model(model=model, **kwargs)
        cls.verify_columnfamilies_for_model(model=model, **kwargs)
        model._keyspace._first_iteration_in_this_cycle = False
        
    @classmethod
    def verify_keyspace_for_model(cls, model, **kwargs):
        first_iteration = model._keyspace._first_iteration_in_this_cycle
        
        client = model.getclient()
        allkeyspaces = client.describe_keyspaces()
        if first_iteration and model._keyspace.name in allkeyspaces and kwargs['auto_drop_keyspace']:
            print 'Autodropping keyspace %s' % (model._keyspace,)
            client.set_keyspace(model._keyspace.name) # this op requires auth
            client.system_drop_keyspace(model._keyspace.name)            
            allkeyspaces = client.describe_keyspaces()
            
        if not model._keyspace.name in allkeyspaces:
            print "Cassandra doesn't know about keyspace %s (only %s)" % (model._keyspace, allkeyspaces)
            if kwargs['auto_create_models']:
                print 'Creating...', kwargs['auto_create_models']
                model._keyspace.register_keyspace_with_cassandra()
    
    @classmethod
    def verify_columnfamilies_for_model(cls, model, **kwargs):
        first_iteration = model._keyspace._first_iteration_in_this_cycle
        
        client = model.getclient()
        if not client._keyspace_set:
            client.set_keyspace(model._keyspace.name)
            
        mykeyspace = client.describe_keyspace(model._keyspace.name)
        if first_iteration and kwargs['auto_drop_columnfamilies']:
            for cf in mykeyspace.keys():
                print 'Dropping %s...' % (cf,)     
                client.system_drop_column_family(model._keyspace.name, cf)
            mykeyspace = client.describe_keyspace(model._keyspace.name)
        if not model._column_family in mykeyspace.keys():
            print "Cassandra doesn't know about ColumnFamily '%s'." % (model._column_family,)
            if kwargs['auto_create_models']:
                print 'Creating...'
                model.register_columnfamiliy_with_cassandra()
                mykeyspace = client.describe_keyspace(model._keyspace.name)            
        mycf = mykeyspace[model._column_family]
        assert model._column_type == mycf['Type'], "Cassandra expects Column Type '%s' for ColumnFamily %s. Tragedy thinks it is '%s'." % (mycf['Type'], model._column_family, model._column_type)
        remotecw = mycf['CompareWith'].rsplit('.',1)[1]
        assert model._order_by == remotecw, "Cassandra thinks ColumnFamily '%s' is sorted by '%s'. Tragedy thinks it is '%s'." % (model._column_family, remotecw, model._order_by)
