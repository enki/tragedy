from .datastructures import (OrderedDict,)
from .util import (CASPATHSEP,
                   CrossModelCache,
                  )

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
        # Register us!
        new_cls._keyspace.register_model(getattr(new_cls, '_column_family', name), new_cls)
        
        from .rows import RowKey, Index
        
        for key, value in new_cls.__dict__.items():
            if isinstance(value, RowKey):
                value.prepare_referencing_class(new_cls, key)

            from .columns import AutoIndex
            if isinstance(value, AutoIndex):
                from .columns import ForeignKey
                class AutoIndexImplementation(Index):
                    _column_family = 'Auto_' + name + '_' + key
                    _klass = new_cls
                    row_key = RowKey(default=key)
                    targetmodel = ForeignKey(foreign_class=new_cls, compare_with='TimeUUIDType')
                    
                    @classmethod
                    def target_saved(cls, target):
                        cls().append(target).save()
                                                
                # print 'CREATED', AutoIndexImplementation._column_family
                setattr(new_cls, key, AutoIndexImplementation)
                new_cls.save_hooks.add(AutoIndexImplementation.target_saved)

        if hasattr(new_cls, 'targetmodel'):
            new_cls._default_field = new_cls.targetmodel

        return new_cls

class Cluster(object):
    def __init__(self, name):
        self.keyspaces = OrderedDict()
        self.name = name
        
        cmcache.append('clusters', self)
        
    def registerKeyspace(self, name, keyspc):
        self.keyspaces[name] = keyspc
        
    def __str__(self):
        return self.name

class Keyspace(object):
    def __init__(self, name, cluster):
        self.models = OrderedDict()
        self.name = name
        self.cluster = cluster
        cluster.registerKeyspace(self.name, self)
        
        cmcache.append('keyspaces', self)

    def path(self):
        return u'%s%s%s' % (self.cluster.name, CASPATHSEP, self.name)

    def register_model(self, name, model):
        self.models[name] = model

    def __str__(self):
        return self.name

    def verify_datamodel(self):
        for model in self.models.values():
            self.verify_datamodel_for_model(model)
    
    @staticmethod
    def verify_datamodel_for_model(cls):
        allkeyspaces = cls._client.describe_keyspaces()
        assert cls._keyspace.name in allkeyspaces, ("Cassandra doesn't know about " + 
                                    "keyspace %s (only %s)" % (cls._keyspace, allkeyspaces))
        mykeyspace = cls._client.describe_keyspace(cls._keyspace.name)
        assert cls._column_family in mykeyspace.keys(), "Cassandra doesn't know about ColumnFamily '%s'. Update your config and restart?" % (cls._column_family,)
        mycf = mykeyspace[cls._column_family]
        assert cls._column_type == mycf['Type'], "Cassandra expects Column Type '%s' for ColumnFamily %s. Tragedy thinks it is '%s'." % (mycf['Type'], cls._column_family, cls._column_type)
        remotecw = mycf['CompareWith'].rsplit('.',1)[1]
        assert cls._default_field.compare_with == remotecw, "Cassandra thinks ColumnFamily '%s' is sorted by '%s'. Tragedy thinks it is '%s'." % (cls._column_family, remotecw, cls._default_field.compare_with)
