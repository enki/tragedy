from .datastructures import (OrderedDict,)
from .util import (CASPATHSEP,
                   CrossModelCache
                  )
                  
cmcache = CrossModelCache()

class InventoryType(type):
    def __new__(cls, name, bases, attrs):
        parents = [b for b in bases if isinstance(b, InventoryType)]
        new_cls = super(InventoryType, cls).__new__(cls, name, bases, attrs)
                
        if '__abstract__' in new_cls.__dict__:
            return new_cls
        
        new_cls._init_class()
        # Register us!
        new_cls._keyspace.registerRowClass(name, new_cls)
        
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
        self.rowclasses = OrderedDict()
        self.name = name
        self.cluster = cluster
        cluster.registerKeyspace(self.name, self)
        
        cmcache.append('keyspaces', self)

    def path(self):
        return u'%s%s%s' % (self.cluster.name, CASPATHSEP, self.name)

    def registerRowClass(self, name, rowclass):
        self.rowclasses[name] = rowclass

    def __str__(self):
        return self.name