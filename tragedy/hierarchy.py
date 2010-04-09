from .datastructures import (OrderedDict,)
from .util import (CASPATHSEP,
                  )
class Cluster(object):
    def __init__(self, name):
        self.keyspaces = OrderedDict()
        self.name = name
        
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

    def path(self):
        return u'%s%s%s' % (self.cluster.name, CASPATHSEP, self.name)

    def registerRowClass(self, name, rowclass):
        self.rowclasses[name] = rowclass

    def __str__(self):
        return self.name