import time
import tragedy
tragedy.connect(['localhost:9160'])

from tragedy.models import Model, cmcache, Cluster, Keyspace, Index
from tragedy.util import BestDictAvailable, unhandled_exception_handler
from tragedy import fields
from tragedy import management

BBQCluster = Cluster(name='BBQ Cluster')
BBQKeyspace = Keyspace(name='BBQ', cluster=BBQCluster)

# class Image(Model):
#     class Meta:
#         keyspace = BBQKeyspace

class URL(Model):        
    class Meta:
        keyspace = BBQKeyspace        
        randomly_indexed = True
        
    data = fields.String(required=False)
    title = fields.String(required=False)
    # images = fields.SetField()

class URLIndex(Index):
    class Meta:
        target = URL        

management.boot(BBQKeyspace)

url = CachedURL(data='OHAI')
url.save()
print url

# cachedurl = CachedURL(('b', 'one'),('c','two'),('a','three')) # kwargs also work, but don't preserve order
# cachedurl.save()
# print 'saved>', cachedurl
# cachedurl.load()
# print 'loaded>', cachedurl
# 
# empty = CachedURL(key=None, a='bub', c='foo')
# print 'empty>', empty
# empty.save()
# empty.load()
# print 'full>', empty