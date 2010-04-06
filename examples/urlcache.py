import tragedy
tragedy.connect(['localhost:9160'])

from tragedy.models import Model
from tragedy.util import BestDictAvailable
from tragedy import fields

class CachedURL(Model):        
    class Meta:
        keyspace = 'BBQ'
        column_family = 'URLCache'
        generate_rowkey_if_empty = True # if no rowkey is specified, use UUID
        
    b = fields.String(required=False)
    c = fields.String()
    a = fields.String() # these get sorted by the database on insert

cachedurl = CachedURL(('b', 'one'),('c','two'),('a','three')) # kwargs also work, but don't preserve order
cachedurl.save()
print 'saved>', cachedurl
cachedurl.load()
print 'loaded>', cachedurl

empty = CachedURL(key=None, a='bub', c='foo')
print 'empty>', empty
empty.save()
empty.load()
print 'full>', empty