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
        
    b = fields.String()
    c = fields.String()
    a = fields.String()

cachedurl = CachedURL(('b', 'one'),('c','two'),('a','three'))
cachedurl.save()
print 'saved>', cachedurl
cachedurl.load()
print 'loaded>', cachedurl

# empty = CachedURL(key='blah', data='bub', other='foo')
# print 'empty>', empty
# empty.save()
# empty.load()
# print 'full>', empty