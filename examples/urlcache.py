import tragedy
tragedy.connect(['localhost:9160'])

from tragedy.models import Model
from tragedy import fields

class CachedURL(Model):        
    class Meta:
        keyspace = 'BBQ'
        column_family = 'URLCache'
        generate_rowkey_if_empty = True # if no rowkey is specified, use UUID
        
    data = fields.String()
    other = fields.String(required=False)

cachedurl = CachedURL(key='blah')
cachedurl.data = 'woot'
cachedurl.save()
print 'saved>', cachedurl

empty = CachedURL(key='blah', data='bub', other='foo')
print 'empty>', empty
empty.save()
empty.load()
print 'full>', empty