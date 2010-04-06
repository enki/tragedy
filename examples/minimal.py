import sys
import pycassa
sys.modules['tragedyclient'] = pycassa.connect(['localhost:9160'])
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
print 'saved>', cachedurl # saved> <CachedURL(blah): {"data": "woot"}>

empty = CachedURL(key='blah')
print 'empty>', empty # empty> <CachedURL(blah): {}>
empty.load()
print 'full>', empty # full> <CachedURL(blah): {"other": null, "data": "woot"}>