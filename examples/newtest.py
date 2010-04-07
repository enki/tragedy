import tragedy
client = tragedy.connect(['localhost:9160'])
from tragedy.newapi import *

class CachedURL(BasicRow):
    class Meta:
        keyspace = 'BBQ'
        column_family = 'CachedURL'
        client = client
        row_key_name = 'uuid'

class URLIndex(BasicRow):
    class Meta:
        keyspace = 'BBQ'
        column_family = 'URLIndex'
        client = client
        row_key_name = 'url'

cachedurl = CachedURL()
cachedurl.update(uuid='ROWKEY', data='OHLALA') #hase='rabbit', viech='toll')
cachedurl.save()

urlhistory = URLIndex()
urlhistory['url'] = 'http://news.ycombinator.com/'
urlhistory['WHOOOAH'] = cachedurl['uuid']

print urlhistory