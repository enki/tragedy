import tragedy
client = tragedy.connect(['localhost:9160'])
from tragedy.hierarchy import Cluster, Keyspace
from tragedy.rows import BasicRow
from tragedy.hacks import boot

bbqcluster = Cluster('BBQ Cluster')
bbqkeyspace = Keyspace('BBQ', bbqcluster)

class CachedURL(BasicRow):
    class Meta:
        keyspace = bbqkeyspace
        column_family = 'CachedURL'
        client = client
        row_key_name = 'uuid'
        column_type = 'Standard'
        compare_with = 'BytesType'

class URLIndex(BasicRow):
    class Meta:
        keyspace = bbqkeyspace
        column_family = 'URLIndex'
        client = client
        row_key_name = 'url'
        column_type = 'Standard'
        compare_with = 'TimeUUIDType'

boot(bbqkeyspace)

cachedurl = CachedURL()
cachedurl.update(uuid='ROWKEY', data='OHLALA') #hase='rabbit', viech='toll')
cachedurl.save()

urlhistory = URLIndex()
urlhistory['url'] = 'http://news.ycombinator.com/'
urlhistory['WHOOOAH'] = cachedurl.get_reference()

print urlhistory