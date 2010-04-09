import tragedy
client = tragedy.connect(['localhost:9160'])
from tragedy.hierarchy import Cluster, Keyspace
from tragedy.rows import BasicRow, DictRow, IndexRow
from tragedy.hacks import boot

bbqcluster = Cluster('BBQ Cluster')
bbqkeyspace = Keyspace('BBQ', bbqcluster)

class CachedURL(DictRow):
    class Meta:
        keyspace = bbqkeyspace
        column_family = 'CachedURL'
        client = client
        row_key_name = 'uuid'
        column_type = 'Standard'
        compare_with = 'BytesType'

class URLIndex(IndexRow):
    class Meta:
        keyspace = bbqkeyspace
        column_family = 'URLIndex'
        client = client
        row_key_name = 'url'
        column_type = 'Standard'
        compare_with = 'TimeUUIDType'

boot(bbqkeyspace)

cachedurl = CachedURL(row_key='ROWKEY')
cachedurl.update(data='OHLALA', hase='rabbit', viech='toll')
cachedurl.save()
print cachedurl

urlhistory = URLIndex('http://xkcd.com/4')
urlhistory.append(cachedurl)
urlhistory.save()

urlhistory = URLIndex('http://xkcd.com/4')
urlhistory.get_last_n_columns()
print urlhistory