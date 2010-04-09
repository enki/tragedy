import tragedy
client = tragedy.connect(['localhost:9160'])
from tragedy.hierarchy import Cluster, Keyspace
from tragedy.rows import BasicRow, DictRow, IndexRow
from tragedy.columns import (BooleanColumnSpec,TimeUUIDColumnSpec)
from tragedy.hacks import boot

bbqcluster = Cluster('BBQ Cluster')
bbqkeyspace = Keyspace('BBQ', bbqcluster)

class CachedURL(DictRow):
    _default_spec = BooleanColumnSpec()

    mybool = BooleanColumnSpec()

class URLIndex(IndexRow):
    _default_spec = TimeUUIDColumnSpec(required=False)
    _compare_with = 'TimeUUIDType'
    
boot(bbqkeyspace)

cachedurl = CachedURL(row_key='ROWKEY')
cachedurl.update(data='OHLALA', hase='rabbit', viech='toll', mybool=False)
cachedurl.save()
print cachedurl

urlhistory = URLIndex('http://xkcd.com/5')
urlhistory.append(cachedurl)
urlhistory.save()

urlhistory = URLIndex('http://xkcd.com/5')
urlhistory.get_last_n_columns()
print urlhistory