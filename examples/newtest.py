import tragedy
client = tragedy.connect(['localhost:9160'])
from tragedy.newapi import *

class CachedURL(BasicRow):
    class Meta:
        keyspace = 'BBQ'
        column_family = 'CachedURL'
        client = client
        row_key_name = 'uuid'

cachedurl = CachedURL()
cachedurl.ordered_columnkeys['uuid'] = None
cachedurl.ordered_columnkeys['data'] = None
cachedurl.ordered_columnkeys['hase'] = None

cachedurl.column_value['uuid'] = 'bOOOO'
cachedurl.column_value['data'] = 'C'
cachedurl.column_value['hase'] = 'B'

cachedurl.insert()
# print cachedurl

foo = CachedURL()
foo.ordered_columnkeys['uuid'] = None
foo.column_value['uuid'] = 'bOOOO'
for colOrSuper in foo.get_last_n_columns().columns:
    column = colOrSuper.column
    name = column.name
    value = column.value
    print name, value
    # timestamp = column.timestamp