import functools
import uuid
from cassandra.ttypes import (Column, ColumnOrSuperColumn, ColumnParent,
    ColumnPath, ConsistencyLevel, NotFoundException, SlicePredicate,
    SliceRange, SuperColumn)

from .datastructures import (OrderedSet,
                             OrderedDict,
                            )
from .util import gm_timestamp

from hacks import InventoryType

class RowDefaults(object):
    """Abstract Helper"""
    __metaclass__ = InventoryType
    default_transformer = None # stuff that gets written to the database
                               # (also changes local state to stay in sync)
                               # -> internal rep is database rep
    default_sanitizer = None # userinput entering the system
    timestamp = staticmethod(gm_timestamp)
    read_consistency_level=ConsistencyLevel.ONE
    write_consistency_level=ConsistencyLevel.ONE
    
    def _wcl(self, alternative):
        return alternative if alternative else self.read_consistency_level

    def _rcl(self, alternative):
        return alternative if alternative else self.read_consistency_level

class BasicRow(RowDefaults):
    """Each sub-class represents exactly one ColumnFamily, and each instance exactly one Row."""
    def __init__(self, *args, **kwargs):
        self.sanityCheck_init()
        # Storage
        self.ordered_columnkeys = OrderedSet()
        self.column_value    = {}  #
        self.column_spec     = {}  # these have no order themselves, but the keys are the same as above
        
        self.init(*args, **kwargs)
    
    def init(self, *args, **kwargs):
        pass # Override me
    
    def sanityCheck_init(self):
        meta = getattr(self, 'Meta', False)
        assert meta, "Need to define Meta!"
        for attr in ('column_family', 'keyspace', 'column_type', 'compare_with', 'client'):
            assert getattr(meta, attr, False), 'Need to define Meta.{0}'.format(attr)

    def sanityCheck_save(self):
        assert self.Meta.row_key_name in self.ordered_columnkeys, 'Need row_key specified somehow!'
    
    def access_by_key_for_internal(self, columnkey):
        """returns internal/database state!"""
        transformer = self.column_spec.get(columnkey, self.default_transformer)
        if transformer:
            newvalue = transformer(columnkey, self.column_value.get(columnkey))
            self.column_value[columnkey] = newvalue
        
        return self.column_value[columnkey]
    
    def calc_kvpairs(self, filter_for_saving=False):
        for columnkey in self.ordered_columnkeys:
            self.access_by_key_for_internal(columnkey)
                            
            if self.column_value.get(columnkey) is None:
                continue
            if filter_for_saving and (columnkey == self.Meta.row_key_name):
                continue
                        
            yield columnkey, self.column_value[columnkey]
       
    def save(self, write_consistency_level=None):
        self.sanityCheck_save()
        save_columns = []
        for columnkey, columnvalue in self.calc_kvpairs(filter_for_saving=True):
            column = Column(name=columnkey, value=columnvalue, timestamp=self.timestamp())
            save_columns.append( ColumnOrSuperColumn(column=column) )
                
        self.Meta.client.batch_insert(keyspace         = str(self.Meta.keyspace),
                                 key              = self.get_key(),
                                 cfmap            = {self.Meta.column_family: save_columns},
                                 consistency_level= self._wcl(write_consistency_level),
                                )
    
    @property
    def partial_get_columns_from_one_row(self):
        column_parent = ColumnParent(column_family=self.Meta.column_family, super_column=None)
        func = functools.partial(self.Meta.client.get_range_slice, 
                                 keyspace          = str(self.Meta.keyspace),
                                 column_parent     = column_parent,
                                 #predicate
                                 start_key         = self.get_key(),
                                 finish_key        = self.get_key(),
                                 row_count         = 1,
                                 #consistency_level
                                )
        return func
    
    def get_slice_predicate(self, column_names=None, start='', finish='', reverse=False, count=10000):
        if column_names:
            return SlicePredicate(column_names=columns)
            
        slice_range = SliceRange(start=start, finish=finish, reversed=reverse, count=count)
        return SlicePredicate(slice_range=slice_range)
    
    def get_last_n_columns(self, n=10000, consistency_level=None):
        predicate = self.get_slice_predicate(count=n)
        key_slices = self.partial_get_columns_from_one_row(predicate=predicate,
                                                       consistency_level=self._rcl(consistency_level)
                                                      )
        if not key_slices:
            return None # empty
        assert len(key_slices) == 1, 'we requested one row, but got more'
        result = key_slices[0]
        assert result.key == self.get_key()
        result = [(colOrSuper.column.name, colOrSuper.column.value) for colOrSuper in result.columns]
        self._update(result)
        return result

    def _update(self, *args, **kwargs):
        tmp = OrderedDict()
        tmp.update(*args, **kwargs)
        
        for key, value in tmp.items():
            sanitizer = self.column_spec.get(key, self.default_sanitizer)
            if sanitizer:
                self.column_value[key] = sanitizer(key, value)
            self.ordered_columnkeys.add(key)
            self.column_value[key] = value
    
    def get_key(self):
        return self.access_by_key_for_internal(self.Meta.row_key_name)
    
    def set_key(self, key):
        self._update( [(self.Meta.row_key_name, key)] )
    
    def __str__(self):
        return '<{0}: {1}>'.format(self.__class__.__name__, repr(OrderedDict( (x,y) for (x,y) in 
                    self.calc_kvpairs())))

class DictRow(BasicRow):
    def __getitem__(self, columnkey):
        return self.access_by_key_for_internal(columnkey)
    
    def __setitem__(self, columnkey, value):
        self.update( [(columnkey, value)] )

    @property
    def update(self):
        return self._update

class IndexRow(BasicRow):
    def init(self, rowkey, *args, **kwargs):
        self.set_key(rowkey) # and don't ever change it.
    
    def append(self, target):
        if hasattr(target, 'get_key'):
            target = target.get_key()
        
        newuuid = uuid.uuid1().bytes
        self._update( [(newuuid, target)] )