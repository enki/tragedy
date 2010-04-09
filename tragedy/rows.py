import functools
import uuid
from cassandra.ttypes import (Column, ColumnOrSuperColumn, ColumnParent,
    ColumnPath, ConsistencyLevel, NotFoundException, SlicePredicate,
    SliceRange, SuperColumn)

from .datastructures import (OrderedSet,
                             OrderedDict,
                            )
from .util import (gm_timestamp, 
                   warn,
                   CASPATHSEP
                  )

from hacks import InventoryType

from columns import (IdentityColumnSpec,
                     TimeUUIDColumnSpec,
                    )

class RowDefaults(object):
    """Configuration Defaults for a BasicRow."""
    __metaclass__ = InventoryType

    default_spec = IdentityColumnSpec()

    timestamp = staticmethod(gm_timestamp)
    read_consistency_level=ConsistencyLevel.ONE
    write_consistency_level=ConsistencyLevel.ONE
    
    def _wcl(self, alternative):
        return alternative if alternative else self.read_consistency_level

    def _rcl(self, alternative):
        return alternative if alternative else self.read_consistency_level

class BasicRow(RowDefaults):
    """Each sub-class represents exactly one ColumnFamily, and each instance exactly one Row."""
    def sanityCheck_init(self):
        meta = getattr(self, u'Meta', False)
        assert meta, u"Need to define Meta!"
        for attr in (u'column_family', u'keyspace', u'column_type', u'compare_with', u'client'):
            assert getattr(meta, attr, False), u'Need to define Meta.{0}'.format(attr)

    def __init__(self, row_key=None, *args, **kwargs):
        self.sanityCheck_init()
        # Storage
        self.ordered_columnkeys = OrderedSet()
        self.column_value    = {}  #
        self.column_spec     = {}  # these have no order themselves, but the keys are the same as above
        
        self.row_key = row_key
        
        self.init(*args, **kwargs)
    
    def init(self, *args, **kwargs):
        pass # Override me
    
    def path(self, column_key=None):
        p = u'%s%s%s' % (self.Meta.keyspace.path(), CASPATHSEP, self.Meta.column_family)
        if self.row_key:
            p += u'%s%s' % (CASPATHSEP,self.row_key)
            if column_key:
                p+= u'%s%s' % (CASPATHSEP, repr(column_key),)
        return p
    
    def get_spec_for_columnkey(self, column_key):
        return self.column_spec.get(column_key, self.default_spec)
    
    def get_value_for_columnkey(self, column_key):
        return self.column_value.get(column_key)
    
    def yield_column_key_value_pairs(self, filter_for_saving=False):
        for column_key in self.ordered_columnkeys:
            if self.get_value_for_columnkey(column_key) is None:
                continue
                                        
            yield column_key, self.get_value_for_columnkey(column_key)

    def sanityCheck_save(self):
        # assert self.Meta.row_key_name in self.ordered_columnkeys, 'Need row_key specified somehow!'
        pass

    def save(self, write_consistency_level=None):
        self.sanityCheck_save()
        save_columns = []
        for column_key, columnvalue in self.yield_column_key_value_pairs(filter_for_saving=True):
            column = Column(name=column_key, value=columnvalue, timestamp=self.timestamp())
            save_columns.append( ColumnOrSuperColumn(column=column) )
                
        self.Meta.client.batch_insert(keyspace         = str(self.Meta.keyspace),
                                 key              = self.row_key,
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
                                 start_key         = self.row_key,
                                 finish_key        = self.row_key,
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
        assert result.key == self.row_key
        result = [(colOrSuper.column.name, colOrSuper.column.value) for colOrSuper in result.columns]
        self._update(result)
        return result

    def _update(self, *args, **kwargs):
        if not hasattr(kwargs, 'access_mode'):
            access_mode = 'to_identity'
        else:
            access_mode = kwargs.pop('access_mode')
            
        tmp = OrderedDict()
        tmp.update(*args, **kwargs)
        
        for column_key, value in tmp.items():
            spec = self.column_spec.get(column_key, self.default_spec)
            if spec:
                column_key, value = getattr(spec, access_mode)(column_key, value)
            else:
                warn(u'No Spec for %s' % (self.path(column_key),))
            self.ordered_columnkeys.add(column_key)
            self.column_value[column_key] = value
        
    def __str__(self):
        return '<{0}: {1}>'.format(self.__class__.__name__, repr(OrderedDict( 
            self.get_spec_for_columnkey(column_key).to_display(column_key,value) for column_key,value in 
                    self.yield_column_key_value_pairs())))

class DictRow(BasicRow):
    def init(self, *args, **kwargs):
        self.update(*args, **kwargs)
    
    def __getitem__(self, column_key):
        spec = self.get_spec_for_columnkey(column_key)
        value = self.get_value_for_columnkey(column_key)
        return spec.value_to_external(value)
    
    def __setitem__(self, column_key, value):
        self.update( [(column_key, value)] )

    @property
    def update(self):
        return functools.partial(self._update, access_mode='to_internal')

class IndexRow(BasicRow):
    default_spec = TimeUUIDColumnSpec()
    def append(self, target):
        if hasattr(target, 'row_key'):
            target = target.row_key
        print 'append', target
            
        newuuid = uuid.uuid1().bytes
        self._update( [(newuuid, target)] )