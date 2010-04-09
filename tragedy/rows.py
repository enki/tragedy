import functools
import itertools
import uuid
from cassandra.ttypes import (Column, ColumnOrSuperColumn, ColumnParent,
    ColumnPath, ConsistencyLevel, NotFoundException, SlicePredicate,
    SliceRange, SuperColumn)

from .datastructures import (OrderedSet,
                             OrderedDict,
                            )
from .util import (gm_timestamp, 
                   CASPATHSEP,
                  )
from .hierarchy import (InventoryType,
                        cmcache,
                       )    
from .columns import (ColumnSpec,
                     IdentityColumnSpec,
                     TimeUUIDColumnSpec,
                     MissingColumnSpec,
                    )

from .hacks import boot

class RowDefaults(object):
    """Configuration Defaults for Rows."""
    __metaclass__ = InventoryType # register with the inventory
    __abstract__ = True # buy only if you are not __abstract__

    # What we use for timestamps.
    timestamp = staticmethod(gm_timestamp)
    
    # We complain when there are attempts to set columns without spec.
    _default_spec = MissingColumnSpec()
    
    # If our class configuration is incomplete, fill in defaults
    _column_type = 'Standard'
    _compare_with = 'BytesType'    
    @classmethod
    def _init_class(cls):
        cls._column_family = getattr(cls, '_column_family', cls.__name__)
        cls._keyspace = getattr(cls, '_keyspace', cmcache.retrieve('keyspaces')[0])
        cls._client = getattr(cls, '_client', cmcache.retrieve('clients')[0])
    
    # Default Consistency levels that have overrides.
    read_consistency_level=ConsistencyLevel.ONE
    write_consistency_level=ConsistencyLevel.ONE
    def _wcl(self, alternative):
        return alternative if alternative else self.read_consistency_level

    def _rcl(self, alternative):
        return alternative if alternative else self.read_consistency_level

class BasicRow(RowDefaults):
    """Each sub-class represents exactly one ColumnFamily, and each instance exactly one Row."""
    __abstract__ = True

# ----- INIT -----

    def __init__(self, row_key=None, *args, **kwargs):
        # We're starting to go live - tell our hacks to check the db!
        boot()

        # Storage
        self.ordered_columnkeys = OrderedSet()
        self.column_value    = {}  #
        self.column_spec     = {}  # these have no order themselves, but the keys are the same as above
        
        # Our Row Key
        self.row_key = row_key
        
        # Extract the Columnspecs
        self.extract_columnspecs_from_class()
        
        self.init(*args, **kwargs)
    
    def init(self, *args, **kwargs):
        pass # Override me

    def extract_columnspecs_from_class(self):
        # Extract the columnspecs from this class
        for attr, elem in itertools.chain(self.__class__.__dict__.iteritems(), self.__dict__.iteritems()):
            if attr[0] == '_' or not isinstance(elem, ColumnSpec):
                continue
            self.column_spec[attr] = elem

# ----- Access and convert data -----
        
    def get_spec_for_columnkey(self, column_key):
        spec = self.column_spec.get(column_key)
        if not spec:
            spec = getattr(self, column_key, None)
        if not spec:
            spec = self._default_spec
        return spec
    
    def get_value_for_columnkey(self, column_key):
        return self.column_value.get(column_key)
    
    def yield_column_key_value_pairs(self, check_for_saving=False, **kwargs):
        access_mode = kwargs.pop('access_mode', 'to_identity')
        
        for name, spec in self.column_spec.items():
            if spec.required and not self.column_value.get(name):
                raise Exception('Column %s of type %s required but missing.' % (name, spec))

        for column_key in self.ordered_columnkeys:
            value = self.get_value_for_columnkey(column_key)
            spec = self.get_spec_for_columnkey(column_key)
            column_key, value = getattr(spec, access_mode)(column_key, value)
            if value is None:
                continue
            
            yield column_key, value

    def __iter__(self):
        return self.yield_column_key_value_pairs(access_mode='to_external')

# ----- Change Data -----

    def _update(self, *args, **kwargs):
        access_mode = kwargs.pop('access_mode', 'to_identity')
        
        tmp = OrderedDict()
        tmp.update(*args, **kwargs)
        
        for column_key, value in tmp.iteritems():
            spec = self.column_spec.get(column_key, self._default_spec)
            column_key, value = getattr(spec, access_mode)(column_key, value)
            self.ordered_columnkeys.add(column_key)
            self.column_value[column_key] = value

# ----- Load Data -----
    
    @property
    def partial_get_columns_from_one_row(self):
        column_parent = ColumnParent(column_family=self._column_family, super_column=None)
        func = functools.partial(self._client.get_range_slice, 
                                 keyspace          = str(self._keyspace),
                                 column_parent     = column_parent,
                                 #predicate
                                 start_key         = self.row_key,
                                 finish_key        = self.row_key,
                                 row_count         = 1,
                                 #consistency_level
                                )
        return func
    
    def get_slice_predicate(self, column_names=None, start='', finish='', reverse=True, count=10000):
        if column_names:
            return SlicePredicate(column_names=columns)
            
        slice_range = SliceRange(start=start, finish=finish, reversed=reverse, count=count)
        return SlicePredicate(slice_range=slice_range)
    
    def get_last_n_columns(self, n=10000, consistency_level=None, **kwargs):
        predicate = self.get_slice_predicate(count=n, **kwargs)
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

# ----- Save Data -----

    def save(self, write_consistency_level=None):
        save_columns = []
        for column_key, columnvalue in self.yield_column_key_value_pairs(check_for_saving=True):
            column = Column(name=column_key, value=columnvalue, timestamp=self.timestamp())
            save_columns.append( ColumnOrSuperColumn(column=column) )
                
        self._client.batch_insert(keyspace         = str(self._keyspace),
                                 key              = self.row_key,
                                 cfmap            = {self._column_family: save_columns},
                                 consistency_level= self._wcl(write_consistency_level),
                                )

# ----- Display -----
        
    def __str__(self):
        return '<{0}: {1}>'.format(self.__class__.__name__, repr(OrderedDict( 
            self.get_spec_for_columnkey(column_key).to_display(column_key,value) for column_key,value in 
                    self.yield_column_key_value_pairs())))

    def path(self, column_key=None):
        """For now just a way to display our position in a kind of DOM."""
        p = u'%s%s%s' % (self._keyspace.path(), CASPATHSEP, self._column_family)
        if self.row_key:
            p += u'%s%s' % (CASPATHSEP,self.row_key)
            if column_key:
                p+= u'%s%s' % (CASPATHSEP, repr(column_key),)
        return p

class DictRow(BasicRow):
    """Row with a public dictionary interface to set and get columns."""
    __abstract__ = True
    
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
    """A row which doesn't care about column names, and that can be appended to."""
    __abstract__ = True
    _default_spec = IdentityColumnSpec(required=False)
    def append(self, target):
        if hasattr(target, 'row_key'):
            target = target.row_key
            
        newuuid = uuid.uuid1().bytes
        self._update( [(newuuid, target)] )