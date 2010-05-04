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
from .columns import (ConvertAPI,
                     Field,
                     IdentityField,
                     ForeignKey,
                     MissingField,
                    )

from .hacks import boot

class RowKey(ConvertAPI):
    def __init__(self, *args, **kwargs):
        self.autogenerate = kwargs.pop('autogenerate', False)
        self.linked_from = kwargs.pop('linked_from', None)
        self.by_funcname = kwargs.pop('by_funcname', None)
        self.autoload_values = kwargs.pop('autoload_values', False)
        self.default = kwargs.pop('default', None)
    
    def value_to_internal(self, value):
        if hasattr(value, 'row_key'):
            value = value.row_key
        
        return value
    
    def prepare_referencing_class(self, cls, keyname):
        if not self.linked_from:
            return
        name = cls.__name__
        
        if not self.by_funcname:
            fixedname = []
            first = True
            for c in name:
                if first:
                    fixedname.append( c.lower() )
                    first = False
                elif c.islower():
                    fixedname.append(c)
                elif c.isupper():
                    fixedname.append('_' + c.lower())
            fixedname = ''.join(fixedname)
        else:
            fixedname = None
        
        if self.autoload_values:
            def acccess_function(self, *args, **kwargs):
                myargs = {keyname: self}
                ts = cls(**myargs).load(*args, **kwargs)
                return ts.loadIterValues()
        else:
            def acccess_function(self, *args, **kwargs):
                myargs = {keyname: self}
                ts = cls(**myargs).load(*args, **kwargs)
                return ts
        
        if isinstance(self.linked_from, type):
            models = [self.linked_from]
        else:
            models = self.linked_from
        
        if not self.by_funcname:
            funcname = 'get_' + fixedname
        else:
            funcname = self.by_funcname
            
        for model in models:
            setattr(model, funcname, acccess_function)

class RowDefaults(object):
    """Configuration Defaults for Rows."""
    __metaclass__ = InventoryType # register with the inventory
    __abstract__ = True # buy only if you are not __abstract__

    # What we use for timestamps.
    _timestamp_func = staticmethod(gm_timestamp)
    
    # We complain when there are attempts to set columns without spec.
    # default specs should normally never be mandatory!
    _default_field = MissingField(mandatory=False)
    
    # we generally try to preserve order of columns, but this tells us it's ok not to occasionally.
    _ordered = False
    
    # If our class configuration is incomplete, fill in defaults
    _column_type = 'Standard'

    @classmethod
    def _init_class(cls):
        cls._column_family = getattr(cls, '_column_family', cls.__name__)
        cls._keyspace = getattr(cls, '_keyspace', cmcache.retrieve('keyspaces')[0])
        cls._client = getattr(cls, '_client', cmcache.retrieve('clients')[0])
    
    # Default Consistency levels that have overrides.
    _read_consistency_level=ConsistencyLevel.ONE
    _write_consistency_level=ConsistencyLevel.ONE

    @classmethod
    def _wcl(cls, alternative):
        return alternative if alternative else cls._write_consistency_level

    @classmethod
    def _rcl(cls, alternative):
        return alternative if alternative else cls._read_consistency_level

class BasicRow(RowDefaults):
    """Each sub-class represents exactly one ColumnFamily, and each instance exactly one Row."""
    __abstract__ = True

# ----- INIT -----

    def __init__(self, *args, **kwargs):
        # We're starting to go live - tell our hacks to check the db!
        boot()
        
        # Storage
        self.ordered_columnkeys = OrderedSet()
        self.column_value    = {}  #
        self.column_changed  = {}  # these have no order themselves, but the keys are the same as above
        self.column_spec     = {}  #
        
        self.indexes = OrderedSet()
        self.mirrors = OrderedSet()
        
        # Our Row Key
        self.row_key = kwargs.pop('row_key', None)
        
        self._row_key_name = None
        self._row_key_spec = None
        
        # Extract the Columnspecs
        self.extract_specs_from_class()
        
        self._update(*args, **kwargs)
        self.init(*args, **kwargs)
    
    def init(self, *args, **kwargs):
        pass

    def extract_specs_from_class(self):
        # Extract the columnspecs from this class
        for attr, elem in itertools.chain(self.__class__.__dict__.iteritems(), self.__dict__.iteritems()):
            if attr[0] == '_':
                continue
            elif isinstance(elem, RowKey):
                self._row_key_name = attr
                self._row_key_spec = elem
                if self.row_key:
                    self.row_key = self._row_key_spec.value_to_internal(self.row_key)
                continue
            elif not isinstance(elem, Field):
                continue
            self.column_spec[attr] = elem
        
        if not self._row_key_name:
            raise Exception('need a name for the row key!')

# ----- Access and convert data -----
    
    # def __getattr__(self, column_key):
    #     spec = self.get_spec_for_columnkey(column_key)
    #     value = self.get_value_for_columnkey(column_key)
    #     return spec.value_to_external(value)
    # 
    # def __setattr__(self, column_key, value):
    #     spec = self.get_spec_for_columnkey(column_key)
    #     internal_value = spec.value_to_internal(value)
    #     return self.set_value_for_columnkey(columnkey, internal_value)

    def __eq__(self, other):
        return self.row_key == other.row_key
    
    def get_spec_for_columnkey(self, column_key):
        spec = self.column_spec.get(column_key)
        if not spec:
            spec = getattr(self, column_key, None)
        if not spec:
            spec = self._default_field
        return spec
    
    def get_value_for_columnkey(self, column_key):
        if column_key == self._row_key_name:
            return self.row_key
        return self.column_value.get(column_key)

    def set_value_for_columnkey(self, column_key, value):
        self.ordered_columnkeys.add(column_key)
        self.column_value[column_key] = value
    
    def listMissingColumns(self):
        missing_cols = OrderedSet()
        
        for column_key, spec in self.column_spec.items():
            value = self.column_value.get(column_key)
            if spec.mandatory and not self.column_value.get(column_key):
                # if for_saving and spec.default:
                if spec.default:
                    if hasattr(spec.default, '__call__'):
                        default = spec.default()
                    else:
                        default = spec.default
                    self.column_value[column_key] = default
                    self.ordered_columnkeys.add(column_key)
                elif not hasattr(self, 'targetmodel'):
                    missing_cols.add(column_key)
                
            if value and column_key not in self.ordered_columnkeys:
                raise Exception('Value set, but column_key not in ordered_columnkeys. WTF?')
            
        return missing_cols
    
    def isComplete(self):
        return not self.listMissingColumns()
    
    def yield_column_key_value_pairs(self, for_saving=False, **kwargs):
        access_mode = kwargs.pop('access_mode', 'to_identity')
        
        missing_cols = self.listMissingColumns()
        if for_saving and missing_cols:
            raise Exception("Columns %s mandatory but missing." % 
                        [(ck,self.column_spec[ck]) for ck in missing_cols])

        for column_key in self.ordered_columnkeys:
            spec = self.get_spec_for_columnkey(column_key)            
            value = self.get_value_for_columnkey(column_key)
            
            if value:
                column_key, value = getattr(spec, access_mode)(column_key, value)
            else:
                column_key = getattr(spec, 'key_' + access_mode)(column_key)
                
            # if value is None:
            #     continue
            
            yield column_key, value

    def __iter__(self):
        return self.yield_column_key_value_pairs(access_mode='to_external')

    def keys(self):
        return self.ordered_columnkeys

    def values(self):
        return [self.column_value[x] for x in self.ordered_columnkeys]

    def iterkeys(self):
        return ( (x, self.column_value[x]) for x in self.ordered_columnkeys)
    
    def itervalues(self):
        return (self.column_value[x] for x in self.ordered_columnkeys)

# ----- Change Data -----

    def _update(self, *args, **kwargs):        
        access_mode = kwargs.pop('access_mode', 'to_identity')
        
        tmp = OrderedDict()
        tmp.update(*args, **kwargs)
        
        for column_key, value in tmp.iteritems():
            if column_key == self._row_key_name:
                self.row_key = self._row_key_spec.value_to_internal(value)
                continue
            spec = self.column_spec.get(column_key, self._default_field)
            column_key, value = getattr(spec, access_mode)(column_key, value)
            self.set_value_for_columnkey(column_key, value)
            self.markChanged(column_key)

    def markChanged(self, column_key):
        self.column_changed[column_key] = True

    def delete(self, column_key):
        # XXX: keep track of delete
        # XXX: can't delete if default columnspec is 'mandatory'.
        spec = self.get_spec_for_columnkey(column_key)
        if spec.mandatory:
            raise 'Trying to delete mandatory column %s' % (column_key,)
        del self.column_value[column_key]

# ----- Load Data -----

    @classmethod
    def column_parent(cls):
        return ColumnParent(column_family=cls._column_family, super_column=None)
    
    @property
    def query_defaults(self):
        d = dict( keyspace          = str(self._keyspace),
                  column_parent     = self.column_parent,
                )
        return d
    
    @staticmethod
    def get_slice_predicate(column_names=None, start='', finish='', reverse=True, count=10000, *args, **kwargs):
        if column_names:
            return SlicePredicate(column_names=columns)
            
        slice_range = SliceRange(start=start, finish=finish, reversed=reverse, count=count)
        return SlicePredicate(slice_range=slice_range)
    
    @staticmethod
    def decodeColumn(colOrSuper):
        return (colOrSuper.column.name, colOrSuper.column.value)
        
    @classmethod
    def load_multi(cls, ordered=True, *args, **kwargs):
        unordered = {}
        for row_key, columns in cls.multiget_slice(*args, **kwargs):
            columns = OrderedDict(columns)
            columns['row_key'] = row_key
            if not ordered:
                yield cls( **columns )
            else:
                unordered[row_key] = columns
        
        if not ordered:
            raise StopIteration
            
        for row_key in kwargs['keys']:
            yield cls( **unordered[row_key] )
    
    def load(self, *args, **kwargs):
        if not self.row_key and self._row_key_spec.default:
            if callable(self._row_key_spec.default):
                self.row_key = self._row_key_spec.default()
            else:
                self.row_key = self._row_key_spec.default                
        assert self.row_key, 'No row_key and no non-null non-empty keys argument. Did you use the right row_key_name?'
        load_subkeys = kwargs.pop('load_subkeys', False)
        tkeys = [self.row_key]
        
        data = list(self.multiget_slice(keys=tkeys, *args, **kwargs))
        assert len(data) == 1
        self._update(data[0][1])
        # return data[0][1]
        if load_subkeys:
            return self.loadIterValues()
        return self
        
    @classmethod
    def multiget_slice(cls, keys=None, consistency_level=None, **kwargs):
        assert keys, 'Need a non-null non-empty keys argument.'
        predicate = cls.get_slice_predicate(**kwargs)
        key_slices = cls._client.multiget_slice(      keyspace          = str(cls._keyspace),
                                                      keys              = keys,
                                                      column_parent     = cls.column_parent(),
                                                      predicate         = predicate,
                                                      consistency_level=cls._rcl(consistency_level),
                                                     )
        for row_key, columns in key_slices.iteritems():
            yield row_key, [cls.decodeColumn(col) for col in columns]
        #     key, value = result[0], [(colOrSuper.column.name, colOrSuper.column.value) for \
        #                         colOrSuper in result[1]]
        #     yield key, value

# ----- Save Data -----
    def generate_row_key(self):
        self.row_key = uuid.uuid4().hex

    def save(self, *args, **kwargs):
        if not kwargs.get('write_consistency_level'):
            kwargs['write_consistency_level'] = None
        
        if not self.row_key:
            if self._row_key_spec.autogenerate:
                self.generate_row_key()
            elif self._row_key_spec.default:
                if callable(self._row_key_spec.default):
                    self.row_key = self._row_key_spec.default()
                else:
                    self.row_key = self._row_key_spec.default
            else:
                raise Exception('No row_key set!')
        
        for save_row_key in itertools.chain((self.row_key,), self.mirrors):
            self._real_save(save_row_key=save_row_key, *args, **kwargs)
        
        return self
        
    def _real_save(self, save_row_key=None, *args, **kwargs):
        save_columns = []
        for column_key, value in self.yield_column_key_value_pairs(for_saving=True):
            column = Column(name=column_key, value=value, timestamp=self._timestamp_func())
            save_columns.append( ColumnOrSuperColumn(column=column) )
                
        self._client.batch_insert(keyspace         = str(self._keyspace),
                                 key              = save_row_key,
                                 cfmap            = {self._column_family: save_columns},
                                 consistency_level= self._wcl(kwargs['write_consistency_level']),
                                )
        
        # reset 'changed' - nothing's changed anymore
        self.column_changed.clear()

# ----- Display -----
        
    def __repr__(self):
        dtype = OrderedDict if self._ordered else dict
        return '<%s %s: %s>' % (self.__class__.__name__, self.row_key, repr(dtype( 
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
        value = self.get(column_key)
        if value is None:
            raise KeyError('No Value set for %s' % (column_key,))
        return value
    
    def __setitem__(self, column_key, value):
        self.update( [(column_key, value)] )

    def get(self, column_key, default=None):
        spec = self.get_spec_for_columnkey(column_key)
        value = self.get_value_for_columnkey(column_key)
        if value:
            value = spec.value_to_external(value)
        else:
            value = default
        return value

    @property
    def update(self):
        return functools.partial(self._update, access_mode='to_internal')

Model = DictRow

class Index(BasicRow):
    """A row which doesn't care about column names, and that can be appended to."""
    __abstract__ = True
    _default_field = None
    _ordered = True

    def is_unique(self, target):
        MAXCOUNT = 20000000
        self.load(count=MAXCOUNT) # XXX: we will blow up here at some point
                                  # i don't know where the real limit is yet.
        assert len(self.column_value) < MAXCOUNT - 1, 'Too many keys to enforce sorted uniqueness!'
        mytarget = self.targetmodel.value_to_internal(target)
        if mytarget in self.itervalues():
            return False
        return True
        
    def append(self, target):
        if self.targetmodel.unique and not self.is_unique(target):
            return self
            
        target = self.targetmodel.value_to_internal(target)

        newuuid = uuid.uuid1().bytes
        self._update( [(newuuid, target)] )
        return self

    def loadIterItems(self):
        return itertools.izip(self.iterkeys(), self.loadIterValues())

    def loadIterValues(self):
        if self.values():
            return self.targetmodel.foreign_class.load_multi(keys=self.values(), orderdata=self.keys())
        return []

    def resolve(self):
        return self.loadIterValues()

    def __iter__(self):
        for row_key in self.itervalues():
            yield self.targetmodel.foreign_class(row_key=row_key)

# class TimeSortedIndex(Index):
#     __abstract__ = True
#     _compare_with = 'TimeUUIDType'
#     
# class TimeSortedUniqueIndex(Index):
#     __abstract__ = True
#     _compare_with = 'TimeUUIDType'
#     
#     def append(self, target):
#         MAXCOUNT = 20000000
#         self.load(count=MAXCOUNT) # XXX: we will blow up here at some point
#                                   # i don't know where the real limit is yet.
#         assert len(self.column_value) < MAXCOUNT - 1, 'Too many keys to enforce sorted uniqueness!'
#         mytarget = self._default_field.value_to_internal(target)
#         if mytarget in self.itervalues():
#             return self
#         else:
#             return super(Index, self).append(target)
