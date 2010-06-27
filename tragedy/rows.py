import functools
import itertools
import uuid
from cassandra.ttypes import (Column, Clock, ColumnOrSuperColumn, ColumnParent,
    ColumnPath, ConsistencyLevel, NotFoundException, SlicePredicate,
    SliceRange, SuperColumn, CfDef, Mutation)

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
                     ByteField,
                     ForeignKey,
                     MissingField,
                     TimeField,
                    )

from .exceptions import TragedyException

known_sort_orders = ('BytesType', 'AsciiType', 'UTF8Type', 'LongType', 'LexicalUUIDType', 'TimeUUIDType')

class RowKey(ConvertAPI):
    def __init__(self, *args, **kwargs):
        self.autogenerate = kwargs.pop('autogenerate', False)
        self.default = kwargs.pop('default', None)
        ConvertAPI.__init__(self, *args, **kwargs)
    
    def value_to_internal(self, value):
        if hasattr(value, 'row_key'):
            value = value.row_key
        
        return value

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
        
    _beensaved = False
    _beenloaded = False
    
    # If our class configuration is incomplete, fill in defaults
    _column_type = 'Standard'
    _order_by = 'BytesType'
    _comment = ''
    _row_cache_size = 0
    _preload_row_cache = False
    _key_cache_size = 200000
    _dont_hash_row_key = False # not in use right now, but we seem to have encoding issues.

    @classmethod
    def _init_class(cls, name=None):
        assert name != None, "Name can't be None!"
        cls._column_family = getattr(cls, '_column_family', cls.__name__)
        keyspaces = cmcache.retrieve('keyspaces')
        if not keyspaces:
            print 'NO KEYSPACES?!?'
        assert keyspaces, 'No Keyspaces defined - make sure you define one before defining modules.'
        cls._keyspace = getattr(cls, '_keyspace', keyspaces[0])
        cls.save_hooks = OrderedSet()
        cls._keyspace.register_model(getattr(cls, '_column_family', name), cls)
    
    @classmethod
    def _init_stage_two(cls):
        # print 'STAGE 2', cls
        pass
    
    @classmethod
    def getclient(cls):
        return cls._keyspace.getclient()
    
    # Default Consistency levels that have overrides.
    _read_consistency_level=ConsistencyLevel.ONE
    _write_consistency_level=ConsistencyLevel.ONE

    @classmethod
    def _wcl(cls, alternative):
        return alternative if alternative else cls._write_consistency_level

    @classmethod
    def _rcl(cls, alternative):
        return alternative if alternative else cls._read_consistency_level

    @classmethod
    def asCfDef(cls):
        assert cls._order_by in known_sort_orders, 'Unknown sort_by %s' % (cls._order_by,)
        cfdef = CfDef(table=cls._keyspace.name, name=cls._column_family, column_type=cls._column_type, 
                      comparator_type=cls._order_by, comment=cls._comment,
                      row_cache_size=cls._row_cache_size, preload_row_cache=cls._preload_row_cache, key_cache_size=cls._key_cache_size,
                      )
        return cfdef
    
    @classmethod
    def register_columnfamiliy_with_cassandra(cls):
        cfdef = cls.asCfDef()
        cls.getclient().system_add_column_family(cfdef)
        

class BasicRow(RowDefaults):
    """Each sub-class represents exactly one ColumnFamily, and each instance exactly one Row."""
    __abstract__ = True

# ----- INIT -----

    def __init__(self, row_key=None, *args, **kwargs):
        # Storage
        self.ordered_columnkeys = OrderedSet()
        self.column_values    = {}  #
        self.column_changed  = {}  # these have no order themselves, but the keys are the same as above
        self.column_spec     = {}  #
        
        self.mirrors = OrderedSet()
                
        # Our Row Key
        self.row_key = row_key
        
        self._row_key_name = None
        self._row_key_spec = None
        
        # Extract the Columnspecs
        self.extract_specs_from_class()
        
        if kwargs.get('_for_loading'):
            self._update(*args, **kwargs)
        else:
            self.update(*args, **kwargs)
            
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
            raise TragedyException('need a name for the row key!')

# ----- Access and convert data -----
    def __eq__(self, other):
        if not other:
            return not self.row_key
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
        return self.column_values.get(column_key)

    def set_value_for_columnkey(self, column_key, value, dont_mark=False):
        assert isinstance(column_key, basestring), "Column Key needs to be a string."
        self.ordered_columnkeys.add(column_key)
        self.column_values[column_key] = value
        
        if dont_mark:
            self.unmarkChanged(column_key)
        else:
            self.markChanged(column_key)
    
    def listMissingColumns(self, for_saving=False):
        missing_cols = OrderedSet()
        
        for column_key, spec in self.column_spec.items():
            value = self.column_values.get(column_key)
            if spec.mandatory and (self.column_values.get(column_key) is None):
                if spec.default:
                    default = spec.get_default()
                    if for_saving:
                        self.set_value_for_columnkey(column_key, default)
                else: #if not hasattr(self, '_default_field'): # XXX: i think this was meant to check if self is an index?
                    missing_cols.add(column_key)
                
            if value and column_key not in self.ordered_columnkeys:
                raise TragedyException('Value set, but column_key not in ordered_columnkeys. WTF?')
        
        return missing_cols
    
    def isComplete(self):
        return not self.listMissingColumns()
    
    def yield_column_key_value_pairs(self, for_saving=False, **kwargs):
        access_mode = kwargs.pop('access_mode', 'to_identity')
        
        missing_cols = self.listMissingColumns(for_saving=for_saving)
        if for_saving and missing_cols:
            raise TragedyException("Columns %s mandatory but missing." % 
                        ([(ck,self.column_spec[ck]) for ck in missing_cols],))


        for column_key in self.ordered_columnkeys:
            if for_saving:
                if not (column_key in self.column_changed): # XXX: there are faster ways - profile?
                    continue
            assert isinstance(column_key, basestring), 'Column Key not of type string?'
            spec = self.get_spec_for_columnkey(column_key)            
            value = self.get_value_for_columnkey(column_key)
            
            if for_saving:
                value = spec.value_for_saving(value)
            
            if not (value is None):
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
        return [self.column_values[x] for x in self.ordered_columnkeys]

    def iterkeys(self):
        return ( (x, self.column_values[x]) for x in self.ordered_columnkeys)
    
    def itervalues(self):
        return (self.column_values[x] for x in self.ordered_columnkeys)

# ----- Change Data -----

    def update(self, *args, **kwargs):
        access_mode = kwargs.pop('access_mode', 'to_internal')
    
        return self._update(access_mode=access_mode, *args, **kwargs)

    def _update(self, *args, **kwargs):        
        access_mode = kwargs.pop('access_mode', 'to_identity')
        _for_loading = kwargs.pop('_for_loading', False)
        
        tmp = OrderedDict()
        tmp.update(*args, **kwargs)
        
        for column_key, value in tmp.iteritems():
            if column_key == self._row_key_name:
                self.row_key = self._row_key_spec.value_to_internal(value)
                continue
            spec = self.column_spec.get(column_key, self._default_field)
            column_key, value = getattr(spec, access_mode)(column_key, value)
            self.set_value_for_columnkey(column_key, value, dont_mark=_for_loading)

    def markChanged(self, column_key):
        self.column_changed[column_key] = True

    def unmarkChanged(self, column_key):
        if column_key in self.column_changed:
            del self.column_changed[column_key]

    def delete(self, column_key):
        # XXX: keep track of delete
        # XXX: can't delete if default columnspec is 'mandatory'.
        spec = self.get_spec_for_columnkey(column_key)
        if spec.mandatory:
            raise TragedyException('Trying to delete mandatory column %s' % (column_key,))
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
        # print 'DECODE', colOrSuper.column.name, colOrSuper.column.clock
        return (colOrSuper.column.name, colOrSuper.column.value)
        
    @classmethod
    def load_multi(cls, ordered=True, *args, **kwargs):
        unordered = {}
        if not kwargs['keys']:
            raise StopIteration

        for row_key in kwargs['keys']:
            assert row_key, 'Empty row_key %s' % (row_key,)
            assert isinstance(row_key, basestring), 'Row Key %s is of type %s should be basestring.' % (row_key, type(row_key,))
        
        for row_key, columns in cls.multiget_slice(*args, **kwargs):
            columns = OrderedDict(columns)
            columns['row_key'] = row_key
            columns['access_mode'] = 'to_identity'
            columns['_for_loading'] = True
            if not ordered:
                yield cls( **columns)
            else:
                unordered[row_key] = columns
        
        if not ordered:
            raise StopIteration
            
        for row_key in kwargs['keys']:
            blah = unordered.get(row_key)
            yield cls( **blah )
    
    def load(self, *args, **kwargs):
        if not self.row_key and self._row_key_spec.default:
                self.row_key = self._row_key_spec.get_default()
        assert self.row_key, 'No row_key and no non-null non-empty keys argument. Did you use the right row_key_name?'
        tkeys = [self.row_key]
        result = list(self.load_multi(keys=tkeys))
        self._update(result[0].column_values, _for_loading=True)
        return self
        # # print self, dir(self), self._row_key_name
        # assert self.row_key, 'No row_key and no non-null non-empty keys argument. Did you use the right row_key_name?'
        # assert isinstance(self.row_key, basestring), 'Row Key is of type %s should be basestring.' % (type(self.row_key,))
        # # load_subkeys = kwargs.pop('load_subkeys', False)
        # tkeys = [self.row_key]
        # 
        # data = list(self.multiget_slice(keys=tkeys, *args, **kwargs))
        # assert len(data) == 1
        # self._update(data[0][1], _for_loading=True)
        # # return data[0][1]
        # 
        # # if load_subkeys:
        # #     return self.loadIterValues()
        # return self
        
    @classmethod
    def multiget_slice(cls, keys=None, consistency_level=None, **kwargs):
        assert keys, 'Need a non-null non-empty keys argument.'
        # print 'GETTING', cls, keys, kwargs
        
        predicate = cls.get_slice_predicate(**kwargs)
        key_slices = cls.getclient().multiget_slice(    #  keyspace          = str(cls._keyspace),
                                                      keys              = keys,
                                                      column_parent     = cls.column_parent(),
                                                      predicate         = predicate,
                                                      consistency_level=cls._rcl(consistency_level),
                                                     )
        if key_slices:
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
                self.row_key = self._row_key_spec.get_default()
            else:
                raise TragedyException('No row_key set!')
        
        for save_row_key in itertools.chain((self.row_key,), self.mirrors):
            if callable(save_row_key):
                save_row_key = save_row_key()
            self._real_save(save_row_key=save_row_key, *args, **kwargs)
        
        for hook in self.save_hooks:
            hook(self)
        
        self._beensaved = True
        
        return self
        
    def _real_save(self, save_row_key=None, *args, **kwargs):
        save_columns = []
        for column_key, value in self.yield_column_key_value_pairs(for_saving=True):
            assert isinstance(value, basestring), 'Not basestring %s:%s (%s)' % (column_key, type(value), type(self))
            newtimestamp = self._timestamp_func()
            import time
            # print 'STORING WITH NEWTIMESTAMP', self.__class__, column_key, newtimestamp #time.ctime( int(newtimestamp) ) 
            column = Column(name=column_key, value=value, clock=Clock(timestamp=newtimestamp))
            save_columns.append( ColumnOrSuperColumn(column=column) )
        
        save_mutations = [Mutation(column_or_supercolumn=sc) for sc in save_columns]
        
        # self.getclient().batch_insert(#keyspace         = str(self._keyspace),
        #                          key              = save_row_key,
        #                          cfmap            = {self._column_family: save_columns},
        #                          consistency_level= self._wcl(kwargs['write_consistency_level']),
        #                         )
        mumap = {save_row_key: {self._column_family: save_mutations} }
        # print u'PREMUMAP', unicode(save_mutations).encode('ascii', 'replace')
        # print u'MUMAP', repr(mumap).encode('ascii', 'replace')
        self.getclient().batch_mutate(
                                      mutation_map=mumap,
                                      consistency_level=self._wcl(kwargs['write_consistency_level']),
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
    
    def __getitem__(self, column_key):
        value = self.get(column_key)
        # print 'FUCK', self.column_values
        if value is None:
            raise KeyError('No Value set for %s (%s)' % (column_key, self._column_family))
        return value
    
    def __setitem__(self, column_key, value):
        self.update( [(column_key, value)] )

    def get(self, column_key, default=None, **kwargs):
        access_mode = kwargs.pop('access_mode', 'to_external')
        
        spec = self.get_spec_for_columnkey(column_key)
        value = self.get_value_for_columnkey(column_key)
        if not (value is None):
            value = getattr(spec, 'value_' + access_mode)(value)
        else:
            value = default
        return value