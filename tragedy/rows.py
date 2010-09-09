import functools
import itertools
import uuid
from cassandra.ttypes import (Column, Clock, ColumnOrSuperColumn, ColumnParent,
    ColumnPath, ConsistencyLevel, NotFoundException, SlicePredicate,
    SliceRange, SuperColumn, CfDef, Mutation, Deletion)

from .datastructures import (OrderedSet,
                             OrderedDict,
                            )
from .util import (gm_timestamp, 
                   CASPATHSEP,
                   jsondumps
                  )
from .hierarchy import (InventoryType,
                        cmcache,
                       )    
from .columns import (
                     RowKeySpec,
                     Field,
                     # ByteField,
                     # ForeignKeyField,
                     MissingField,
                     # TimeField,
                    )

from .exceptions import TragedyException

known_sort_orders = ('BytesType', 'AsciiType', 'UTF8Type', 'LongType', 'LexicalUUIDType', 'TimeUUIDType')

from .util import buchtimer

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
        cfdef = CfDef(keyspace=cls._keyspace.name, name=cls._column_family, column_type=cls._column_type, 
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
        # print 'BASIC ROW INIT'
        # Storage
        self.ordered_columnkeys = OrderedSet()
        self.column_values    = {}  #
        self.column_changed  = {}  # these have no order themselves, but the keys are the same as above
        self.column_spec     = {}  #
        
        self.mirrors = OrderedSet()
        
        # Our Row Key
        self.row_key = row_key
        
        # print 'INIT ME UP', args, kwargs,  row_key, self.row_key
        
        self._row_key_name = None
        self._row_key_spec = None
        
        # print 'NOW WE DO', self._row_key_name
        
        
        # print 'INITED', self, self.row_key, self._row_key_name, self._row_key_spec
        
        # Extract the Columnspecs
        self.extract_specs_from_class()
        
        # print 'NOW WE DOX1', self._row_key_name
        
        
        # print 'MID-INITED', self, self.row_key, self._row_key_name, self._row_key_spec
                
        if kwargs.get('_for_loading'):
            self._beensaved = True  # having both of these here kinda is redundant
            self._beenloaded = True # but also makes sense and is nonsense.. oh well...
            self._update(*args, **kwargs)
        else:
            self.update(*args, **kwargs)
        
        # print 'NOW WE DOX2', self._row_key_name
        
        # print 'POST-INITED', self, self.row_key, self._row_key_name, self._row_key_spec
        
        
        self.init(*args, **kwargs)
        
        # print 'NOW WE DOX3', self._row_key_name
    
    def init(self, *args, **kwargs):
        pass

    def extract_specs_from_class(self):
        # Extract the columnspecs from this class
        for attr, elem in itertools.chain(self.__class__.__dict__.iteritems(), self.__dict__.iteritems()):
            if attr[0] == '_':
                continue
            elif isinstance(elem, RowKeySpec):
                # print 'WHOA ROWKEY', attr, elem
                self._row_key_name = attr
                self._row_key_spec = elem
                # if self.row_key:
                #     self.row_key = self._row_key_spec.to_internal(self.row_key)
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
        if column_key == 'row_key':
            # raise TragedyException('ROW_KEY CANT BE SET HERE %s %s' % (column_key, value))
            self.row_key = value
            return
            
        self.ordered_columnkeys.add(column_key)
        self.column_values[column_key] = value
        
        if dont_mark:
            self.unmarkChanged(column_key)
        else:
            self.markChanged(column_key)
    
    def listMissingColumns(self, for_saving=False):
        missing_cols = OrderedSet()
        dummy = object()
        
        for column_key, spec in self.column_spec.items():
            value = self.column_values.get(column_key, dummy)
            if spec.mandatory:
                if (self.column_values.get(column_key) is None):
                    if spec.value.default:
                        default = spec.value.get_default()
                        if for_saving:
                            self.set_value_for_columnkey(column_key, default)
                    else: #if not hasattr(self, '_default_field'): # XXX: i think this was meant to check if self is an index?
                        # print column_key, spec.value, spec.value.default
                        missing_cols.add(column_key)
                else:
                    self.ordered_columnkeys.add(column_key)
                
            if (not value is dummy) and column_key not in self.ordered_columnkeys:
                raise TragedyException('Value set, but column_key not in ordered_columnkeys. WTF?')
        
        return missing_cols
    
    def isComplete(self):
        return not self.listMissingColumns()
    
    def yield_column_key_value_pairs(self, with_row_key=False, for_saving=False, with_private=True, **kwargs):
        # print 'YIELDEDIDOO'
        # if self.column_spec:
            # print 'OPHAI', self.column_spec, self.ordered_columnkeys
        access_mode = kwargs.pop('access_mode', 'to_identity')
        
        missing_cols = self.listMissingColumns(for_saving=for_saving)
        # print 'WHAT IS AMISS', missing_cols
        if for_saving and missing_cols:
            raise TragedyException("Columns %s mandatory but missing." % 
                        ([(ck,self.column_spec[ck]) for ck in missing_cols],))

        if with_row_key:
            yield (self._row_key_name, self.row_key)

        for column_key in self.ordered_columnkeys:
            if for_saving:
                spec = self.get_spec_for_columnkey(column_key)            
                if getattr(spec.value, '_autoset_on_save', False):
                   pass 
                elif not (column_key in self.column_changed): # XXX: there are faster ways - profile?
                    # print 'COLUMN NOT CHANGED? HAH', column_key, self.column_changed
                    continue
            assert isinstance(column_key, basestring), 'Column Key not of type string?'
            spec = self.get_spec_for_columnkey(column_key)            
            value = self.get_value_for_columnkey(column_key)
            
            # print 'WHXT', column_key, spec
            if spec.config.get('private') and (not with_private):
                print 'PRIVATE', with_private
                continue
            
            if for_saving:
                value = spec.value.for_saving(value)
                self.set_value_for_columnkey(column_key, value)
            
            if not (value is None):
                column_key, value = getattr(spec, access_mode)(column_key, value)
            else:
                column_key = getattr(spec.key, access_mode)(column_key)
                
            # if value is None:
            #     continue
            
            yield column_key, value

    def __iter__(self):
        return self.iteritems()

    def keys(self):
        return list(self.iterkeys())

    def values(self):
        return list(self.itervalues())

    def iterkeys(self):
        return (x for x in self.ordered_columnkeys)
    
    def itervalues(self):
        return (y for x,y in self.iteritems())

    def iteritems(self, *args, **kwargs):
        if not kwargs.get('access_mode'):
            kwargs['access_mode'] = 'to_external'
        return self.yield_column_key_value_pairs(*args, **kwargs)

    def toDICT(self, recurse=1, fields=None):
        data = {}
        if recurse >= 0:
            self.load()
        for key, value in self.iteritems(with_row_key=True):
            if fields and (key not in fields):
                continue
            
            if (key != self._row_key_name) and ( getattr(self.column_spec[key],'is_datetime',None)):
                value = self.get(key, access_mode='to_identity')
            
            if hasattr(value, 'toDICT'):
                if recurse >= 0:
                    childfields = None
                    if hasattr(fields,'get'):
                        childfields = fields.get(key)
                    value = value.toDICT(recurse=recurse - 1, fields=childfields)
                else:
                    value = {}
            data[key] = value
        return data

    def toJSON(self, recurse=1, fields=None):
        data = self.toDICT(recurse=recurse, fields=fields)
        return jsondumps(data)

# ----- Change Data -----

    def update(self, *args, **kwargs):
        access_mode = kwargs.pop('access_mode', 'to_internal')
    
        return self._update(access_mode=access_mode, *args, **kwargs)

    def _update(self, *args, **kwargs):
        # print 'UPDATE', args, kwargs
        # import traceback
        # traceback.print_stack()
        access_mode = kwargs.pop('access_mode', 'to_identity')
        _for_loading = kwargs.pop('_for_loading', False)
        
        tmp = OrderedDict()
        tmp.update(*args, **kwargs)
        
        for column_key, value in tmp.iteritems():
            if column_key == 'row_key':
                self.row_key = self._row_key_spec.to_internal(value)
                continue
                    
            if column_key == self._row_key_name:
                self.row_key = self._row_key_spec.to_internal(value)
                continue
            spec = self.column_spec.get(column_key, self._default_field)
            column_key, value = getattr(spec, access_mode)(column_key, value)
            self.set_value_for_columnkey(column_key, value, dont_mark=_for_loading)
        
        if not self._beensaved:
            for column_key in self.column_values:
                self.markChanged(column_key)

    def markChanged(self, column_key):
        self.column_changed[column_key] = True

    def unmarkChanged(self, column_key):
        if column_key in self.column_changed:
            del self.column_changed[column_key]

    # def delete(self, column_key):
    #     # XXX: keep track of delete
    #     # XXX: can't delete if default columnspec is 'mandatory'.
    #     spec = self.get_spec_for_columnkey(column_key)
    #     if spec.mandatory:
    #         raise TragedyException('Trying to delete mandatory column %s' % (column_key,))
    #     del self.column_values[column_key]

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
    def get_slice_predicate(column_names=None, start='', finish='', reversed=False, count=20, *args, **kwargs):
        if column_names:
            return SlicePredicate(column_names=column_names)
            
        slice_range = SliceRange(start=start, finish=finish, reversed=reversed, count=count)
        return SlicePredicate(slice_range=slice_range)
    
    @staticmethod
    def decodeColumn(colOrSuper):
        # print 'DECODE', colOrSuper.column.name, colOrSuper.column.clock
        return (colOrSuper.column.name, colOrSuper.column.value)
    
    @buchtimer()
    def delete(self, **kwargs):
        if not kwargs.get('write_consistency_level'):
            kwargs['write_consistency_level'] = None
        
        newtimestamp = self._timestamp_func()    
        clock=Clock(timestamp=newtimestamp)

        if kwargs.get('column_names'):
            print 'DELETE WITH COLUMN NAMES', kwargs.get('column_names'), kwargs
            sp = self.get_slice_predicate(**kwargs)
            deletion = Deletion(clock=clock, predicate=sp)
            delmutation = Mutation(deletion=deletion)
        
            mumap = {self.row_key: {self._column_family: [delmutation]} }
            print 'PREFUCKER', self.load()
            
            print 'DELMUMAP', mumap
            self.getclient().batch_mutate(
                                          mutation_map=mumap,
                                          consistency_level=self._wcl(kwargs['write_consistency_level']),
                                         )
            print 'FUCKER', self.load()
        else:
            cp = ColumnPath(column_family=self._column_family)
            self.getclient().remove(self.row_key, cp, clock, self._wcl(kwargs['write_consistency_level']))
            print 'FULLDELFUCKER', self.load()
    
    @classmethod
    @buchtimer()    
    def load_multi(cls, ordered=True, *args, **kwargs):
        unordered = {}
        if not kwargs['keys']:
            raise StopIteration

        k = []
        for row_key in kwargs['keys']:
            assert row_key, 'Empty row_key %s' % (row_key,)
            row_key = RowKeySpec().to_internal(row_key)
            k.append(row_key) # XXX: make non-generic
            
            assert isinstance(row_key, basestring), 'Row Key %s is of type %s should be basestring.' % (row_key, type(row_key,))
        
        kwargs['keys'] = k
        
        # print 'LOADMULTI', kwargs['keys']
        
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
            # print 'WHUT', blah, blah.pop(cls._row_key_name, None)
            # print 'WHUT', blah, blah.pop('row_key', None)
            
            access_mode = blah.pop('access_mode','to_identity')
            _for_loading = blah.pop('_for_loading', True)
            
            yield cls( row_key, blah.items(), access_mode=access_mode, _for_loading=_for_loading)
    
    def load(self, *args, **kwargs):
        if not self.row_key and self._row_key_spec.default:
                self.row_key = self._row_key_spec.get_default()
        self.row_key = self._row_key_spec.to_internal(self.row_key)
        assert self.row_key, 'No row_key and no non-null non-empty keys argument. Did you use the right row_key_name?'
        tkeys = [self.row_key]
        # print 'AHAH', self.row_key, self._row_key_name, args, kwargs
        result = list(self.load_multi(keys=tkeys, *args, **kwargs))
        # print 'FROB', list(result[0].yield_column_key_value_pairs())
        self._update(list(result[0].yield_column_key_value_pairs()), _for_loading=True)
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

    @buchtimer()
    def save(self, *args, **kwargs):
        if kwargs.pop('force_save', False):
            self._beensaved = False
        # print '=' * 30
        # print 'ASKED TO SAVE', self._column_family, args, kwargs, self.column_changed
        if not kwargs.get('write_consistency_level'):
            kwargs['write_consistency_level'] = None
        
        if not self.row_key:
            if self._row_key_spec.autogenerate:
                self.generate_row_key()
            elif self._row_key_spec.default:
                self.row_key = self._row_key_spec.get_default()
            else:
                raise TragedyException('No row_key set!')
            
            self.column_changed = dict([(k, True) for k in self.column_values.keys()])
        
        self.row_key = self._row_key_spec.to_internal(self.row_key)
        
        for save_row_key in itertools.chain((self.row_key,), self.mirrors):
            if callable(save_row_key):
                save_row_key = save_row_key()
            self._real_save(save_row_key=save_row_key, *args, **kwargs)
        
        for hook in self.save_hooks:
            # print 'CALLING HOOK', hook
            hook(self)
        
        self._beensaved = True
        
        # print '-ByE' + '-' * 30
        
        return self
        
    
    @buchtimer()
    def _real_save(self, save_row_key=None, *args, **kwargs):
        save_columns = []
        if not self.column_changed:
            # print 'NOT SAVING', self._column_family, self.column_changed
            return
        for column_key, value in self.yield_column_key_value_pairs(for_saving=True):
            assert isinstance(value, basestring), 'Not basestring %s:%s (%s)' % (column_key, type(value), type(self))
            newtimestamp = self._timestamp_func()
            import time
            foo = self.get_spec_for_columnkey(column_key)
            # print 'STORING WITH NEWTIMESTAMP', save_row_key, self.__class__._column_family, repr(column_key), foo.key, value, newtimestamp, time.ctime( int(newtimestamp) ) 
            # print 'TIMESTAMP', int(newtimestamp)
            # print 'OPHAI', column_key
            column = Column(name=column_key, value=value, clock=Clock(timestamp=newtimestamp))
            save_columns.append( ColumnOrSuperColumn(column=column) )

        # print '_REAL_SAVE', self._column_family, save_row_key, save_columns
        save_mutations = [Mutation(column_or_supercolumn=sc) for sc in save_columns]
                
        # self.getclient().batch_insert(#keyspace         = str(self._keyspace),
        #                          key              = save_row_key,
        #                          cfmap            = {self._column_family: save_columns},
        #                          consistency_level= self._wcl(kwargs['write_consistency_level']),
        #                         )
        mumap = {save_row_key: {self._column_family: save_mutations} }
        # print 'WHUT', mumap
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
    
    def __init__(self, *args, **kwargs):
        # print 'DICTROW INIT'
        super(DictRow, self).__init__(*args, **kwargs)
        # print 'NOW WE DO2', self._row_key_name
        
    
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
            value = getattr(spec.value, access_mode)(value)
        else:
            value = default
        return value