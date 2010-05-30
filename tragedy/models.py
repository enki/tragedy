from .rows import DictRow, RowKey
from .columns import (ByteField, 
                      TimeField,
                      ObjectIndex,
                      SecondaryIndex,
                      ForeignKey,
                      BaseField,
                     )
import uuid
from .exceptions import TragedyException

class Model(DictRow):
    _auto_timestamp = True
    __abstract__ = True
    
    @classmethod
    def _init_class(cls, *args, **kwargs):
        super(Model, cls)._init_class(*args, **kwargs)
        print 'STAGE1', cls
        if cls._auto_timestamp:
            cls.created_at = TimeField(autoset_on_create=True)
            cls.last_modified = TimeField(autoset_on_save=True)
        
        cls._set_ownership_of_fields()

    @classmethod
    def _set_ownership_of_fields(cls):
        for key, value in cls.__dict__.items():
            if isinstance(value, BaseField):
                value.set_owner_and_name(cls, key)
    
    @classmethod
    def _init_stage_two(cls, *args, **kwargs):
        super(Model, cls)._init_stage_two(*args, **kwargs)
        cls._activate_autoindexes()
    
    @classmethod
    def _activate_autoindexes(cls):
        for key, value in cls.__dict__.items():
            if isinstance(value, ObjectIndex):
                print 'SCREAM', cls, key, value, value.target, 'Auto_%s_%s' % (value.target.get_owner()._column_family, key) 
                class ObjectIndexImplementation(TimeOrderedIndex):
                    _column_family = 'Auto_%s_%s' % (value.target.get_owner()._column_family, key)
                    _default_field = ForeignKey(foreign_class=value.target.get_owner(), unique=True)
                    _index_name = key
                    _target_name = value.target._name
                    _allkey = getattr(value, 'allkey', None)
                    
                    def __init__(self, *args, **kwargs):
                        TimeOrderedIndex.__init__(self, *args, **kwargs)
                        if self._allkey and not self.row_key:
                            self.row_key = self._allkey
                    
                    @classmethod
                    def target_saved(cls, target):
                        print 'AUTOSAVE', cls._column_family, cls._index_name, cls._target_name, target.row_key, target
                        allkey = cls._allkey
                        if allkey:
                            print "ALLKEY", allkey, getattr(target, cls._target_name)
                            cls(allkey).append(target).save()
                        else:
                            seckey = target.get(cls._target_name)
                            mandatory = getattr(getattr(target, cls._target_name), 'mandatory', False)
                            if seckey:
                                cls( seckey ).append(target).save()
                            elif (not seckey) and mandatory:
                                raise TragedyException('Mandatory Secondary Field %s not present!' % (cls._target_name,))
                            else:
                                pass # not mandatory
                
                setattr(ObjectIndexImplementation, value.target.get_owner()._column_family.lower(), RowKey())
                print 'SETTING', cls, key, ObjectIndexImplementation
                setattr(cls, key, ObjectIndexImplementation)
                print getattr(cls, key)
                
                if getattr(value, 'autosave', False):
                    cls.save_hooks.add(ObjectIndexImplementation.target_saved) 

class Index(DictRow):
    """A row which doesn't care about column names, and that can be appended to."""
    __abstract__ = True
    _default_field = ByteField()
    _ordered = True

    def is_unique(self, target):
        if self._order_by != 'TimeUUIDType':
            return True
            
        MAXCOUNT = 20000000
        self.load(count=MAXCOUNT) # XXX: we will blow up here at some point
                                  # i don't know where the real limit is yet.
        assert len(self.column_values) < MAXCOUNT - 1, 'Too many keys to enforce sorted uniqueness!'
        mytarget = self._default_field.value_to_internal(target)
        if mytarget in self.itervalues():
            return False
        return True
        
    def get_next_column_key(self):
        assert self._order_by == 'TimeUUIDType', 'Append makes no sense for sort order %s' % (self._order_by,)
        return uuid.uuid1().bytes
        
    def append(self, target):
        assert self._order_by == 'TimeUUIDType', 'Append makes no sense for sort order %s' % (self._order_by,)
        if (self._default_field.unique and not self.is_unique(target)):
            return self
            
        target = self._default_field.value_to_internal(target)
        
        column_key = self.get_next_column_key()

        self._update( [(column_key, target)] )
        return self

    def loadIterItems(self):
        return itertools.izip(self.iterkeys(), self.loadIterValues())

    def loadIterValues(self):
        if self.values():
            return self._default_field.foreign_class.load_multi(keys=self.values(), orderdata=self.keys())
        return []

    def resolve(self):
        return self.loadIterValues()

    def __iter__(self):
        for row_key in self.itervalues():
            yield self._default_field.foreign_class(row_key=row_key)

class TimeOrderedIndex(Index):
    __abstract__ = True
    _order_by = 'TimeUUIDType'
