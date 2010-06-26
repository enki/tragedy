from .rows import DictRow, RowKey
from .columns import (ByteField, 
                      TimeField,
                      ManualIndex,
                      SecondaryIndex,
                      ForeignKey,
                      BaseField,
                     )
import uuid
from .exceptions import TragedyException

from .hierarchy import cmcache

class Model(DictRow):
    _auto_timestamp = True
    __abstract__ = True
    
    def __init__(self, *args, **kwargs):
        DictRow.__init__(self, *args, **kwargs)
        for key, value in self.__class__.__dict__.items():
            # print 'BLAH', key, GeneratedIndex in getattr(value, '__bases__', ())
            if GeneratedIndex in getattr(value, '__bases__', ()) and \
                   not getattr(value, '_default_key', None) and getattr(value, '_autosetrow', False):
                # print 'OHAI INIT', self.__class__, key
                value._default_key = self
    
    @classmethod
    def _init_class(cls, *args, **kwargs):
        super(Model, cls)._init_class(*args, **kwargs)
        # print 'STAGE1', cls
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
            if isinstance(value, ManualIndex):
                # print 'SCREAM', cls, key, value, value.target_field, 'Auto_%s_%s' % (value.target_model._column_family, key) 
                default_field = value.target_model
                if value.target_field:
                    target_fieldname = getattr(value.target_field, '_name', None)
                else:
                    target_fieldname = None
                default_key = getattr(value, 'default_key', None)
                autosetrow = getattr(value, 'autosetrow', False)
                
                class ManualIndexImplementation(GeneratedIndex):
                    _column_family = 'Auto_%s_%s' % (cls._column_family, key)
                    _default_field = ForeignKey(foreign_class=default_field, unique=True)
                    _index_name = key
                    _target_fieldname = target_fieldname
                    _default_key = default_key
                    _autosetrow = autosetrow
                    
                    def __init__(self, *args, **kwargs):
                        TimeOrderedIndex.__init__(self, *args, **kwargs)
                        
                        if self._default_key and not self.row_key:
                            if not isinstance(self._default_key, basestring):
                                self.row_key = self._default_key.row_key
                            else:
                                self.row_key = self._default_key
                    
                    @classmethod
                    def target_saved(cls, instance):
                        # print 'AUTOSAVE', cls._column_family, cls._index_name, getattr(cls,'_target_fieldname', None), instance.row_key, instance
                        default_key = cls._default_key
                        # print 'WORKING WITH', cls._column_family, cls._target_fieldname, default_key
                        
                        if default_key:
                            cls(default_key).append(instance).save()
                        else:
                            seckey = instance.get(cls._target_fieldname)
                            mandatory = getattr(getattr(instance, cls._target_fieldname), 'mandatory', False)
                            if seckey:
                                cls( seckey ).append(instance).save()
                            elif (not seckey) and mandatory:
                                raise TragedyException('Mandatory Secondary Field %s not present!' % (cls._target_fieldname,))
                            else:
                                pass # not mandatory
                
                # print 'OHAIFUCK TARGETMODEL', cls._column_family, value.target_model 
                setattr(ManualIndexImplementation, cls._column_family.lower(), RowKey())
                # print 'SETTING', cls, key, ManualIndexImplementation
                setattr(cls, key, ManualIndexImplementation)
                # print getattr(cls, key)
                
                if getattr(value, 'autosave', False):
                    cls.save_hooks.add(ManualIndexImplementation.target_saved) 

class Index(DictRow):
    """A row which doesn't care about column names, and that can be appended to."""
    __abstract__ = True
    # _default_field = ByteField()
    _order_by = 'TimeUUIDType'
    _ordered = True

    @classmethod
    def _init_class(cls, *args, **kwargs):
        super(Index, cls)._init_class(*args, **kwargs)
        if hasattr(cls, 'targetmodel'):
            cls._default_field = cls.targetmodel
            del cls.targetmodel

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
        
        # print 'APPENDCODE', target, self._default_field
        assert isinstance(target, self._default_field.foreign_class), "Trying to store ForeignKey of wrong type!"
        target = self._default_field.value_to_internal(target)
        # print 'SUPERTARGET', target, self._default_field.foreign_class, self._default_field.value_to_display(target)
                
        column_key = self.get_next_column_key()

        self._update( [(column_key, target)] )
        return self

    def loadIterItems(self):
        return itertools.izip(self.iterkeys(), self.loadIterValues())

    def loadIterValues(self):
        if self.values():
            return self._default_field.foreign_class.load_multi(keys=self.values()) #orderdata=self.keys())
        return []

    def resolve(self):
        return self.loadIterValues()

    def __iter__(self):
        for row_key in self.itervalues():
            yield self._default_field.foreign_class(row_key=row_key)

class TimeOrderedIndex(Index):
    __abstract__ = True
    _order_by = 'TimeUUIDType'

class GeneratedIndex(TimeOrderedIndex):
    __abstract__ = True