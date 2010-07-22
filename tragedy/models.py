from .rows import DictRow
from .columns import (ByteField, 
                      TimeField,
                      TimeSpec,
                      UnicodeSpec,
                      TimeStampSpec,
                      ManualIndexField,
                      SecondaryIndexField,
                      ForeignKeySpec,
                      Field,
                      RowKeySpec,
                     )
from . util import uuid
from .exceptions import TragedyException

from .hierarchy import cmcache

from .util import buchtimer

class Model(DictRow):
    _auto_timestamp = True
    __abstract__ = True
    
    def __init__(self, *args, **kwargs):
        DictRow.__init__(self, *args, **kwargs)
        for key, value in self.__class__.__dict__.items():
            # print 'BLAH', key, GeneratedIndex in getattr(value, '__bases__', ())
            if getattr(value, '_generated', None) and \
                   (not getattr(value, '_default_key', None)) and getattr(value, 'autosetrow', False):
                print 'OHAI INIT', self, key, value
                bah = value(row_key=lambda: self.row_key)
                setattr(self, key, bah)
                # print 'FROB', bah
                # print 'SETTING DEFAULT', value, self
                # value._default_key = self
    
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
            if isinstance(value, Field):
                value.set_owner_and_name(cls, key)
    
    @classmethod
    def _init_stage_two(cls, *args, **kwargs):
        super(Model, cls)._init_stage_two(*args, **kwargs)
        cls._activate_autoindexes()
    
    @classmethod
    def _activate_autoindexes(cls):
        for key, value in cls.__dict__.items():
            if isinstance(value, ManualIndexField):
                # print 'SCREAM', cls, key, value, value.target_field,getattr(value.target_field,'_name',None), value.target_model, 'Auto_%s_%s' % (value.target_model._column_family, key) 
                row_key_name = getattr(value.target_field,'_name',None)
                row_key_name = row_key_name if row_key_name else cls._column_family.lower()
                
                default_field = value.target_model
                if value.target_field:
                    target_fieldname = getattr(value.target_field, '_name', None)
                else:
                    target_fieldname = None
                default_key = getattr(value, 'default_key', None)
                # print 'AUTOSETROW', cls._column_family, getattr(value, 'autosetrow')
                autosetrow = getattr(value, 'autosetrow', False)
                order_by = getattr(value, 'order_by', 'TimeUUIDType')
                if order_by == 'TimeUUIDType':
                    defkeyspec = TimeStampSpec()
                    defvalspec = ForeignKeySpec(foreign_class=default_field, unique=True)
                elif order_by == 'BytesType':
                    defkeyspec = UnicodeSpec()
                    defvalspec = ForeignKeySpec(foreign_class=default_field, unique=True)
                
                class ManualIndexImplementation(BaseIndex):
                    _column_family = 'Auto_%s_%s' % (cls._column_family, key)
                    _default_field = Field(key=defkeyspec, 
                                           value=defvalspec)
                    _index_name = key
                    _target_fieldname = target_fieldname
                    _default_key = default_key
                    _autosetrow = autosetrow
                    _order_by = order_by
                    _row_key_name = row_key_name
                    _generated = True
                    
                    def __init__(self, *args, **kwargs):
                        BaseIndex.__init__(self, *args, **kwargs)
                        # print 'START ME UP', args, kwargs, self.row_key
                        
                        
                        # import traceback
                        # traceback.print_stack()
                        
                        if self._default_key and not self.row_key:
                            if not isinstance(self._default_key, basestring):
                                self.row_key = self._default_key.row_key
                            else:
                                self.row_key = self._default_key
                        
                        # print 'KAH', self._column_family, self.row_key, args, kwargs
                        
                        # print 'INITING', list( [str(x) for x in self.get_spec_for_columnkey(column_key).to_display(column_key,value)] for column_key,value in 
                        #                 self.yield_column_key_value_pairs())
                    
                    @classmethod
                    def target_saved(cls, instance):
                        # print 'AUTOSAVE', cls._column_family, cls._index_name, getattr(cls,'_target_fieldname', None), instance.row_key, instance
                        default_key = cls._default_key
                        # print 'WORKING WITH', cls._column_family, cls._target_fieldname, default_key
                        
                        if default_key:
                            tmp = cls(default_key)
                            tmp.append(instance)
                            tmp.save()
                        else:
                            seckey = instance.get(cls._target_fieldname)
                            mandatory = getattr(getattr(instance, cls._target_fieldname), 'mandatory', False)
                            print 'FINAL KEY', seckey
                            if seckey:
                                tmp = cls( seckey )
                                tmp.append(instance)
                                tmp.save()
                                # print 'WHOA SAVED', cls(seckey).load()
                            elif (not seckey) and mandatory:
                                raise TragedyException('Mandatory Secondary Field %s not present!' % (cls._target_fieldname,))
                            else:
                                print 'SKIPPING'
                                pass # not mandatory
                
                # print 'OHAIFUCK TARGETMODEL', cls._column_family, value.target_model 
                setattr(ManualIndexImplementation, row_key_name, RowKeySpec())
                # print 'SETTING', cls, key, ManualIndexImplementation
                setattr(cls, key, ManualIndexImplementation)
                # print getattr(cls, key)
                
                if getattr(value, 'autosave', False):
                    cls.save_hooks.add(ManualIndexImplementation.target_saved) 

class BaseIndex(DictRow):
    """A row which doesn't care about column names, and that can be appended to."""
    __abstract__ = True
    # _default_field = ByteField()
    _order_by = 'TimeUUIDType'
    _ordered = True

    def __call__(*args, **kwargs):
        print 'calling this baseindex instance is deprecated'

    def __init__(self, *args, **kwargs):
        # print 'BASEINDEX INIT'        
        super(BaseIndex, self).__init__(*args, **kwargs)
        # print 'NOW WE DO3', self._row_key_name

    @classmethod
    def _init_class(cls, *args, **kwargs):
        super(BaseIndex, cls)._init_class(*args, **kwargs)
        if hasattr(cls, 'targetmodel'):
            cls._default_field = cls.targetmodel
            del cls.targetmodel

    # def is_unique(self, target):
    #     # if self._order_by != 'TimeUUIDType':
    #     #     return True
    #     return True
    #         
    #     MAXCOUNT = 20000000
    #     self.load(count=MAXCOUNT) # XXX: we will blow up here at some point
    #                               # i don't know where the real limit is yet.
    #     assert len(self.column_values) < MAXCOUNT - 1, 'Too many keys to enforce sorted uniqueness!'
    #     mytarget = self._default_field.value.to_internal(target)
    #     if mytarget in self.itervalues():
    #         return False
    #     return True
        
    def get_next_column_key(self):
        # assert self._order_by == 'TimeUUIDType', 'Append makes no sense for sort order %s' % (self._order_by,)
        return uuid.uuid1().bytes
        
    @buchtimer()
    def append(self, target):
        # print 'ASKED TO APPEND', target
        # assert self._order_by == 'TimeUUIDType', 'Append makes no sense for sort order %s' % (self._order_by,)
        # if (self._default_field.value.unique and not self.is_unique(target)):
        #     return self
        
        if not isinstance(target, basestring):
            if target._beensaved:
                # print 'NOT STORING', self._column_family, target._column_family, target
                return self
            if isinstance(target, self._default_field.value.foreign_class): #, "Trying to store ForeignKeySpec of wrong type!"
                target = self._default_field.value.to_internal(target)
        
        # print 'APPENDCODE', target, self._default_field
        # print 'SUPERTARGET', target, self._default_field.value.foreign_class, self._default_field.value_to_display(target)
                
        column_key = self.get_next_column_key()
        # print 'WTF COLUMN KEY', uuid.uuid1(), uuid.UUID(bytes=column_key).hex, target

        # self.load()
        # print 'APPEND', self, target
        self._update( [(column_key, target)] )
        return column_key

    def loadIterItems(self):
        return itertools.izip(self.iterkeys(), self.loadIterValues())

    def loadIterValues(self):
        vals = self.values()
        if vals:
            return self._default_field.value.foreign_class.load_multi(keys=vals) #orderdata=self.keys())
            # if self._order_by == 'TimeUUIDType':
            #     return self._default_field.value.foreign_class.load_multi(keys=vals) #orderdata=self.keys())
            # else:
            #     return vals
        return []

    def resolve(self):
        return self.loadIterValues()

    def __iter__(self):
        for row_key in self.itervalues():
            yield self._default_field.value.foreign_class(row_key=row_key)