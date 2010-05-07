from .rows import DictRow
from .columns import (ByteField, 
                      TimeField,
                     )
import uuid

class Model(DictRow):
    _auto_timestamp = True
    __abstract__ = True
    
    @classmethod
    def _init_class(cls):
        super(Model, cls)._init_class()
        if cls._auto_timestamp:
            cls.created_at = TimeField(autoset_on_create=True)
            cls.last_modified = TimeField(autoset_on_save=True)    

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
