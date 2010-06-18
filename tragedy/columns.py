import time
import uuid
from . import timestamp
from datetime import datetime
import simplejson as json
from .exceptions import TragedyException
from .hierarchy import cmcache

class BaseField(object):
    def set_owner_and_name(self, owner, name):
        self._owner = owner
        self._name = name
    
    def get_owner(self):
        assert self._owner, "Owner can't be none!"
        return self._owner

class ConvertAPI(BaseField):
    default = False
    unique = False
    _owner = None
    
    def __init__(self, *args, **kwargs):
        self.mandatory = kwargs.pop('mandatory', True)
    
    def to_internal(self, column_key, value):
        return self.key_to_internal(column_key), self.value_to_internal(value)

    def key_to_internal(self, column_key): # called for userinput (before adding)
        return column_key

    def value_to_internal(self, value): # called for userinput (before adding)
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        return value

    def to_display(self, column_key, value):
        return self.key_to_display(column_key), self.value_to_display(value)

    def key_to_display(self, column_key): # called before displaying data
        return self.key_to_external(column_key)

    def value_to_display(self, value): # called before displaying data
        return self.value_to_external(value)

    def to_external(self, column_key, value):
        return self.key_to_external(column_key), self.value_to_external(value)
    
    def key_to_external(self, column_key): # turn data into object for use outside of tragedy
        return column_key

    def value_to_external(self, value): # turn data into object for use outside of tragedy
        return value

    def to_identity(self, column_key, value):
        return self.key_to_identity(column_key), self.value_to_identity(value)

    def key_to_identity(self, column_key): # don't modify
        return column_key

    def value_to_identity(self, value): # don't modify
        return value

    def get_default(self):
        if callable(self.default):
            return self.default()
        else:
            return self.default

    def value_for_saving(self, value):
        return value

class Field(ConvertAPI):
    pass
    
class ByteField(Field):    
    def key_to_internal(self, column_key):
        return str(column_key)

class AsciiField(Field):    
    def key_to_internal(self, column_key):
        return str(column_key)

class UnicodeField(Field):    
    def key_to_internal(self, column_key):
        if isinstance(column_key, unicode):
            column_key = column_key.encode('utf-8')
        return column_key

class TimeField(Field):    
    def __init__(self, *args, **kwargs):
        self._autoset_on_create = kwargs.pop('autoset_on_create', False)    
        self._autoset_on_save = kwargs.pop('autoset_on_save', False) 
        self._microseconds = kwargs.pop('microseconds', True)    
        self._utc = kwargs.pop('utc', True)
        
        self._nowfunc = datetime.utcnow if self._utc else datetime.now
        
        if self._autoset_on_create or self._autoset_on_save:
            self.default = lambda: self.value_to_internal(self._nowfunc())
        super(TimeField, self).__init__(self, *args, **kwargs) 

    def value_to_display(self, value): # called before displaying data
        return str(value)

    def value_to_external(self, value):
        return timestamp.importUnix( float(value) )

    def value_to_internal(self, value):
        return str(timestamp.exportUnix(value, self._microseconds))

    def value_for_saving(self, value):
        if self._autoset_on_save:
            return self.get_default()
        else:
            return value

class TimestampField(Field):
    def __init__(self, *args, **kwargs):
        autoset_on_create = kwargs.pop('autoset_on_create', False)    
        if autoset_on_create:
            self.default = lambda: uuid.uuid1().bytes
        super(TimestampField, self).__init__(self, *args, **kwargs) 
        
    def value_to_display(self, value): # called before displaying data
        return time.ctime(timestamp.fromUUID(uuid.UUID(bytes=value)))

    def value_to_external(self, value):
        return uuid.UUID(bytes=value).hex
    
    def value_to_internal(self, value):
        return uuid.UUID(hex=value).bytes

class ForeignKey(Field):
    def __init__(self, *args, **kwargs):
        self.foreign_class = kwargs.pop('foreign_class')
        self.resolve = kwargs.pop('resolve', False)
        self.unique = kwargs.pop('unique', False)
        super(ForeignKey, self).__init__(self, *args, **kwargs)
        
    def value_to_external(self, row_key):
        instance = self.foreign_class(row_key=row_key)
        if self.resolve:
            instance.multiget_slice()
        return instance
    
    def value_to_internal(self, instance):
        if hasattr(instance, 'row_key'):
            return instance.row_key
        return instance

class MissingField(Field):
    def key_to_internal(self, column_key):
        raise TragedyException('No Specification for Key %s' % (column_key,))

class IntegerField(Field):
    def value_to_external(self, value):
        return int(value)
    
    def value_to_internal(self, value):
        return str(int(value))

class FloatField(Field):
    def value_to_external(self, value):
        return float(value)
    
    def value_to_internal(self, value):
        return str(float(value))

class BooleanField(Field):    
    def value_to_external(self, value):
        if value == True or value == "1":
            return True
        return False
    
    def value_to_internal(self, value):
        return "1" if value else "0"

class JSONField(Field):
    def value_to_internal(self, value):
        return json.dumps(value)
    
    def value_to_external(self, value):
        return json.loads(value)

DictField = JSONField
ListField = JSONField

class CustomIndex(BaseField):
    pass

class ObjectIndex(BaseField):
    default_key = None
    autosetrow = True
    def __init__(self, target_model, *args, **kwargs):
        self._target_model = target_model
        self.target_field = None

    def doresolve(self):
        if isinstance(self._target_model, basestring):
            for keyspace in cmcache.retrieve('keyspaces'):
                for mname, model in keyspace.models.items():
                    if mname == self._target_model:
                        self._target_model = model
                        break
                if not isinstance(self._target_model, basestring):
                    break

    @property
    def target_model(self):
        self.doresolve()
        return self._target_model

class SecondaryIndex(ObjectIndex):
    autosave = True
    autosetrow = False
    def __init__(self, target_field, *args, **kwargs):
        self.target_field = target_field

    @property
    def target_model(self):
        # print 'OWNER', self.target_field.get_owner()
        return self.target_field.get_owner()

class AllIndex(ObjectIndex):
    autosave = True
    default_key = '!ALL!'
    def __init__(self, *args, **kwargs):
        self.target_field = self
    
    @property
    def target_model(self):
        return self.target_field.get_owner()