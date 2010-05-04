import time
import uuid
from . import timestamp
import simplejson as json

class ConvertAPI(object):
    default = False
    
    def __init__(self, *args, **kwargs):
        self.mandatory = kwargs.pop('mandatory', True)
        
    def to_internal(self, column_key, value):
        return self.key_to_internal(column_key), self.value_to_internal(value)

    def key_to_internal(self, column_key): # called for userinput (before adding)
        return column_key

    def value_to_internal(self, value): # called for userinput (before adding)
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

class Field(ConvertAPI):
    compare_with = 'BytesType'

class IdentityField(Field):
    pass

class StringField(Field):
    pass

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
        self.compare_with = kwargs.pop('compare_with', 'BytesType')
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

class AutoIndex(object):
    pass
    # __autoindex__ = True
