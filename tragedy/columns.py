import time
import uuid
from . import timestamp

class ColumnSpec(object):
    def __init__(self, required=True):
        self.required = required
        
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

class IdentityColumnSpec(ColumnSpec):
    pass

class TimeUUIDColumnSpec(ColumnSpec):
    def key_to_display(self, column_key): # called before displaying data
        return time.ctime(timestamp.fromUUID(uuid.UUID(bytes=column_key)))

    def key_to_external(self, column_key):
        return uuid.UUID(bytes=column_key).hex
    
    def key_to_internal(self, column_key):
        return uuid.UUID(hex=column_key).bytes

class MissingColumnSpec(ColumnSpec):
    def key_to_internal(self, column_key):
        raise Exception('No Specification for Key %s' % (column_key,))

class BooleanColumnSpec(ColumnSpec):    
    def value_to_external(self, value):
        if value == True or value == "1":
            return True
        return False
    
    def value_to_internal(self, value):
        return "1" if value else "0"