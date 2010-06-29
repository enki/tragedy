import time
import uuid
from . import timestamp
from datetime import datetime
import simplejson as json
from .exceptions import TragedyException
from .hierarchy import cmcache

class Spec(object):
    default = False
    unique = False
    
    def __init__(self, *args, **kwargs):
        pass
    
    def to_internal(self, data): # called for userinput (before adding)
        return data
        
    def to_display(self, data): # called before displaying data
        return self.to_external(data)
        
    def to_external(self, data): # turn data into object for use outside of tragedy
        return data
        
    def to_identity(self, data): # don't modify
        return data
    
    def get_default(self):
        if callable(self.default):
            d = self.default()
        else:
            d = self.default
        return d
    
    def for_saving(self, internal):
        return internal

class MissingSpec(Spec):
    def to_internal(self, data): # called for userinput (before adding)
        raise NotImplementedError('Missing field: %s' %(data,))
        
    def to_display(self, data): # called before displaying data
        raise NotImplementedError('Missing field: %s' %(data,))
                        
    def to_external(self, data): # turn data into object for use outside of tragedy
        raise NotImplementedError('Missing field: %s' %(data,))
                        
    def to_identity(self, data): # don't modify
        raise NotImplementedError('Missing field: %s' %(data,))
                        
    def for_saving(self, internal):
        raise NotImplementedError('Missing field: %s' %(internal,))
        
class UnicodeSpec(Spec):    
    def to_internal(self, elem):
        if isinstance(elem, unicode):
            elem = elem.encode('utf-8')
        return elem

class AsciiSpec(Spec):    
    def to_internal(self, elem):
        return str(elem)

class AutosetSpec(Spec):
    def __init__(self, *args, **kwargs):
        self._autoset_on_create = kwargs.pop('autoset_on_create', False)    
        self._autoset_on_save = kwargs.pop('autoset_on_save', False) 
    
        if self._autoset_on_create or self._autoset_on_save:
            self.default = lambda: self.to_internal(None)
        
        super(AutosetSpec, self).__init__(self, *args, **kwargs)
    
    def for_saving(self, value):
        if self._autoset_on_save:
            return self.get_default()
        else:
            return value
    
class TimeSpec(AutosetSpec):    
    def __init__(self, *args, **kwargs):
        self._microseconds = kwargs.pop('microseconds', True)    
        self._utc = kwargs.pop('utc', True)        
        super(TimeSpec, self).__init__(self, *args, **kwargs) 

    def to_display(self, value): # called before displaying data
        return str(value)

    def to_external(self, value):
        return timestamp.importUnix( float(value) )

    def to_internal(self, value):
        if value is None:
            _nowfunc = datetime.utcnow if self._utc else datetime.now
            value = _nowfunc()
        return str(timestamp.exportUnix(value, self._microseconds))

class TimestampSpec(AutosetSpec):
    def __init__(self, *args, **kwargs):
        super(TimestampSpec, self).__init__(self, *args, **kwargs) 
        
    def to_display(self, value): # called before displaying data
        return time.ctime(timestamp.fromUUID(uuid.UUID(bytes=value)))

    def to_external(self, value):
        return uuid.UUID(bytes=value).hex
    
    def to_internal(self, value):
        if value is None:
            value = uuid.uuid1().hex
        return uuid.UUID(hex=value).bytes

class ForeignKeySpec(Spec):
    def __init__(self, *args, **kwargs):
        self.foreign_class = kwargs.pop('foreign_class')
        
        self.resolve = kwargs.pop('resolve', False)
        self.unique = kwargs.pop('unique', False)
        super(ForeignKeySpec, self).__init__(self, *args, **kwargs)
        
    def to_external(self, row_key):
        instance = self.foreign_class(row_key=row_key)
        if self.resolve:
            instance.multiget_slice()
        return instance
    
    def to_internal(self, instance):
        if hasattr(instance, 'row_key'):
            return instance.row_key
        return instance

class IntegerSpec(Spec):
    def to_external(self, value):
        return int(value)
    
    def to_internal(self, value):
        return str(int(value))

class FloatSpec(Spec):
    def to_external(self, value):
        return float(value)
    
    def to_internal(self, value):
        return str(float(value))

class BooleanSpec(Spec):    
    def to_external(self, value):
        if value == True or value == "1":
            return True
        return False
    
    def to_internal(self, value):
        return "1" if value else "0"

class JSONSpec(Spec):
    def to_internal(self, value):
        return json.dumps(value)
    
    def to_external(self, value):
        return json.loads(value)

class RowKeySpec(Spec):
    def __init__(self, *args, **kwargs):
        self.autogenerate = kwargs.pop('autogenerate', False)
        self.default = kwargs.pop('default', None)
        Spec.__init__(self, *args, **kwargs)
    
    def to_internal(self, value):
        if callable(value):
            value = value()
            
        if hasattr(value, 'row_key'):
            value = value.row_key
        
        return value
