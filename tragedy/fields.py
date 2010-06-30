from .specs import *

class Field(object):
    _owner = None
    key = None
    value = None
    def __init__(self, *args, **kwargs):
        self.mandatory = kwargs.pop('mandatory', True)
        self.key = kwargs.pop('key', self.key)
        self.value = kwargs.pop('value', self.value)
        assert self.key
        assert self.value
    
    def set_owner_and_name(self, owner, name):
        self._owner = owner
        self._name = name
    
    def get_owner(self):
        assert self._owner, "Owner can't be none!"
        return self._owner
    
    def to_internal(self, key, value):
        return (self.key.to_internal(key), self.value.to_internal(value))
    
    def to_identity(self, key, value):
        return (self.key.to_identity(key), self.value.to_identity(value))

    def to_display(self, key, value):
        return (self.key.to_display(key), self.value.to_display(value))

    def to_external(self, key, value):
        return (self.key.to_external(key), self.value.to_external(value))

class MissingField(Field):
    key = MissingSpec()
    value = MissingSpec()

class ByteField(Field):
    key = UnicodeSpec()
    value = AsciiSpec()

class UnicodeField(Field):
    key = UnicodeSpec()
    value = UnicodeSpec() 

class TimeField(Field):
    def __init__(self, *args, **kwargs):
        self.key = UnicodeSpec(*args, **kwargs)
        self.value = TimeSpec(*args, **kwargs)
        
        super(TimeField, self).__init__(self, *args, **kwargs)

class BooleanField(Field):
    key = UnicodeSpec()
    value = TimeSpec()

class AsciiField(Field):
    key = UnicodeSpec()
    value = AsciiSpec()

class IntegerField(Field):
    key = UnicodeSpec()
    value = IntegerSpec()

class FloatField(Field):
    key = UnicodeSpec()
    value = FloatSpec()

class ManualIndexField(Field):
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
                if not isinstance(self._target_model, basestring): # TODO: wtf?
                    break

    @property
    def target_model(self):
        self.doresolve()
        return self._target_model

class SecondaryIndexField(ManualIndexField):
    autosave = True
    autosetrow = False
    def __init__(self, target_field, *args, **kwargs):
        self.target_field = target_field

    @property
    def target_model(self):
        # print 'OWNER', self.target_field.get_owner()
        return self.target_field.get_owner()

class AllIndexField(ManualIndexField):
    autosave = True
    default_key = '!ALL!'
    def __init__(self, *args, **kwargs):
        self.target_field = self
        # super(AllIndexField, self).__init__(self, *args, **kwargs)
    
    @property
    def target_model(self):
        return self.target_field.get_owner()

class ForeignKeyField(Field):
    def __init__(self, *args, **kwargs):
        self.key = UnicodeSpec()
        self.value = ForeignKeySpec(*args, **kwargs)
        
        super(ForeignKeyField, self).__init__(self, *args, **kwargs)

class JSONField(Field):
    key = UnicodeSpec()
    value = JSONSpec()

# class ListField(Field):
#     key = UnicodeSpec()
#     value = ListSpec()
