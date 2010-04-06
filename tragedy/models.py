from __future__ import absolute_import
import sys
import pycassa
import simplejson as json
import uuid
import traceback
import types

import pycassa

from . import fields

class ModelType(type):
    def __new__(cls, name, bases, attrs):
        parents = [b for b in bases if isinstance(b, ModelType)]
        if not parents:
            # This is the model itself, not a child            
            model_cls = super(ModelType, cls).__new__(cls, name, bases, attrs)
            return model_cls
        
        new_cls = super(ModelType, cls).__new__(cls, name, bases, attrs)
        
        if not hasattr(new_cls, 'Meta'):
            raise Exception("Class has no Meta definition.")
        
        new_cls._processMeta()
        new_cls._setManagerFromMeta()
        
        return new_cls
    
    def _processMeta(cls):
        name = cls.__name__
        meta = getattr(cls, 'Meta')
        keyspace = getattr(meta, 'keyspace', None)
        if keyspace is None:
            raise Exception("Class-Meta has no Keyspace.")
        
        column_family = getattr(meta, 'column_family', name)
        client = getattr(meta, 'client', None)
        if not client:
            client = sys.modules.get('tragedyclient')
        
        setattr(meta, 'keyspace', keyspace)
        setattr(meta, 'column_family', column_family)
        setattr(meta, 'client', client)
        
    def _setManagerFromMeta(cls):
        if cls.Meta.client:
            cls.Meta._cfamily = pycassa.ColumnFamily(cls.Meta.client, cls.Meta.keyspace, cls.Meta.column_family)
            cls.objects = pycassa.ColumnFamilyMap(cls, cls.Meta._cfamily)

def unhandled_exception_handler():
    tb = sys.exc_info()[2]
    stack = []

    while tb:
        stack.append(tb.tb_frame)
        tb = tb.tb_next

    traceback.print_exc()

    for frame in stack:
            print
            print "Frame %s in %s at line %s" % (frame.f_code.co_name,
                                                 frame.f_code.co_filename,
                                                 frame.f_lineno)
            for key, value in frame.f_locals.items():
                print "\t%20s = " % key,
                try:                   
                    print value
                except:
                    print "<ERROR WHILE PRINTING VALUE>"

class Model(object):
    __metaclass__ = ModelType
    
    unhandled_exception_handler = staticmethod(unhandled_exception_handler)

    def setManagerFromMeta(self):
        """Override binding for instance."""
        self.Meta._cfamily = pycassa.ColumnFamily(self.Meta.client, self.Meta.keyspace, self.Meta.column_family)
        self.objects = pycassa.ColumnFamilyMap(self.__class__, self.Meta._cfamily)

    def __init__(self, key=None, client=None):
        if client:
            self.Meta.client = client
            self.setManagerFromMeta()
        self.key = key
        self.possiblekeys = set(self.objects.columns.keys())
    
    def activecolumns(self, complain=False, reportbadmisses=False):
        columns = []
        failure = False
        for name, value in self.objects.columns.items():
            if name in self.__dict__.keys():
                columns.append(name)
            elif not value.required:
                continue
            elif complain:
                raise Exception("Row lacks required column '%s'." % RowLacksRequiredColumn(name))
                
        return columns
    
    def save(self):
        if self.key is None:
            if getattr(self.Meta, 'generate_rowkey_if_empty', False):
                self.key = uuid.uuid4().hex
            else:
                raise Exception('Trying to save with undefined row key.')
        
        columns = self.activecolumns(complain=True)
        self.objects.insert(instance=self, columns=columns)
    
    def combine_columns(self, column_dict, columns):
        combined_columns = {}
        for column, type in column_dict.iteritems():
            combined_columns[column] = type.default
        for column, value in columns.iteritems():
            if column not in column_dict.keys():
                try:
                    raise Exception("Impossible key for combining: %s" % (column,))
                except:
                    self.unhandled_exception_handler()
                continue
            combined_columns[column] = column_dict[column].unpack(value)
        return combined_columns
    
    def load(self):
        rowkey, freshcolumns = (self.objects.column_family.get_range(start=self.key, finish=self.key,
                               column_start='', column_finish='', column_count=1, row_count=1)).next()
        assert self.key == rowkey, "Received different key %s than what I asked for (%s)" % (rowkey, self.key)
        columns = self.combine_columns(self.objects.columns, freshcolumns)
        self.update(columns)
    
    def setIfPossible(self, key, value):
        if key in self.possiblekeys:
            setattr(self, key, value)
        else:
            warn(Exception("IMPOSSIBLE KEY FROM SERVER: %s: %s" % (key, value)))
    
    def update(self, arg=None, **kwargs):
        if not (arg is None):
            if hasattr(arg, 'keys'):
                for key in arg:
                    self.setIfPossible(key, arg[key])
            elif 'arg' in possiblekeys:
                setattr(self, 'arg', arg)
            else:
                for key, value in arg:
                    self.setIfPossible(key, value)
        
        for key, value in kwargs:
            self.setIfPossible(key, value)

    def __repr__(self):
        return '<%s(%s): %s>' % (
                self.__class__.__name__,
                self.key,
                json.dumps( {col:getattr(self, col) for col in self.activecolumns()})
        )
