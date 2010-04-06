from __future__ import absolute_import
import sys
import pycassa
import simplejson as json
import uuid
import traceback
import types

import pycassa

from .util import (unhandled_exception_handler,
                   BestDictAvailable,
                   ObjWithFakeDictAndKey)
from . import fields

def create_instance(cls, **kwargs):
    instance = cls()
    instance.update(kwargs)
    return instance

pycassa.columnfamilymap.create_instance = create_instance

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
            cls.Meta._cfamily = pycassa.ColumnFamily(cls.Meta.client, cls.Meta.keyspace, cls.Meta.column_family,
                                               dict_class=BestDictAvailable)
            cls.objects = pycassa.ColumnFamilyMap(cls, cls.Meta._cfamily)

class Model(object):
    __metaclass__ = ModelType
        
    unhandled_exception_handler = staticmethod(unhandled_exception_handler)

    def __getattr__(self, key):
        return self.__dict__['columnstorage'][key]
        
    def __setattr__(self, key, value):
        if not self.setIfPossible(self.__dict__['columnstorage'], key, value):
            self.__dict__[key] = value
            
    def setManagerFromMeta(self):
        """Override binding for instance."""
        self.Meta._cfamily = pycassa.ColumnFamily(self.Meta.client, self.Meta.keyspace, self.Meta.column_family,
                                               dict_class=BestDictAvailable)
        self.objects = pycassa.ColumnFamilyMap(self.__class__, self.Meta._cfamily)

    def __init__(self, key=None, client=None, **kwargs):
        self.__dict__['columnstorage'] = BestDictAvailable()
        if client:
            self.Meta.client = client
            self.setManagerFromMeta()
        self.__dict__['possiblekeys'] = set(self.objects.columns.keys())
        self.key = key
        self.update(kwargs)
    
    def activecolumns(self, complain=False, reportbadmisses=False):
        columns = []
        failure = False
        for name, value in self.objects.columns.items():
            if name in self.__dict__['columnstorage'].keys():
                columns.append(name)
            elif not value.required:
                continue
            elif complain:
                raise Exception("Row lacks required column '%s'." % name)
                
        return columns
    
    def save(self):
        if self.key is None:
            if getattr(self.Meta, 'generate_rowkey_if_empty', False):
                self.key = uuid.uuid4().hex
            else:
                raise Exception('Trying to save with undefined row key.')
        
        columns = self.activecolumns(complain=True)
        fakedictproxy = ObjWithFakeDictAndKey(realdict=self.__dict__['columnstorage'], key=self.key)
        self.objects.insert(instance=fakedictproxy, columns=columns)
    
    def combine_columns(self, column_dict, columns):
        combined_columns = BestDictAvailable()
        for column, value in columns.iteritems():
            if column not in column_dict.keys():
                try:
                    raise Exception("Impossible key for combining: %s" % (column,))
                except:
                    self.unhandled_exception_handler()
                continue
            combined_columns[column] = column_dict[column].unpack(value)
        for column, coltype in column_dict.iteritems():
            if column not in combined_columns.keys():
                combined_columns[column] = coltype.default
        return combined_columns
    
    def load(self, column_count=10000):
        rowkey, freshcolumns = (self.objects.column_family.get_range(start=self.key, finish=self.key,
                               column_start='', column_finish='', column_reversed=False, column_count=column_count, row_count=1)).next()
        assert self.key == rowkey, "Received different key %s than what I asked for (%s)" % (rowkey, self.key)
        columns = self.combine_columns(self.objects.columns, freshcolumns)
        self.update(columns)
    
    def setIfPossible(self, work, key, value):
        if key in self.possiblekeys:
            work[key] = value
            return True
        else:
            return False
    
    def update(self, arg=None, **kwargs):
        work = BestDictAvailable()
        
        if not (arg is None):
            if hasattr(arg, 'keys'):
                for key in arg:
                    self.setIfPossible(work, key, arg[key])
            elif 'arg' in possiblekeys:
                setattr(self, 'arg', arg)
            else:
                for key, value in arg:
                    self.setIfPossible(work, key, value)
        
        for key, value in kwargs:
            self.setIfPossible(work, key, value)
        
        self.__dict__['columnstorage'] = work
        columnmap = BestDictAvailable()
        for key in work:
            columnmap[key] = self.__class__.__dict__[key]
        
        for key in self.objects.columns:
            if key not in columnmap:
                columnmap[key] = self.objects.columns[key]
        
        self.objects.columns = columnmap

    def __repr__(self):
        nicerepr = BestDictAvailable((col, self.__dict__['columnstorage'].get(col)) for col in self.activecolumns())
        return '<%s(%s): %s>' % (
                self.__class__.__name__,
                self.key,
                json.dumps(nicerepr )
        )
