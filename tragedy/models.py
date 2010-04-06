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
                   CrossModelCache,)
from . import fields
from . import connection

cmcache = CrossModelCache()

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
        
        column_type = getattr(meta, 'column_type', 'Standard')
        compare_with = getattr(meta, 'compare_with', 'BytesType')
        
        column_family = getattr(meta, 'column_family', name)
        client = getattr(meta, 'client', None)
        if not client:
            client = connection.client
        
        setattr(meta, 'keyspace', keyspace)
        setattr(meta, 'column_family', column_family)
        setattr(meta, 'column_type', column_type)
        setattr(meta, 'compare_with', compare_with)
        setattr(meta, 'client', client)
        
        skipverify = getattr(meta, 'skipverify', False)
        setattr(meta, 'skipverify', skipverify)
        
        # Note: No support for supercolumns right now!
        
    def _setManagerFromMeta(cls):
        if cls.Meta.client:
            cls.Meta._cfamily = pycassa.ColumnFamily(cls.Meta.client, cls.Meta.keyspace, cls.Meta.column_family,
                                               dict_class=BestDictAvailable)
            cls.objects = pycassa.ColumnFamilyMap(cls, cls.Meta._cfamily)
            
            cls.verifyDataModel()

class Model(object):
    __metaclass__ = ModelType
        
    unhandled_exception_handler = staticmethod(unhandled_exception_handler)

    @classmethod
    def verifyDataModel(cls):
        if cls.Meta.skipverify:
            return
        allkeyspaces = cmcache.retrieve_or_exec(cls.Meta.client.describe_keyspaces)
        assert cls.Meta.keyspace in allkeyspaces, ("Cassandra doesn't know about " + 
                                    "keyspace %s (only %s)" % (cls.Meta.keyspace, allkeyspaces))
        mykeyspace = cmcache.retrieve_or_exec(cls.Meta.client.describe_keyspace, cls.Meta.keyspace)
        mycf = mykeyspace[cls.Meta.column_family]
        assert cls.Meta.column_type == mycf['Type'], 'Wrong column type (local %s, remote %s)' % (cls.Meta.column_type, mycf['Type'])
        remotecw = mycf['CompareWith'].rsplit('.',1)[1]
        assert cls.Meta.compare_with == remotecw, 'Wrong CompareWith (local %s, remote %s)' % (cls.Meta.compare_with, remotecw)

    def setManagerFromMeta(self):
        """Override binding for instance."""
        self.Meta._cfamily = pycassa.ColumnFamily(self.Meta.client, self.Meta.keyspace, self.Meta.column_family,
                                               dict_class=BestDictAvailable)
        self.objects = pycassa.ColumnFamilyMap(self.__class__, self.Meta._cfamily)

    def __init__(self, *args, **kwargs):
        key = kwargs.pop('key', None)
        client = kwargs.pop('client', None)
        
        self.columnstorage = BestDictAvailable()
        if client:
            self.Meta.client = client
            self.setManagerFromMeta()
        self.possiblekeys = set(self.objects.columns.keys())
        self.key = key
        self.update(args, **kwargs)
    
    def activecolumns(self, complain=False):
        columns = []
        failure = False
        columnmap = self.objects.columns
        for name in self.columnstorage:
            if name in columnmap:
                columns.append(name)
            elif not columnmap[name].required:
                continue
            elif complain:
                raise Exception("Row lacks required column '%s'." % name)
        
        for name, value in columnmap.items():
            if (not name in self.columnstorage.keys()) and value.required and complain:
                raise Exception("Row lacks required column '%s'." % name)
        
        return columns
    
    def save(self):
        if self.key is None:
            if getattr(self.Meta, 'generate_rowkey_if_empty', False):
                self.key = uuid.uuid4().hex
            else:
                raise Exception('Trying to save with undefined row key.')
        
        columns = self.activecolumns(complain=True)
        insert_dict = BestDictAvailable()
        for column in columns:
            insert_dict[column] = self.objects.columns[column].pack(self.columnstorage[column])
        self.objects.column_family.insert(self.key, insert_dict)
    
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
        self.columnstorage = BestDictAvailable()
        try:
            rowkey, freshcolumns = (self.objects.column_family.get_range(start=self.key, finish=self.key,
                                   column_start='', column_finish='', column_reversed=False, column_count=column_count, row_count=1)).next()
        except StopIteration:
            raise Exception("Received zero answers to query.")
        assert self.key == rowkey, "Received different key %s than what I asked for (%s)" % (rowkey, self.key)
        columns = self.combine_columns(self.objects.columns, freshcolumns)
        self.update(columns)
    
    def setIfPossible(self, work, key, value):
        if key in self.possiblekeys:
            work[key] = value
            return True
        else:
            return False
    
    def update(self, __magic_arg=None, **kwargs):
        work = BestDictAvailable()

        if not (__magic_arg is None):
            if hasattr(__magic_arg, 'keys'):
                for key in __magic_arg:
                    self.setIfPossible(work, key, __magic_arg[key])
            elif '__magic_arg' in self.possiblekeys:
                setattr(self, '__magic_arg', __magic_arg)
            else:                
                for key, value in __magic_arg:                    
                    self.setIfPossible(work, key, value)
        
        for key, value in kwargs.items():
            self.setIfPossible(work, key, value)
        
        self.columnstorage = work
        columnmap = BestDictAvailable()
        for key in work:
            columnmap[key] = self.__class__.__dict__[key]
        
        for key in self.objects.columns:
            if key not in columnmap:
                columnmap[key] = self.objects.columns[key]
        
        self.objects.columns = columnmap

    def __repr__(self):
        nicerepr = BestDictAvailable((col, self.columnstorage.get(col)) for col in self.activecolumns())
        return '<%s(%s): %s>' % (
                self.__class__.__name__,
                self.key,
                json.dumps(nicerepr )
        )
