# Helpers for rapid and dirty local development.
import sys
import os
import time
from .util import unhandled_exception_handler

template = '/Users/enki/Projects/apache-cassandra-0.6.0-beta3/conf/tmpl.storage-conf.xml'
target = '/Users/enki/Projects/apache-cassandra-0.6.0-beta3/conf/storage-conf.xml'
pidfile = '/Users/enki/Projects/apache-cassandra-0.6.0-beta3/cassandra.pid'
cassandrabin = '/Users/enki/Projects/apache-cassandra-0.6.0-beta3/bin/cassandra'

class InventoryType(type):
    def __new__(cls, name, bases, attrs):
        parents = [b for b in bases if isinstance(b, InventoryType)]
        new_cls = super(InventoryType, cls).__new__(cls, name, bases, attrs)
        
        if (not parents) or (not hasattr(new_cls, 'Meta')):
            # This is the model with the defined metaclass itself, not its child
            return new_cls
                
        # Register us!
        new_cls.Meta.keyspace.registerRowClass(name, new_cls)
        
        return new_cls

def boot(keyspace):
    success = False
    e = None
    try:
        verifyAll(keyspace)
        success = True
    except:
        print 'RETRY'
        replacePlaceholder( genconfigsnippet(keyspace) )
        restartCassandra()
        for i in range(0,10):
            try:
                sleepytime = i*1.2
                time.sleep(sleepytime)
                verifyAll(keyspace)
                success = True
            except Exception, e:
                pass
            if success:
                break

    if success:
        print 'SUCCESSFULLY STARTED'
    else:
        raise e

def verifyDataModel(cls):
    allkeyspaces = cls.Meta.client.describe_keyspaces()
    assert cls.Meta.keyspace.name in allkeyspaces, ("Cassandra doesn't know about " + 
                                "keyspace %s (only %s)" % (cls.Meta.keyspace, allkeyspaces))
    mykeyspace = cls.Meta.client.describe_keyspace(cls.Meta.keyspace.name)
    assert cls.Meta.column_family in mykeyspace.keys(), "CF {0} doesn't exist on server.".format(cls.Meta.column_family)
    mycf = mykeyspace[cls.Meta.column_family]
    assert cls.Meta.column_type == mycf['Type'], 'Wrong column type (local %s, remote %s)' % (cls.Meta.column_type, mycf['Type'])
    remotecw = mycf['CompareWith'].rsplit('.',1)[1]
    assert cls.Meta.compare_with == remotecw, 'Wrong CompareWith (local %s, remote %s)' % (cls.Meta.compare_with, remotecw)

def verifyAll(keyspace):
    for cf in getattr(keyspace, 'rowclasses').values():
        verifyDataModel(cf)

def genconfigsnippet(keyspace):
    return '\n'.join([genconfiglinefor(cf) for cf in getattr(keyspace, 'rowclasses').values()])

def genconfiglinefor(cls):
    return '<ColumnFamily Name="{name}" CompareWith="{compare_with}"/>'.format(
                name=cls.Meta.column_family, compare_with=cls.Meta.compare_with )

def replacePlaceholder(configstring):
    newconfig = open(template, 'r').read().replace('[[[PLACEHOLDER]]]', configstring)
    open(target, 'w').write(newconfig)

def startCassandra():
    os.system(cassandrabin + ' -p ' + pidfile + '>/dev/null 2>&1')

def stopCassandra():
    try:
        pid = int(open(pidfile).read())
        os.kill(pid, 9)
        while True:
            try:
                open(pidfile).read()
                os.kill(pid, 0)
                time.sleep(0.5)
            except:
                break
    except:
        unhandled_exception_handler()

def restartCassandra():
    stopCassandra()
    time.sleep(0.2)
    startCassandra()
    time.sleep(3)


