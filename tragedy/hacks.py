# Helpers for rapid and dirty local development.
import sys
import os
import time
from .util import unhandled_exception_handler

template = '/Users/enki/Projects/apache-cassandra-0.6.0-beta3/conf/tmpl.storage-conf.xml'
target = '/Users/enki/Projects/apache-cassandra-0.6.0-beta3/conf/storage-conf.xml'
pidfile = '/Users/enki/Projects/apache-cassandra-0.6.0-beta3/cassandra.pid'
cassandrabin = '/Users/enki/Projects/apache-cassandra-0.6.0-beta3/bin/cassandra'

def boot(keyspace):
    success = False
    e = None
    try:
        verifyAll(keyspace)
        success = True
    except:
        unhandled_exception_handler()
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
                unhandled_exception_handler()
            if success:
                break

    if success:
        print 'SUCCESSFULLY STARTED'
    else:
        raise e

def verifyDataModel(cls):
    allkeyspaces = cls._client.describe_keyspaces()
    assert cls._keyspace.name in allkeyspaces, ("Cassandra doesn't know about " + 
                                "keyspace %s (only %s)" % (cls._keyspace, allkeyspaces))
    mykeyspace = cls._client.describe_keyspace(cls._keyspace.name)
    assert cls._column_family in mykeyspace.keys(), "CF {0} doesn't exist on server.".format(cls._column_family)
    mycf = mykeyspace[cls._column_family]
    assert cls._column_type == mycf['Type'], 'Wrong column type (local %s, remote %s)' % (cls._column_type, mycf['Type'])
    remotecw = mycf['CompareWith'].rsplit('.',1)[1]
    assert cls._compare_with == remotecw, 'Wrong CompareWith (local %s, remote %s)' % (cls._compare_with, remotecw)

def verifyAll(keyspace):
    for cf in getattr(keyspace, 'rowclasses').values():
        verifyDataModel(cf)

def genconfigsnippet(keyspace):
    return '\n'.join([genconfiglinefor(cf) for cf in getattr(keyspace, 'rowclasses').values()])

def genconfiglinefor(cls):
    return '<ColumnFamily Name="{name}" CompareWith="{compare_with}"/>'.format(
                name=cls._column_family, compare_with=cls._compare_with )

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


