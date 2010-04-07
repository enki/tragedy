# Helpers for rapid and dirty local development.
import sys
import os
import time
from .util import unhandled_exception_handler

template = '/Users/enki/Projects/apache-cassandra-0.6.0-beta3/conf/tmpl.storage-conf.xml'
target = '/Users/enki/Projects/apache-cassandra-0.6.0-beta3/conf/storage-conf.xml'
pidfile = '/Users/enki/Projects/apache-cassandra-0.6.0-beta3/cassandra.pid'
cassandrabin = '/Users/enki/Projects/apache-cassandra-0.6.0-beta3/bin/cassandra'

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


def boot(keyspace):
    success = False
    e = None
    try:
        keyspace.verifyModels()
        success = True
    except:
        print 'RETRY'
        replacePlaceholder( keyspace.innerConfig() )
        restartCassandra()
        for i in range(0,10):
            try:
                sleepytime = i*1.2
                time.sleep(sleepytime)
                keyspace.verifyModels()
                success = True
            except Exception, e:
                pass
            if success:
                break

    if success:
        print 'SUCCESSFULLY STARTED'
    else:
        raise e