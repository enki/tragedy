# This file contains ugly/unfinished hacks that almost certainly noone should ever use.
import sys
import os
import time
from .util import unhandled_exception_handler

want_boot = False

try: # this isn't intended for use by anyone yet.
    from tragedyconf import *
except:
    pass

started = False

def boot(keyspace=None):
    global started
    if started or not want_boot:
        return
    started = True
    if not keyspace:
        import hierarchy
        keyspace = hierarchy.cmcache.retrieve('keyspaces')[0]
    keyspace.verify_datamodel(fix=True)

keyspaceconf = """
<ReplicaPlacementStrategy>org.apache.cassandra.locator.RackUnawareStrategy</ReplicaPlacementStrategy>
<ReplicationFactor>1</ReplicationFactor>
<EndPointSnitch>org.apache.cassandra.locator.EndPointSnitch</EndPointSnitch>
"""

# def genconfigsnippet(keyspace):
#     print 'OHWOW', keyspace
#     begin = """<Keyspace Name="%s">\n""" % (keyspace.name)
#     middle = '\n'.join([genconfiglinefor(cf) for cf in getattr(keyspace, 'models').values()])
#     end = """</Keyspace>"""
#     total = begin + middle + keyspaceconf + end
#     print total
#     return total
# 
# def genconfiglinefor(cls):
#     mydesc = '<ColumnFamily Name="{name}" CompareWith="{compare_with}"/>'.format(
#                 name=cls._column_family, compare_with=cls._default_field.compare_with )
#     return mydesc
# 
# def replacePlaceholder(configstring):
#     newconfig = open(template, 'r').read().replace('[[[PLACEHOLDER]]]', configstring)
#     open(target, 'w').write(newconfig)

# def startCassandra():
#     os.system(cassandrabin + ' -p ' + pidfile + '>/dev/null 2>&1')
# 
# def stopCassandra():
#     try:
#         pid = int(open(pidfile).read())
#         os.kill(pid, 9)
#         while True:
#             try:
#                 open(pidfile).read()
#                 os.kill(pid, 0)
#                 time.sleep(0.5)
#             except:
#                 break
#     except:
#         unhandled_exception_handler()
# 
# def restartCassandra():
#     stopCassandra()
#     time.sleep(0.2)
#     startCassandra()
#     time.sleep(3)
# 

