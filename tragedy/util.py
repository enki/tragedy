import sys
import traceback
import pycassa
import threading
import time

def gm_timestamp():
    """int : UNIX epoch time in GMT"""
    return int(time.time() * 1e6)

class CrossModelCache(object):
    def __init__(self):
        self.lock = threading.Lock()
        self.storage = {}
        
    def store(self, key, value):
        with self.lock:
            self.storage[key] = value
    
    def retrieve(self, key):
        with self.lock:
            return self.storage.get(key)
    
    def retrieve_or_exec(self, func, *args, **kwargs):
        signature = (func, tuple(args), tuple(kwargs.items()))
        result = self.retrieve(signature)
        if not result:
            print 'MISS', signature
            result = func(*args, **kwargs)
            self.store(signature, result)
        else:
            print 'HIT', signature
        return result

    def append(self, key, value):
        with self.lock:
            b = self.storage.get(key, [])
            b.append(value)
            self.storage[key] = b

    def drop(self, *args):
        with self.lock:
            try:
                del self.storage[args]
            except:
                pass

def unhandled_exception_handler(reraise=False):
    print '<<<<<<<<<<<<<<<<<<<'
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
    
    if reraise:
        raise
    print '>>>>>>>>>>>>>>>>'

class ObjWithFakeDictAndKey(object):
    __slots__ = ['realdict', 'key']
    
    def __init__(self, realdict, key):
        self.realdict = realdict
        self.key = key
    
    def __getattr__(self, name):
        if name == '__dict__':
            return self.realdict
        elif name == 'name':
            return self.name
        else:
            raise AttributeError('wrong getattr %s' % name)

try:
    import collections
    BestDictAvailable = collections.OrderedDict
except:
    unhandled_exception_handler()
    BestDictAvailable = dict

# def create_instance(cls, **kwargs):
#     instance = cls()
#     instance.update(kwargs)
#     return instance
# 
# pycassa.columnfamilymap.create_instance = create_instance    