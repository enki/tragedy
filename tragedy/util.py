import sys
import traceback
import threading
import time

CASPATHSEP = ' -> '
    
def popntup(d, key, *args, **kwargs):
    return (key, d.pop(key, *args, **kwargs))

def popmulti(d, *args):
    return dict( [popntup(d, arg[0], arg[1]) for arg in args] )

def warn(msg):
    print 'WARN:', msg

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
    tb = sys.exc_info()[2]
    stack = []

    while tb:
        # stack.append(tb.tb_frame) # too verbose!
        stack = [tb.tb_frame]
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