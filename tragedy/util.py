import sys
import traceback
import threading
import time
import simplejson
import cjson
import functools
import datetime
import datetime_safe
CASPATHSEP = ' -> '

class DjangoJSONEncoder(simplejson.JSONEncoder):
    """
    JSONEncoder subclass that knows how to encode date/time and decimal types.
    """

    DATE_FORMAT = "%Y-%m-%d"
    TIME_FORMAT = "%H:%M:%S"

    def default(self, o):
        if isinstance(o, datetime.datetime):
            d = datetime_safe.new_datetime(o)
            return d.strftime("%s %s" % (self.DATE_FORMAT, self.TIME_FORMAT))
        elif isinstance(o, datetime.date):
            d = datetime_safe.new_date(o)
            return d.strftime(self.DATE_FORMAT)
        elif isinstance(o, datetime.time):
            return o.strftime(self.TIME_FORMAT)
        elif isinstance(o, decimal.Decimal):
            return str(o)
        else:
            return super(DjangoJSONEncoder, self).default(o)

# jsondumps = functools.partial(simplejson.dumps, cls=DjangoJSONEncoder)
jsondumps = cjson.encode
jsonloads = cjson.decode

def buchtimer(maxtime=0.2):
    def wrap1(func):
        def wrap2(*args,**kwargs):
            starttime = time.time()
            result = func(*args, **kwargs)
            timediff = time.time() - starttime
            if timediff > maxtime:
                print 'Func %s took %s seconds (maxtime=%s).' % (func, timediff, maxtime)
            return result
        return wrap2
    return wrap1

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