import sys
import traceback

def unhandled_exception_handler(reraise=False):
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
    