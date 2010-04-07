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