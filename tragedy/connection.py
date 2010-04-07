import sys
import pycassa

client = None
def connect(*args, **kwargs):
    global client
    client = pycassa.connect(*args, **kwargs)
    return client