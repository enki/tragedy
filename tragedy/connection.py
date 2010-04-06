import sys
import pycassa

client = None
def connect(*args, **kwargs):
    global client
    client = pycassa.connect(*args, **kwargs)
    
    # gatherinfo()

# def gatherinfo(client=None):
#     if not client:
#         client = globals()['client']
#     print client.describe_keyspace('BBQ')