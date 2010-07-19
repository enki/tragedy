# Based on pycassa http://github.com/vomjom/pycassa/
# Copyright (c) 2009 Jonathan Hseu <vomjom@vomjom.net>
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify, merge,
# publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons
# to whom the Software is furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all copies or
# substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
# PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE
# FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

import socket
import time
import threading
from Queue import Queue

import pkg_resources
pkg_resources.require('Thrift')
from thrift import Thrift
from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from cassandra import Cassandra
from cassandra.ttypes import InvalidRequestException

from .util import unhandled_exception_handler
from .exceptions import NoServerAvailable

__all__ = ['connect', 'connect_thread_local', 'NoServerAvailable']

DEFAULT_SERVER = 'localhost:9160'

def create_client_transport(server, framed_transport, timeout):
    host, port = server.split(":")
    socket = TSocket.TSocket(host, int(port))
    if timeout is not None:
        socket.setTimeout(timeout*1000.0)
    if framed_transport:
        transport = TTransport.TFramedTransport(socket)
    else:
        transport = TTransport.TBufferedTransport(socket)
    protocol = TBinaryProtocol.TBinaryProtocolAccelerated(transport)
    client = Cassandra.Client(protocol)
    transport.open()

    return client, transport

def connect(servers=None, framed_transport=False, timeout=None):
    """
    Constructs a single Cassandra connection. Initially connects to the first
    server on the list.
    
    If the connection fails, it will attempt to connect to each server on the
    list in turn until one succeeds. If it is unable to find an active server,
    it will throw a NoServerAvailable exception.

    Parameters
    ----------
    servers : [server]
              List of Cassandra servers with format: "hostname:port"

              Default: ['localhost:9160']
    framed_transport: bool
              If True, use a TFramedTransport instead of a TBufferedTransport
    timeout: float
              Timeout in seconds (e.g. 0.5)

              Default: None (it will stall forever)

    Returns
    -------
    Cassandra client
    """

    if servers is None:
        servers = [DEFAULT_SERVER]
    client = SingleConnection(servers, framed_transport, timeout)
    return client

def connect_thread_local(servers=None, round_robin=True, framed_transport=False, timeout=None):
    """
    Constructs a Cassandra connection for each thread. By default, it attempts
    to connect in a round_robin (load-balancing) fashion. Turn it off by
    setting round_robin=False

    If the connection fails, it will attempt to connect to each server on the
    list in turn until one succeeds. If it is unable to find an active server,
    it will throw a NoServerAvailable exception.

    Parameters
    ----------
    servers : [server]
              List of Cassandra servers with format: "hostname:port"

              Default: ['localhost:9160']
    round_robin : bool
              Balance the connections. Set to False to connect to each server
              in turn.
    framed_transport: bool
              If True, use a TFramedTransport instead of a TBufferedTransport
    timeout: float
              Timeout in seconds (e.g. 0.5 for half a second)

              Default: None (it will stall forever)

    Returns
    -------
    Cassandra client
    """

    if servers is None:
        servers = [DEFAULT_SERVER]
    return ThreadLocalConnection(servers, round_robin, framed_transport, timeout)

class SingleConnection(object):
    def __init__(self, servers, framed_transport, timeout):
        self._servers = servers
        self._client = None
        self._framed_transport = framed_transport
        self._timeout = timeout
        self._keyspace_set = None

    def set_keyspace(self, keyspace):
        self._keyspace_set = keyspace
        if self._client and not self._client.__dict__.get('keyspace_already_set'):
            self.__getattr__('set_keyspace')(keyspace)

    def __getattr__(self, attr):
        def client_call(*args, **kwargs):
            if self._client is None:
                self._find_server()
            try:
                timer = 0.1
                def trycall():
                    try:
                        return getattr(self._client, attr)(*args, **kwargs)
                    except InvalidRequestException:
                        self.__getattr__('set_keyspace')(self._keyspace_set)
                        time.sleep(timer)
                        if timer < 15:
                            timer *= 2
                        else: 
                            raise NoServerAvailable()
                        return trycall()
                return trycall()    
            except (Thrift.TException, socket.timeout, socket.error), exc:
                unhandled_exception_handler()
                # Connection error, try to connect to all the servers
                self._transport.close()
                self._client = None

                for server in self._servers:
                    try:
                        self._client, self._transport = create_client_transport(server, self._framed_transport, self._timeout)
                        return getattr(self._client, attr)(*args, **kwargs)
                    except (Thrift.TException, socket.timeout, socket.error), exc:
                        unhandled_exception_handler()
                        continue
                self._client = None
                raise NoServerAvailable()
            except:
                unhandled_exception_handler()
                raise

        setattr(self, attr, client_call)
        return getattr(self, attr)

    def _find_server(self):
        for server in self._servers:
            try:
                self._client, self._transport = create_client_transport(server, self._framed_transport, self._timeout)
                if self._keyspace_set:
                    self.__getattr__('set_keyspace')(self._keyspace_set)
                    self._client.__dict__['keyspace_already_set'] = True
                return
            except (Thrift.TException, socket.timeout, socket.error), exc:
                unhandled_exception_handler()
                continue
        self._client = None
        raise NoServerAvailable()

class ThreadLocalConnection(object):
    def __init__(self, servers, round_robin, framed_transport, timeout):
        self._servers = servers
        self._queue = Queue()
        for i in xrange(len(servers)):
            self._queue.put(i)
        self._local = threading.local()
        self._round_robin = round_robin
        self._framed_transport = framed_transport
        self._timeout = timeout
        self._keyspace_set = None

    def set_keyspace(self, keyspace):
        self._keyspace_set = keyspace
        if self._local_client and not self._local_client.__dict__.get('keyspace_already_set'):
            self.__getattr__('set_keyspace')(keyspace)

    def __getattr__(self, attr):
        def client_call(*args, **kwargs):
            if getattr(self._local, 'client', None) is None:
                self._find_server()

            try:
                return getattr(self._local.client, attr)(*args, **kwargs)
            except (Thrift.TException, socket.timeout, socket.error), exc:
                # Connection error, try to connect to all the servers
                self._local.transport.close()
                self._local.client = None

                servers = self._round_robin_servers()

                for server in servers:
                    try:
                        self._local.client, self._local.transport = create_client_transport(server, self._framed_transport, self._timeout)
                        return getattr(self._local.client, attr)(*args, **kwargs)
                    except (Thrift.TException, socket.timeout, socket.error), exc:
                        continue
                self._local.client = None
                raise NoServerAvailable()

        setattr(self, attr, client_call)
        return getattr(self, attr)

    def _round_robin_servers(self):
        servers = self._servers
        if self._round_robin:
            i = self._queue.get()
            self._queue.put(i)
            servers = servers[i:]+servers[:i]

        return servers

    def _find_server(self):
        servers = self._round_robin_servers()

        for server in servers:
            try:
                self._local.client, self._local.transport = create_client_transport(server, self._framed_transport, self._timeout)
                if self._keyspace_set:
                    self._local_client.__getattr__('set_keyspace')(self._keyspace_set)
                    self._local_client.__dict__['keyspace_already_set'] = True
                return
            except (Thrift.TException, socket.timeout, socket.error), exc:
                continue
        self._local.client = None
        raise NoServerAvailable()
