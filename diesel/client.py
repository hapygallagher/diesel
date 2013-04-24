# vim:ts=4:sw=4:expandtab
import errno
from diesel import log
import ipdb
import socket

class Client(object):
    '''An agent that connects to an external host and provides an API to
    return data based on a protocol across that host.
    '''
    def __init__(self, addr, port, ssl_ctx=None, timeout=None, source_ip=None):
        self.ssl_ctx = ssl_ctx
        self.connected = False
        self.conn = None
        self.addr = addr
        self.port = port

        log.debug("UDPCLIENT SETUPSOCKET %s %s" % (self.addr, str(self.port)) )

        ip = self._resolve(self.addr)
        self._setup_socket(ip, timeout, source_ip)

    def _resolve(self, addr):
        from resolver import resolve_dns_name
        return resolve_dns_name(addr)

    def _setup_socket(self, ip, timeout, source_ip=None):
        log.debug("UDPCLIENT SETUPSOCKET %s %s" % (ip, str(self.port)) )
        from core import _private_connect
        remote_addr = (ip, self.port)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(0)

        if source_ip:
            sock.bind((source_ip, 0))

        try:
            sock.connect(remote_addr)
        except socket.error, e:
            if e.args[0] == errno.EINPROGRESS:
                _private_connect(self, ip, sock, self.addr, self.port, timeout=timeout)
            else:
                raise

    def on_connect(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args, **kw):
        self.close()

    def close(self):
        '''Close the socket to the remote host.
        '''
        if not self.is_closed:
            self.conn.close()
            self.conn = None
            self.connected = True

    @property
    def is_closed(self):
        return not self.conn or self.conn.closed

class UDPClient(Client):

    def __init__(self, addr, port, source_ip=None):
        super(UDPClient, self).__init__(addr, port, source_ip = source_ip)
        log.debug("UDPCLIENT SETUP %s %s" % (addr, str(port)) )

    def _setup_socket(self, ip, timeout, source_ip=None):
        log.debug("UDPCLIENT SETUPSOCKET %s %s" % (ip, str(self.port)) )
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setblocking(0)

        if source_ip:
            sock.bind((source_ip, 0))

        self.conn = self._internal_create_connection(sock, ip, self.port)
        self.connected = True

    def _internal_create_connection(self, sock, ip, port):
        from core import UDPSocket
        return UDPSocket(self, sock, ip, port)

    def _resolve(self, addr):
        return addr

    class remote_addr(object):
        def __get__(self, inst, other):
            return (inst.addr, inst.port)

        def __set__(self, inst, value):
            inst.addr, inst.port = value
    remote_addr = remote_addr()

class UDPConnectionClient(UDPClient):
    def __init__(self, addr, port, connection_handler):
        self.connection_handler = connection_handler
        super(UDPConnectionClient, self).__init__(addr, port)

    def _create_new_connection(self, parent, sock, ip, port, f_connection_loop, *args, **kw):
        assert(False) #this must be overridden in subclass!
        pass

    def _internal_create_connection(self, sock, ip, port):
        return self._create_new_connection(self, sock, ip, port, self.connection_handler)

