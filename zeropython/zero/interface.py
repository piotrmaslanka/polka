import struct
from socket import socket, AF_INET, SOCK_STREAM
from zero.seriesdefinition import SeriesDefinition

class IOException(Exception):
    pass
class SeriesNotFoundException(Exception):
    pass
class DefinitionMismatchException(Exception):
    pass
class IllegalArgumentException(Exception):
    pass

class ClientInterface(object):
    
    def __init__(self, addr):
        self.addr = addr
        self.__reconnect()
        
    def __reconnect(self):
        try:
            self.sock.close()
        except:
            pass
        self.sock = socket(AF_INET, SOCK_STREAM)
        self.sock.connect(self.addr)
        self.sock.send('\x03')
        
    def updateDefinition(self, defn):
        self.sock.send('\x01'+defn.toINTP())
        result = ord(self.sock.recv(1))
        if result == 0:
            return
        elif result == 1:
            raise IOException()
        
    def getDefinition(self, sernam):
        self.sock.send('\x00'+struct.pack('>H', len(sernam))+sernam)
        result = ord(self.sock.recv(1))
        if result == 1:
            raise IOException()
        if result == 2:
            return None
        else:
            return SeriesDefinition.fromINTP(self.sock.recv(1024))