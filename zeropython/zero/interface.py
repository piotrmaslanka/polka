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
        
    def close(self):
        self.sock.close()
        
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
        
    def getHeadTimestamp(self, sd):
        self.sock.send('\x02'+sd.toINTP())
        result = ord(self.sock.recv(1))
        if result == 1:
            raise IOException()
        elif result == 2:
            raise SeriesNotFoundException()
        elif result == 3:
            raise DefinitionMismatchException() 
        else:
            return struct.unpack('>q', self.sock.recv(8))[0]
    
    def writeSeries(self, sd, prev_timestamp, cur_timestamp, data):
        self.sock.send('\x03'+sd.toINTP()+struct.pack('>qqi', prev_timestamp, cur_timestamp, len(data))+data)
        result = ord(self.sock.recv(1))
        if result == 1:
            raise IOException()
        elif result == 2:
            raise SeriesNotFoundException()
        elif result == 3:
            raise DefinitionMismatchException() 
        elif result == 4:
            raise IllegalArgumentException()

    def read(self, sd, from_, to):
        self.sock.send('\x04'+sd.toINTP()+struct.pack('>qq', from_, to))
        result = ord(self.sock.recv(1))
        if result == 1:
            raise IOException()
        elif result == 2:
            raise SeriesNotFoundException()
        elif result == 3:
            raise DefinitionMismatchException() 
        elif result == 4:
            raise IllegalArgumentException()
        
        print 'result was %s' % (result, )

        ts, = struct.unpack('>q', self.sock.recv(8))
        dat = []
        while ts != -1:
            data = self.sock.recv(sd.recordsize)
            dat.append((ts, data))

            ts, = struct.unpack('>q', self.sock.recv(8))
            
        return dat            
