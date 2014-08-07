from polkacon.orders import BaseOrder
from polkacon.exceptions import IOException, SeriesNotFoundException, DefinitionMismatchException
import struct

class ReadHead(BaseOrder):
    def __init__(self, name, recordsize):
        BaseOrder.__init__(self)
        self.name = name        
        self.recordsize = recordsize

    def __str__(self):
        return '\x05' + struct.pack('>h', len(self.name)) + self.name
        
    def on_data(self, buffer):
        if buffer[0] == 0:
            # Done OK!!
            if len(buffer) < 9:
                return
            
            ts, = struct.unpack('>q', buffer[1:9])
            if ts == -1:
                # Head is not present. Leave None for result
                del buffer[1:9]
            else:
                if len(buffer) < 9 + self.recordsize:
                    return
                else:
                    self.result = ts, buffer[9:9+self.recordsize]
                    
        elif buffer[0] == 1:
            self.result = IOException()
            del buffer[0]
        elif buffer[0] == 2:
            self.result = SeriesNotFoundException()            
            del buffer[0]
        elif buffer[0] == 3:
            self.result = DefinitionMismatchException()
            del buffer[0]

        self.is_completed = True

    def copy(self):
        return ReadHead(self.name, self.recordsize)

