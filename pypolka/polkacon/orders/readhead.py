from polkacon.orders import BaseOrder
from polkacon.exceptions import IOException, SeriesNotFoundException
import struct

class ReadHead(BaseOrder):
    def __init__(self, name, recordsize):
        BaseOrder.__init__(self)
        self.name = name        
        self.recordsize = recordsize

        self.data = []

    def __str__(self):
        return '\x05' + struct.pack('>h', len(self.name)) + self.name
        
    def on_data(self, buffer):        
        # This could be optimized more by postponing del buffer[..] but
        # I'm too lazy to do it
        while len(buffer) >= 8:
            qpsk, = struct.unpack('>q', str(buffer[:8]))
            
            if qpsk > -1:
                if len(buffer) < 8+self.recordsize: return
                self.data.append((qpsk, buffer[:8+self.recordsize]))
                del buffer[:8+self.recordsize]
            elif qpsk == -1:
                self.is_completed = True
                self.data = self.data[0]
                return
            elif qpsk == -2:
                self.result = IOException()
                self.is_completed = True
                del buffer[0:8]
                return
            elif qpsk == -3:
                self.result = SeriesNotFoundException()
                self.is_completed = True
                del buffer[0:8]
                return
            elif qpsk == -4:
                self.result = IllegalArgumentException()
                self.is_completed = True
                del buffer[0:8]
                return
            elif qpsk == -1:
                self.is_completed = True
                self.result = data
                del buffer[0:8]

    def copy(self):
        return ReadHead(self.name, self.recordsize)

