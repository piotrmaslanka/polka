import struct
from pypolka.orders import BaseOrder
from pypolka.exceptions import IOException, SeriesNotFoundException, IllegalArgumentException

class Read(BaseOrder):
    def __init__(self, name, from_, to, recordsize):
        BaseOrder.__init__(self)
        self.name = name
        self.from_ = from_
        self.to = to
        self.recordsize = recordsize
        
        self.data = []    # if list means header was readed in

    def __str__(self):
        return '\x04' + struct.pack('>h', len(self.name)) + self.name + struct.pack('>qq', self.from_, self.to)
        
    def get_result(self):
        return self.data
        
    def on_data(self, buffer):        
        # This could be optimized more by postponing del buffer[..] but
        # I'm too lazy to do it
        while len(buffer) >= 8:
            qpsk, = struct.unpack('>q', str(buffer[:8]))
            if qpsk > -1:
                if len(buffer) < 8+self.recordsize: return
                self.data.append((qpsk, buffer[8:8+self.recordsize]))
                del buffer[:8+self.recordsize]
            elif qpsk == -1:
                self.is_completed = True
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
        return Read(self.name, self.from_, self.to, self.recordsize)
