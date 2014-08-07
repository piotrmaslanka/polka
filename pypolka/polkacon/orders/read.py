from polkacon.orders import BaseOrder
from polkacon.exceptions import IOException, SeriesNotFoundException, IllegalArgumentException
import struct

class Read(BaseOrder):
    def __init__(self, name, from_, to, recordsize):
        BaseOrder.__init__(self)
        self.name = name
        self.from_ = from_
        self.to = to
        self.recordsize = recordsize
        
        self.data = None    # if list means header was readed in

    def __str__(self):
        return '\x04' + struct.pack('>h', len(self.name)) + self.name + struct.pack('>qq', self.from_, self.to)
        
    def get_result(self):
        return self.data
        
    def on_data(self, buffer):        
        if self.data == None:            
            if buffer[0] == 0:
                self.data = []
                self.on_data(buffer)
            elif buffer[0] == 1:
                self.result = IOException()
                self.is_completed = True
            elif buffer[0] == 2:
                self.result = SeriesNotFoundException()            
                self.is_completed = True
            elif buffer[0] == 4:
                self.result = IllegalArgumentException()
                self.is_completed = True

            del buffer[0]
        else:
            # This could be optimized more by postponing del buffer[..] but
            # I'm too lazy to do it
            while True:
                if len(buffer) < 8:
                    return  # Not enough bits n bytes
                
                ts, = struct.unpack('>q', buffer[:8])
                
                if ts == -1:
                    # end of read.
                    self.is_completed = True
                    del buffer[:8]
                    return
                else:
                    # record to be read inside!
                    if len(buffer) < 8+self.recordsize:
                        return # not yet ready
                    data = buffer[8:8+self.recordsize]
                    del buffer[:8+self.recordsize]
                    self.data.append((ts, data))

    def copy(self):
        return Read(self.name, self.from_, self.to, self.recordsize)
