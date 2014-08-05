from zerocon.orders import BaseOrder
from zerocon.exceptions import IOException
from zerocon.seriesdefinition import SeriesDefinition
import struct

class GetDefinition(BaseOrder):
    def __init__(self, name):
        BaseOrder.__init__(self)
        self.name = name

    def __str__(self):
        return '\x00'+struct.pack('>H', len(self.name)) + self.name
        
    def on_data(self, buffer):
        if buffer[0] == 1:
            self.result = IOException()
            del buffer[0]
        elif buffer[0] == 2:
            del buffer[0]       # not found, keep None
        elif buffer[0] == 0:
            # Found something
            try:
                sd = SeriesDefinition.fromINTP(buffer[1:])
            except:
                return
            else:
                del buffer[:1+sd._lengthInBytes()]
                
        self.is_completed = True
