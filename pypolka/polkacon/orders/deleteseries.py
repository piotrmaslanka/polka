from polkacon.orders import BaseOrder
from polkacon.exceptions import IOException, SeriesNotFoundException
import struct

class DeleteSeries(BaseOrder):
    def __init__(self, name):
        BaseOrder.__init__(self)
        self.name = name

    def __str__(self):
        return '\x06' + struct.pack('>h', len(self.name)) + self.name
        
    def on_data(self, buffer):
        self.is_completed = True
        if buffer[0] == 0:
            # Done OK!!
            self.result = True
        elif buffer[0] == 1:
            # Failed
            self.result = IOException()
        elif buffer[0] == 2:
            self.result = SeriesNotFoundException()
        del buffer[0]
        
    def copy(self):
        return DeleteSeries(self.name)        
