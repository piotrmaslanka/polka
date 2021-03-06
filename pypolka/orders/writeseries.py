from pypolka.orders import BaseOrder
from pypolka.exceptions import IOException, SeriesNotFoundException, IllegalArgumentException
import struct

class WriteSeries(BaseOrder):
    def __init__(self, name, cur_timestamp, data):
        BaseOrder.__init__(self)
        self.name = name
        self.cur_timestamp = cur_timestamp
        self.data = data

    def __str__(self):
        return '\x03' + struct.pack('>h', len(self.name)) + self.name + struct.pack('>qi', self.cur_timestamp, len(self.data)) + self.data
        
    def on_data(self, buffer):
        if buffer[0] == 1:
            self.result = IOException()
        elif buffer[0] == 2:
            self.result = SeriesNotFoundException()
        elif buffer[0] == 4:
            self.result = IllegalArgumentException()

        self.is_completed = True
        del buffer[0]

    def copy(self):
        return WriteSeries(self.name, self.cur_timestamp, self.data)     
