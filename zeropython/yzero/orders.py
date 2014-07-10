import struct
from yzero.seriesdefinition import SeriesDefinition

class UpdateDefinition(object):
    def __init__(self, defn: SeriesDefinition, callback: callable):
        self.defn = defn
        self.callback = callback
                
    def serialize(self) -> bytes:
        return b'\x01' + self.defn.toINTP()
    
    def execute(self, buf: bytearray) -> bool:
        """Returns whether the order was executed"""
        if len(buf) == 0: return False
        k = buf[0]
        del buf[0]
        
        self.callback(k == 0)
        return True
    
    def failed(self) -> bool:
        """Called upon connection failing hard. Return whether it should be retried"""
        return True
        
class GetDefinition(object):
    def __init__(self, name: str, callback: callable):
        self.name = name
        self.callback = callback
        
    def serialize(self):
        nam = self.name.encode('utf8')
        return struct.pack('>BH', 0, len(nam)) + nam
    
    def failed(self):
        return True    
    
    def execute(self, buf): 
        if len(buf) == 0: return False
        
        statkod = buf[0]
        if statkod == 1:    # IOException
            self.callback(False)
            del buf[0]
            return True
        elif statkod == 2:   # not found
            self.callback(None)
            del buf[0]
            return True
        else:           # found, relaying
            try:
                defn, len_ = SeriesDefinition.fromINTP(buf[1:])
            except:
                return False

            del buf[:len_+1]
            self.callback(defn)
            return True
        
        
class GetHeadTimestamp(object):
    def __init__(self, defn: SeriesDefinition, callback: callable):
        self.defn = defn
        self.callback = callback
        
    def serialize(self) -> bytes:
        return b'\x02' + self.defn.toINTP()
        
    def failed(self):
        return True            
        
    def execute(self, buf) -> bool:
        if len(buf) == 0: return False
        
        result = buf[0]
        if result > 0: 
            del buf[0]
            self.callback(False)
            return True
        
        if len(buf) < 9: return False
        statkod, head = struct.unpack('>bq', buf[:9])
        print("Head timestamp for", self.defn.seriesname, "is", head)
        del buf[:9]
        self.callback(head)
        return True
        
class Write(object):
    def __init__(self, defn: SeriesDefinition, prev_ts: int, ts: int, data: bytes, callback: callable):
        self.defn = defn
        self.prev_ts = prev_ts
        self.ts = ts
        self.data = data
        self.callback = callback
        
    def serialize(self):
        return b'\x03' + self.defn.toINTP() + struct.pack('>qqi', self.prev_ts, self.ts, len(self.data)) + self.data

    def failed(self):
        return True        
    
    def execute(self, buf):
        if len(buf) == 0: return False
        
        statkod = buf[0]
        print("Write executed with code", statkod)
        del buf[0]
        self.callback(statkod == 0)
        return True
    
class Read(object):
    def __init__(self, defn: SeriesDefinition, from_, to, onData: callable, onEnd: callable):
        self.defn = defn
        self.from_ = from_
        self.to = to
        self.onData = onData
        self.onEnd = onEnd
        
        self.codeReadedIn = False
        self.anythingCalledYet = False  
            # if no onData() was called yet, we can safely retry it on failed

    def failed(self):
        if self.anythingCalledYet:
            self.onEnd(False)
            return False
        else:
            return True
        
    def serialize(self):
        return b'\x04' + self.defn.toINTP() + struct.pack('>qq', self.from_, self.to)
            
    def execute(self, buf):
        """This can return False with erasing parts of buffer"""
        if not self.codeReadedIn:
            if len(buf) == 0: return False
            statkod = buf[0]
            del buf[0]
            if statkod != 0:
                self.onEnd(False)
                return True
            self.codeReadedIn = True
            return self.execute(buf)
        else:
            legit_data = []
            while True:
                if len(buf) < 8:
                    break
                timestamp, = struct.unpack('>q', buf[:8])
                if timestamp != -1:
                    # this is legitimate data
                    if len(buf) < (8+self.defn.recordsize):
                        break
                    legit_data.append((timestamp, buf[8:8+self.defn.recordsize]))
                    print("Readed row in", timestamp, buf[8:8+self.defn.recordsize])
                    del buf[:8+self.defn.recordsize]
                else:
                    del buf[:8]
                    # end of data
                    if len(legit_data) > 0:
                        self.onData(legit_data)
                        self.onEnd(True)
                        print("Minus jeden readed, finishing")
                        return True
            # returning FALSE
            if len(legit_data) > 0:
                self.anythingCalledYet = True
                self.onData(legit_data)
            return False