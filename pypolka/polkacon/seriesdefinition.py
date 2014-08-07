import struct

class SeriesDefinition(object):
    
    def __init__(self, seriesname, replicacount, generation, autotrim, recordsize, options, tombstonedon):
        self._seriesname = seriesname
        self._autotrim = autotrim
        self._recordsize = recordsize
        self._options = options
        
        self._intp = None
        
    @property
    def seriesname(self):
        return self._seriesname
    @seriesname.setter
    def seriesname(self, value):
        self._seriesname = value
        self._intp = None

    @property
    def recordsize(self):
        return self._recordsize
    @recordsize.setter
    def recordsize(self, value):
        self._recordsize = int(value)
        self._intp = None        
        
    @property
    def options(self):
        return self._options
    @options.setter
    def options(self, value):
        self._options = value
        self._intp = None        
        
    @property
    def autotrim(self):
        return self._autotrim
    @autotrim.setter
    def autotrim(self, value):
        self._autotrim = value
        self._intp = None        
        
    def __precompileINTP(self):
        a = struct.pack('>iqh', self._recordsize, self._autotrim, len(self._options))
        b = self._options + struct.pack('>h', len(self._seriesname)) + self._seriesname
        self._intp = a+b
    
    def toINTP(self):
        if self._intp == None:
            self.__precompileINTP()
        return self._intp

    def _lengthInBytes(self):
        return len(self.toINTP())
    
    @staticmethod
    def fromINTP(dat):
        recs, autr, lenopt = struct.unpack('>iqh', dat[:34])
        options = dat[34:34+lenopt]
        lennam, = struct.unpack('>h', dat[34+lenopt:34+lenopt+2])
        nam = dat[34+lenopt+2:34+lenopt+2+lennam]
        if len(nam) != lennam: 
            raise Exception
        return SeriesDefinition(nam, autr, recs, options)

    