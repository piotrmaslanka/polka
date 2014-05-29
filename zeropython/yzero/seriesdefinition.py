import struct

class SeriesDefinition(object):
    """
    A class symbolizing series
    """
    
    def __init__(self, seriesname, replicacount, generation, autotrim, recordsize, options='', tombstonedon=0):
        self._seriesname = seriesname
        self._replicacount = replicacount
        self._generation = generation
        self._autotrim = autotrim
        self._recordsize = recordsize
        self._options = options
        self._tombstonedon = tombstonedon
        
        self._intp = None
        
    @property
    def seriesname(self):
        return self._seriesname
    @seriesname.setter
    def seriesname(self, value):
        self._seriesname = value
        self._intp = None

    @property
    def replicacount(self):
        return self._replicacount
    @replicacount.setter
    def replicacount(self, value):
        self._replicacount = value
        self._intp = None

    @property
    def generation(self):
        return self._generation
    @generation.setter
    def generation(self, value):
        self._generation = int(value)
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
        
    @property
    def tombstonedon(self):
        return self._tombstonedon
    @tombstonedon.setter
    def tombstonedon(self, value):
        self._tombstonedon = long(value)
        self._intp = None        
        
    def __precompileINTP(self):        
        ssernam = self._seriesname.encode('utf8')
        opts = self._options.encode('utf8')
        
        a = struct.pack('>iiqqqh', self._replicacount, self._recordsize, self._generation,
                                         self._autotrim, self._tombstonedon, len(opts))
        b = opts + struct.pack('>h', len(ssernam)) + ssernam
        self._intp = a+b
    
    def toINTP(self):
        if self._intp == None:
            self.__precompileINTP()
        return self._intp
    
    @staticmethod
    def fromINTP(dat):
        """dat may be arbitrary stream, it can contain trailing data after the INTP"""
        repc, recs, genr, autr, tombs, lenopt = struct.unpack('>iiqqqh', dat[:34])
        options = dat[34:34+lenopt].decode('utf8')
        lennam, = struct.unpack('>h', dat[34+lenopt:34+lenopt+2])
        nam = dat[34+lenopt+2:34+lenopt+2+lennam].decode('utf8')
        if len(nam) != lennam: 
            raise Exception
        return SeriesDefinition(nam, repc, genr, autr, recs, options, tombs), 34+lenopt+2+lennam

    