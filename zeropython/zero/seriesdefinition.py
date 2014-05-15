import struct

class SeriesDefinition(object):
    
    def __init__(self, seriesname, replicacount, generation, autotrim, recordsize, options, tombstonedon):
        self.seriesname = seriesname
        self.replicacount = replicacount
        self.generation = generation
        self.autotrim = autotrim
        self.recordsize = recordsize
        self.options = options
        self.tombstonedon = tombstonedon
        
    
    def toINTP(self):
        a = struct.pack('>iiqqqh', self.replicacount, self.recordsize, self.generation,
                                         self.autotrim, self.tombstonedon, len(self.options))
        b = self.options + struct.pack('>h', len(self.seriesname)) + self.seriesname
        return a+b
    
    @staticmethod
    def fromINTP(dat):
        repc, recs, genr, autr, tombs, lenopt = struct.unpack('>iiqqqh', dat[:34])
        options = dat[34:34+lenopt]
        lennam, = struct.unpack('>h', dat[34+lenopt:34+lenopt+2])
        nam = dat[34+lenopt+2:34+lenopt+2+lennam]
        if len(nam) != lennam: 
            raise Exception
        return SeriesDefinition(nam, repc, genr, autr, recs, options, tombs)
