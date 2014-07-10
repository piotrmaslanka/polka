import time, struct, threading, random
from zero import ClientInterface, SeriesDefinition

sds = [SeriesDefinition('kurwa_was_mac%s' % (x, ), 1, 1, 60000, 4, 'slabsize=10922', 0) for x in xrange(0, 200)]
s = ClientInterface(('10.0.0.103', 20))
for sd in sds: s.updateDefinition(sd)

s.close()

class Dupa(threading.Thread):
    def __init__(self, x):
        threading.Thread.__init__(self)
        self.sd = x
        
    def run(self):
        s = ClientInterface(('10.0.0.103', 20))
    
        head = s.readHead(self.sd)
        if head == None:
        	print 'Head was null'
        	head = -1
        else:
        	head = head[0]
        while True:
            k = int(time.time()*100)
            if k == head:
                time.sleep(0.01)
                continue
            s.writeSeries(self.sd, head, k, struct.pack('>i', head/100))
            head = k


class DupaRd(threading.Thread):
    def __init__(self, definitions):
        threading.Thread.__init__(self)
        self.defs = definitions
		
    def run(self):
        s = ClientInterface(('10.0.0.103', 20))
		
        while True:
            time.sleep(random.randint(5, 10))
            sd = random.choice(self.defs)
            k = int(time.time()*100)
            f = s.read(sd, k-random.randint(3000, 15000), k)
            print("Read completed with %s rows\n" % (len(f), ))

for x in sds:
    Dupa(x).start()
 
for a in xrange(0, 0):
	DupaRd(sds).start()