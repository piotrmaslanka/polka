import time, struct, threading
from zero import ClientInterface, SeriesDefinition

sds = [SeriesDefinition('kurwa_was_mac%s' % (x, ), 2, 1, 0, 4, '', 0) for x in xrange(0, 50)]
s = ClientInterface(('192.168.224.252', 20))
for sd in sds: s.updateDefinition(sd)


start = time.time()

for x in sds:
	s.readHead(x)
	
print 'Took %s seconds for 50 entries' % (time.time()-start, )



s.close()

class Dupa(threading.Thread):
    def __init__(self, x):
        threading.Thread.__init__(self)
        self.sd = x
        
    def run(self):
        s = ClientInterface(('192.168.224.252', 20))
    
        head = s.readHead(self.sd)
        if head == None:
        	print 'Head was null'
        	head = -1
        else:
        	head = head[0]
        	print 'Head is '+str(head)
        while True:
            s.writeSeries(self.sd, head, head+1, struct.pack('>i', head))
            head += 1


for x in sds:
    Dupa(x).start()