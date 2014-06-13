import time, struct
from zero import ClientInterface, SeriesDefinition

sds = [SeriesDefinition('kurwa_was_mac%s' % (x, ), 2, 1, 0, 4, '', 0) for x in xrange(0, 20)]

s = ClientInterface(('192.168.224.100', 20))

for sd in sds: s.updateDefinition(sd)

for sd in sds:

    head = s.getHeadTimestamp(sd)
    
    for x in range(0, 1):
        print 'Current HEAD is '+str(head)
        s.writeSeries(sd, head, head+1, struct.pack('>i', head))
        head += 1

s.close()