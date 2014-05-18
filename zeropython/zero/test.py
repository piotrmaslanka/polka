import time, struct
from zero import ClientInterface, SeriesDefinition

sd = SeriesDefinition('kurwa_was_mac', 2, 1, 0, 4, '', 0)
s = ClientInterface(('10.0.0.100', 20))

s.updateDefinition(sd)

head = s.getHeadTimestamp(sd)

for x in range(0, 1):
    print 'Current HEAD is '+str(head)
    s.writeSeries(sd, head, head+1, struct.pack('>i', head))
    head += 1

#print s.read(sd, 10, 100)

s.close()