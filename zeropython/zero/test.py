import time

from zero import ClientInterface, SeriesDefinition
sd1 = SeriesDefinition('miley', 1, 1, 0, 4, '', 0)
sd2 = SeriesDefinition('bench', 1, 1, 0, 4, '', 0)
sd3 = SeriesDefinition('pik', 1, 1, 0, 4, '', 0)
sd4 = SeriesDefinition('pik2', 1, 1, 0, 4, '', 0)
s = ClientInterface(('10.0.0.12', 20))

s.updateDefinition(sd1)
head = s.getHeadTimestamp(sd1)



start = time.time()
for x in range(0, 1430):
    s.writeSeries(sd1, head, head+1, 'DUPA')
    head += 1
stop = time.time()