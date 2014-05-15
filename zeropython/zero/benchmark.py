from __future__ import division
import multiprocessing, time
from zero import ClientInterface, SeriesDefinition

processes = multiprocessing.cpu_count() * 4


def benchmark(index, evt, iq):
    multiprocessing.freeze_support()
    ifc = ClientInterface(('10.0.0.12', 20))
    sd = SeriesDefinition('miley%s' % (index, ), 1, 1, 0, 4, '', 0)
    sd.toINTP()
    ifc.updateDefinition(sd)
    head = ifc.getHeadTimestamp(sd)

    evt.wait()
    
    start = time.time()
    yolo = 'YOLO'
    WRITES = 10000
    for i in xrange(0, WRITES):
        ifc.writeSeries(sd, head, head+1, yolo)
        head += 1
    stop = time.time()
        
    iq.put((WRITES, stop-start))

if __name__ == '__main__':
    multiprocessing.freeze_support()        
    evt = multiprocessing.Event()
    evt.clear()        
    iq = multiprocessing.Queue()
    mps = [multiprocessing.Process(target=benchmark, args=(i, evt, iq)) for i in range(0, processes)]
    
    for p in mps: p.start()

    evt.set()
    
    sumW = 0
    sumT = 0
    for i in xrange(0, processes):
        w, t = iq.get()
        sumW += w
        sumT += t
        
    sumT = sumT / processes
    
    print '%s writes in %s seconds' % (sumW, sumT)

    wpS = sumW / sumT
    
    GOAL = 40000
    
    print '%s%% goal performance' % (wpS / GOAL * 100.0)

        