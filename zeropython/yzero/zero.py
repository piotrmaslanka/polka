import itertools
import collections
from yos.io import NetworkSocket
from yzero.orders import Read, Write, GetDefinition, UpdateDefinition, GetHeadTimestamp

class Zero(object):
    """
    Series access
    """
    
    def __init__(self, nodes: tuple):
        """Initializes access. Nodes is a list of nodes to connect to (if one fails,
        system rolls with next one"""
        
        self.nodes = itertools.cycle(nodes)
       
        self.state = 0      # 0: NOT CONNECTED
                            # 1: CONNECTING
                            # 2: CONNECTED
                            # 3: QUERY SENT
        self._curNS = None
        self.orders = collections.deque()
        self.buf = bytearray()
        
    # Handler pool
    def _onConnected(self, sock):
        sock.write(b'\x03')
        self.state = 2
        # Is there something I should know?
        if len(self.orders) > 0:
            self.__execute()
        
    def _onCloseFail(self, sock):
        if self.state == 3:
            # a query was running...
            if not self.orders[-1].failed():
                # We will not attempt a retry..
                self.orders.pop()
                    
        self._curNS = 0
        self.state = 0
        if len(self.orders) > 0: # Is there a reason for me to connect?
            self.__connect()
            
    def _onData(self, sock, data):
        self.buf.extend(data)
        if self.orders[-1].execute(self.buf):
            self.orders.pop()
            self.state = 2
            if len(self.orders) > 0:
                self.__execute()
    
    def __connect(self):
        self._curNS = NetworkSocket.client(NetworkSocket.SOCK_TCP, next(self.nodes))
        self._curNS.register(self._onData, None, self._onConnected, self._onCloseFail, self._onCloseFail)        
        self.state = 1
        
    def __execute(self):
        """Called when state is 2 and a new order can be scheduled"""
        print("Executing ", self.orders[-1])
        self._curNS.write(self.orders[-1].serialize())
        self.state = 3
        
    def updateDefinition(self, defn, callback: callable):
        """callback will be called with a bool whether the operation completed successfully"""
        self.orders.appendleft(UpdateDefinition(defn, callback))
        if self.state == 0: self.__connect()
        if self.state == 2: self.__execute()
        return self
            
    def getDefinition(self, sernam, callback: callable):
        """callback will be called with:
                None if definition was not found
                False if error occurred
                SeriesDefinition instance upon retrieval
        """
        self.orders.appendleft(GetDefinition(sernam, callback))
        if self.state == 0: self.__connect()
        if self.state == 2: self.__execute()
        return self
        
    def getHeadTimestamp(self, sd, callback):
        """callback will be called with a False if operation failed,
        a -1 if series is empty or head timestamp: int"""
        self.orders.appendleft(GetHeadTimestamp(sd, callback))
        if self.state == 0: self.__connect()
        if self.state == 2: self.__execute()
        return self
    
    def writeSeries(self, sd, prev_timestamp, cur_timestamp, data, callback):
        """callback will be called with a bool whether the operation completed successfully"""
        self.orders.appendleft(Write(sd, prev_timestamp, cur_timestamp, data, callback))
        if self.state == 0: self.__connect()
        if self.state == 2: self.__execute()
        return self
            
    def readSeries(self, sd, from_, to, callbackOnData, callbackOnEnd):
        """
        callbackOnData will be called with a list containing tuples: (timestamp, data: bytes)
        callbackOnEnd will be called with a bool, whether the read completed successfully
        """
        self.orders.appendleft(Read(sd, from_, to, callbackOnData, callbackOnEnd))
        if self.state == 0: self.__connect()
        if self.state == 2: self.__execute()
        return self
    