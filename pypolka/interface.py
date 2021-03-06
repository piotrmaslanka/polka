import itertools, socket, collections
from pypolka.orders.getdefinition import GetDefinition
from pypolka.orders.getheadtimestamp import GetHeadTimestamp
from pypolka.orders.updatedefinition import UpdateDefinition
from pypolka.orders.writeseries import WriteSeries
from pypolka.orders.read import Read
from pypolka.orders.readhead import ReadHead
from pypolka.orders.deleteseries import DeleteSeries

class PolkaInterface(object):
    def __init__(self, address, autoexecute=True):
        """
        Initializes a polka interface
        
        @param address: TCP socket address in form (host, port)
            
        @param autoexecute: if execute() needs to be called to process queries
        """
        
        self.address = address
        self.sock = None
        self.autoexecute = autoexecute
        self.orders = collections.deque()
        self.buffer = bytearray()       # buffer for received data
        
    def close(self):
        """Call when everything is read!"""
        try:
            self.sock.close()
        except:
            pass
        
    def __on_dc(self):
        """I was just disconnected!!!"""
        
        if len(self.orders) == 0:
            try:
                self.sock.close()
            except:
                pass
            self.sock = None
            # so what? we don't need to reconnect right now..
            return
        
        while True:            
            self.__reconnect()
            
            newords = collections.deque()
            for order in self.orders:
                newords.append(order.copy())
            self.orders = newords
            
            try:
                for ord in self.orders:
                    self.sock.send(str(ord))
            except IOError as e:
                continue
            else:
                break
        
    def __reconnect(self):
        while True:
            self.buffer = bytearray()
            try:
                self.sock.close()
            except:
                pass
            
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.settimeout(10)
                sock.connect(self.address)
            except IOError:
                self.sock = None
                continue
            else:
                self.sock = sock
                break

    def get(self):
        """Block until leftmost order is complete, returning it's result
        or throwing a suitable exception"""
        
        if len(self.orders) == 0:
            raise IndexError('Cannot get() if there are no orders pending')
        
        while True:
            otx = self.orders[0]
                    
            try:
                data = self.sock.recv(1024)
                if len(data) == 0:
                    self.__on_dc()
                    continue
            except IOError:
                self.__on_dc()
                continue
            
            self.buffer.extend(data)
            otx.on_data(self.buffer)

            if otx.is_completed:
                self.orders.popleft()
                return otx.get_result()
            # and if not completed, while will make another pass...                
                
            
    def _addOrder(self, orderObject):
        if self.sock == None:
            self.__reconnect()

        self.orders.append(orderObject)

        try:
            self.sock.send(str(orderObject))
        except socket.error:
            self.__on_dc()
        
        if self.autoexecute:
            return self.get()                
        
    def execute(self):
        """Execute enqueued operations, returning a sequence of results"""
        results = []
        while len(self.orders) > 0:
            results.append(self.get())
        return results
        
    # Helper operations
    
    def delete_series(self, name):
        return self._addOrder(DeleteSeries(name))
    
    def write_series(self, name, cur_timestamp, data):
        return self._addOrder(WriteSeries(name, cur_timestamp, data))
              
    def read(self, name, from_, to, recordsize):
        return self._addOrder(Read(name, from_, to, recordsize))
        
    def read_head(self, name, recordsize):
        return self._addOrder(ReadHead(name, recordsize))
        
    def get_definition(self, name):
        return self._addOrder(GetDefinition(name))
        
    def get_head_timestamp(self, name):
        return self._addOrder(GetHeadTimestamp(name))
        
    def update_definition(self, sd):
        return self._addOrder(UpdateDefinition(sd))