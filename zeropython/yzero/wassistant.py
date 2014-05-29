import collections

class WriteAssistant(object):
    """
    A helper that aids the user with writing series
    """
    
    def __init__(self, seriesName: str, zero):
        """
        Initializes the assistant with given series name, and Zero executor
        """
        
        self.seriesName = seriesName
        self.zero = zero
        self.orders = collections.deque()
        self.is_write_going_on = False
        self.is_ready = False
        
    def write(self, timestamp, data):
        """
        Schedules data to be written with given timestamp
        """
        self.orders.appendleft((timestamp, data))
        if self.is_ready:
            self.__schwrite()
    
    def __schwrite(self):
        """Schedule a write for execution"""
        
        def onWriteCompleted(status):
            if status:
                # it's a-OK!
                self.head = self.orders[-1][0]
                self.orders.pop()
            
            self.is_write_going_on = False
            self.__schwrite()
            
        if len(self.orders) > 0:
            if not self.is_write_going_on:
                self.zero.writeSeries(self.sd, self.head, self.orders[-1][0], self.orders[-1][1], onWriteCompleted)     
        
    def start(self, onReady: callable=lambda x: None):
        """Schedules to acquire required data for leading writes.
        Calls onReady when it's done - with a bool on whether it succeeded"""
    
        def onGotHead(head):
            if head == False:
                onReady(False)
            else:
                self.head = head
                onReady(True)
                self.is_ready = True
                self.__schwrite()
    
        def onGotDefinition(defstat):
            if defstat in (False, None):
                onReady(False)
            else:
                self.sd = defstat
                self.zero.getHeadTimestamp(self.sd, onGotHead)
    
    
        self.zero.getDefinition(self.seriesName, onGotDefinition)
        
        return self
                                
                                
    
        
        