import sys
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
import redis
from redis import Redis

redis = redis.Redis(host='localhost',port=8000, charset="utf-8", decode_responses=True)

class MyFunctions:
 
    def __init__(self,workersURL=[]):
        self.workersURL = workersURL

    def workers(self):
        self.workersURL = redis.hvals("WorkerURL")
        return self.workersURL

    def readAPI(self):
        allItems = []
        for x in MyFunctions.workers(self):
            serverWorker = ServerProxy(x,allow_none=True)
            allItems.append(serverWorker.readCSV())  
        return (allItems)

    def applyAPI(self,newColumn,column,function):
        allItems = []
        for x in MyFunctions.workers(self):
            serverWorker = ServerProxy(x,allow_none=True)
            allItems.append(serverWorker.applyCSV(newColumn,column,function))
        return (allItems)

    def columnsAPI(self):
        allItems = []
        for x in MyFunctions.workers(self):
            serverWorker = ServerProxy(x,allow_none=True)
            allItems.append(str(serverWorker.columnsCSV()))
        return (allItems)
    
    def groupByAPI(self,column):
        allItems = []
        for x in MyFunctions.workers(self):
            serverWorker = ServerProxy(x,allow_none=True)
            allItems.append(serverWorker.groupByCSV(column))
        return (allItems)

    def headAPI(self,num):
        allItems = []
        for x in MyFunctions.workers(self):
            serverWorker = ServerProxy(x,allow_none=True)
            allItems.append(serverWorker.headCSV(num))
        return (allItems)

    def isinAPI(self,column,num1,num2):
        allItems = []
        for x in MyFunctions.workers(self):
            serverWorker = ServerProxy(x,allow_none=True)
            allItems.append(serverWorker.isinCSV(column,num1,num2))
        return (allItems)
        
    def itemsAPI(self):
        allItems = []
        for x in MyFunctions.workers(self):
            serverWorker = ServerProxy(x,allow_none=True)
            for a in serverWorker.itemsCSV():
                allItems.append(a)
        return (allItems)

    def minAPI(self,column):
        minWorkers = []
        for x in MyFunctions.workers(self):
            serverWorker = ServerProxy(x,allow_none=True)
            minWorkers.append(int(serverWorker.minCSV(column)))
        return (str (min(minWorkers)))

    def maxAPI(self,column):
        maxWorkers= []
        for x in MyFunctions.workers(self):
            serverWorker = ServerProxy(x,allow_none=True)
            maxWorkers.append(int(serverWorker.maxCSV(column)))
        return (str(max(maxWorkers)))

with SimpleXMLRPCServer(('localhost', 8001)) as server:
    server.register_instance(MyFunctions(), allow_dotted_names=True)
    server.register_multicall_functions()
    print('Serving XML-RPC on localhost port 8001')
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nKeyboard interrupt received, exiting.")
        sys.exit(0)
