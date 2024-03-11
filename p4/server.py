import threading
from collections import OrderedDict
import grpc
from concurrent import futures
import mathdb_pb2_grpc
import mathdb_pb2
import traceback

class MathCache:
    def __init__(self):
        self.dictionary = {} 
        self.cache = OrderedDict() # (opp, key_a, key_b) : result | holds max 10
        self.lock = threading.Lock()
        
    def Set(self, key, value):
        with self.lock:
            self.dictionary[key] = value
            self.cache = OrderedDict()

    def Get(self, key):
        return self.dictionary[key]
    
    def cache_handler(self, opp, key_a, key_b, result):
        with self.lock:
            ## If cache is hit, adjust order and return True ##
            if (opp, key_a, key_b) in self.cache.keys():
                self.cache.move_to_end((opp, key_a, key_b))
                return True 
            
            ## If cache needs to be updated; adjust accordingly ##
            
            # Handle a full cache
            elif len(self.cache) >= 10:
                self.cache.popitem(last = False)
            
            # Add items to cache
            self.cache[(opp, key_a, key_b)] = result
            self.cache.move_to_end((opp, key_a, key_b))
            return False # adding a value is a miss
    
    def Add(self, key_a, key_b):
        result = self.Get(key_a) + self.Get(key_b)  
        cache_hit = self.cache_handler('add', key_a, key_b, result)
        return (result, cache_hit)
    
    def Sub(self, key_a, key_b):
        result = self.Get(key_a) - self.Get(key_b)
        cache_hit = self.cache_handler('sub', key_a, key_b, result)
        return (result, cache_hit)
    
    def Mult(self, key_a, key_b):
        result = self.Get(key_a) * self.Get(key_b)
        cache_hit = self.cache_handler('mult', key_a, key_b, result)
        return (result, cache_hit)
    
    def Div(self, key_a, key_b):
        result = self.Get(key_a)/self.Get(key_b)
        cache_hit = self.cache_handler('div', key_a, key_b, result)
        return (result, cache_hit)
    

class MathDb(mathdb_pb2_grpc.MathDbServicer):
    def __init__(self):
        super().__init__()
        self.MathCache = MathCache()

    def Set(self, request, context):
        try:
            err = ""
            self.MathCache.Set(request.key, request.value)
        except Exception:
            err = traceback.format_exc()
        return mathdb_pb2.SetResponse(error = err)
        
    def Get(self, request, context):
        try:
            err = ""
            val = self.MathCache.Get(request.key)
        except Exception:
            err = traceback.format_exc()
            val = 0.0

        return mathdb_pb2.GetResponse(value = val, error = err)

    def Add(self, request, context):
        err = ""
        result = (0.0, False)
        try:
            result = self.MathCache.Add(request.key_a, request.key_b) # result, cache hit
        except Exception:
            err = traceback.format_exc()
        return mathdb_pb2.BinaryOpResponse(value = result[0], cache_hit = result[1], error = err)

    def Sub(self, request, context):
        err = ""
        result = (0.0, False)
        try:
            result = self.MathCache.Sub(request.key_a, request.key_b) # result, cache hit
        except Exception:
            err = traceback.format_exc()

        return mathdb_pb2.BinaryOpResponse(value = result[0], cache_hit = result[1], error = err)
        
    def Mult(self, request, context):
        err = ""
        result = (0.0, False)
        try:
            result = self.MathCache.Mult(request.key_a, request.key_b) # result, cache hit
        except Exception:
            err = traceback.format_exc()

        return mathdb_pb2.BinaryOpResponse(value = result[0], cache_hit = result[1], error = err)

    def Div(self, request, context):
        err = ""
        result = (0.0, False)

        try:
            result = self.MathCache.Div(request.key_a, request.key_b) # result, cache hit
        except Exception:
            err = traceback.format_exc()

        return mathdb_pb2.BinaryOpResponse(value = result[0], cache_hit = result[1], error = err)


if __name__ == "__main__":
  server = grpc.server(futures.ThreadPoolExecutor(max_workers=4), options=(('grpc.so_reuseport', 0),))
  mathdb_pb2_grpc.add_MathDbServicer_to_server(MathDb(), server)
  server.add_insecure_port("[::]:5440", )
  server.start()
  server.wait_for_termination()