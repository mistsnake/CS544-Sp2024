import mathdb_pb2_grpc, mathdb_pb2
import grpc
import threading
import sys
import csv
import traceback 

csv_list = sys.argv[2:]
channel = grpc.insecure_channel(f'localhost:{int(sys.argv[1])}')
stub = mathdb_pb2_grpc.MathDbStub(channel)

total_hits = 0
total_misses = 0
lock = threading.Lock()


def process_csv(csv_path):
    with open(csv_path) as f: 
        reader = csv.DictReader(f)
        for line in reader:
             # Obtain the correct response
            operation = line['operation']
            resp = None
            try:
                if operation == 'get':
                    resp = stub.Get(mathdb_pb2.GetRequest(key = line['key_a']))
                elif operation == 'set':
                    resp = stub.Set(mathdb_pb2.SetRequest(key = line['key_a'], value = float(line['key_b']))) 
                elif operation == 'add':
                    resp = stub.Add(mathdb_pb2.BinaryOpRequest(key_a = line['key_a'], key_b = line['key_b']))
                elif operation == 'sub':
                    resp = stub.Sub(mathdb_pb2.BinaryOpRequest(key_a = line['key_a'], key_b = line['key_b']))
                elif operation == 'mult':
                    resp = stub.Mult(mathdb_pb2.BinaryOpRequest(key_a = line['key_a'], key_b = line['key_b']))
                elif operation == 'div':
                    resp = stub.Div(mathdb_pb2.BinaryOpRequest(key_a = line['key_a'], key_b = line['key_b']))
                
                # Add hits and misses as we go through each line
                # Trying to access cache_hits when there was a miss
                # Results in an exception as it doesn't exist in a response
                with lock:
                    global total_hits
                    global total_misses

                    if resp.cache_hit:
                        total_hits += 1
                    else:
                        total_misses += 1
            
            # Catch any exceptions immediately
            except Exception:
                print(traceback.format_exc())


# Launch threads
threads = []
for path in csv_list:
    thread = threading.Thread(target=process_csv, args=(path,))
    thread.start()
    threads.append(thread)

# Join threads after threads are finished
for thread in threads:
    thread.join()

# Print hit rate
total = total_hits + total_misses
hit_rate = 0
if total > 0:
    hit_rate = total_hits / total 
print(hit_rate)
channel.close()


