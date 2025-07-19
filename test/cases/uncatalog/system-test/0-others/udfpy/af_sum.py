import pickle

def init():
    pass

def destroy():
    pass

def start():
    return pickle.dumps(None)

def finish(buf):
    sum = pickle.loads(buf)
    return sum

def reduce(datablock, buf):
    (rows, cols) = datablock.shape()
    sum = pickle.loads(buf)
    for i in range(rows):
        val = datablock.data(i, 0)
        if val is not None:
           if sum is None:
               sum = val
           else:
               sum += val               
    return pickle.dumps(sum) 
