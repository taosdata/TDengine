import pickle

def init():
    pass

def destroy():
    pass

def start():
    return pickle.dumps(0)

def finish(buf):
    count = pickle.loads(buf)
    return count

def reduce(datablock, buf):
    (rows, cols) = datablock.shape()
    count = pickle.loads(buf)
    for i in range(rows):
        val = datablock.data(i, 0)
        if val is not None:
           count += 1
    return pickle.dumps(count)