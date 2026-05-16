import pickle

def init():
    pass

def destroy():
    pass

def start():
    return pickle.dumps([])

def finish(buf):
    mins = pickle.loads(buf)
    min_val = None
    for min in mins:
        if min_val is None or (min is not None and min < min_val):
            min_val = min
    return min_val

def reduce(datablock, buf):
    (rows, cols) = datablock.shape()
    mins = pickle.loads(buf)
    min = None
    for i in range(rows):
        val = datablock.data(i, 0)
        if min is None or (val is not None and val < min) :
            min = val
    if min is not None: 
       mins.append(min)
    return pickle.dumps(mins) 
