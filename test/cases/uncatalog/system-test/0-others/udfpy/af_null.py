import pickle

def init():
    pass

def destroy():
    pass

def start():
    return pickle.dumps([])

def finish(buf):
    return None

def reduce(datablock, buf):
    (rows, cols) = datablock.shape()
    mins = pickle.loads(buf)
    mins.append(None)
    return pickle.dumps(mins) 
