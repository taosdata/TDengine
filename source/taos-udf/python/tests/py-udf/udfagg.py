import pickle

def init():
    pass

def destroy():
    pass

def start():
    return pickle.dumps([])

def finish(buf):
    return repr(pickle.loads(buf))

def reduce(datablock, buf):
    (rows, cols) = datablock.shape()
    state = pickle.loads(buf)
    for i in range(rows):
        row = []
        for j in range(cols):
            row.append(datablock.data(i, j))
        state.append(row)
    return pickle.dumps(state) 
