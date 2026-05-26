import pickle
import numpy as np

def init():
    pass

def destroy():
    pass

def start():
    return pickle.dumps(0.0)

def finish(buf):
    return pickle.loads(buf)

def reduce(datablock, buf):
    (rows, cols) = datablock.shape()
    state = pickle.loads(buf)
    row = []
    for i in range(rows):
        for j in range(cols):
            row.append(datablock.data(i, j))
    if len(row) > 1:
        new_state = np.cumsum(row)[-1]
    else:
        new_state = state
    return pickle.dumps(new_state)
