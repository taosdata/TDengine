import json

def init():
    pass

def destroy():
    pass

def start():
    return json.dumps(0).encode('utf-8')

def finish(buf):
    return json.loads(buf)

def reduce(datablock, state):
    (rows, cols) = datablock.shape()
    state = json.loads(state)
    for i in range(rows):
        if datablock.data(i,0) is not None:
            state += datablock.data(i, 0)
    return json.dumps(state).encode('utf-8') 
