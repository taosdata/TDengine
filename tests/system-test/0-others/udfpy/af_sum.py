import pickle

def init():
    pass

def destroy():
    pass

def start():
    return pickle.dumps([])

def finish(buf):
    sums = pickle.loads(buf)
    all = 0
    for sum in sums:
        all += sum
    return all

def reduce(datablock, buf):
    (rows, cols) = datablock.shape()
    sums = pickle.loads(buf)
    sum = 0
    for i in range(rows):
        val = datablock.data(i, 0)
        if val is not None:
           sum += val
    sums.append(sum)
    return pickle.dumps(sums) 
