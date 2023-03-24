import pickle

def init():
    pass

def destroy():
    pass

def start():
    return pickle.dumps([])

def finish(buf):
    sums = pickle.loads(buf)
    all = None
    for sum in sums:
        if all is None:
            all = sum
        else:
            all += sum
    return all

def reduce(datablock, buf):
    (rows, cols) = datablock.shape()
    sums = pickle.loads(buf)
    sum = None
    for i in range(rows):
        val = datablock.data(i, 0)
        if val is not None:
           if sum is None:
               sum = val
           else:
               sum += val
               
    if sum is not None:
        sums.append(sum)
    return pickle.dumps(sums) 
