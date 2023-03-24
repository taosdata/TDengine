import pickle

def init():
    pass

def destroy():
    pass

def start():
    return pickle.dumps([])

def finish(buf):
    counts = pickle.loads(buf)
    all_count = 0
    for count in counts:
        all_count += count

    return all_count

def reduce(datablock, buf):
    (rows, cols) = datablock.shape()
    counts = pickle.loads(buf)
    batch_count = 0
    for i in range(rows):
        val = datablock.data(i, 0)
        if val is not None:
           batch_count += 1
    counts.append(batch_count)
    return pickle.dumps(counts) 
