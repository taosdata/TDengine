# init
def init():
    pass

# destroy
def destroy():
    pass

def process(block):
    (nrows, ncols) = block.shape()
    results = []
    for i in range(nrows):
        row = []
        for j in range(ncols):
            val = block.data(i, j)
            if val is None:
                row = None
                break
            row.append(val.decode('utf-8'))
        if row is None:
            results.append(None)
        else:    
            results.append(''.join(row))
    return results


