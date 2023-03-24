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
                return [None]
            row.append(val.decode('utf-8'))
        results.append(''.join(row))
    return results


