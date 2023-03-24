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
            row.append(val.decode('utf_32_le'))
        row_str = ''.join(row)
        results.append(row_str.encode('utf_32_le'))
    return results


