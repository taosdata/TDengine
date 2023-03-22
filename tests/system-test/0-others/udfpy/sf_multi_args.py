# init
def init():
    pass

# destroy
def destroy():
    pass

# return origin column one value 
def process(block):
    (nrows, ncols) = block.shape()
    results = []
    for i in range(nrows):
        rows = []
        for j in range(ncols):
            val = block.data(i, j)
            rows.append(val)
        results.append(','.join(rows))
    return results

