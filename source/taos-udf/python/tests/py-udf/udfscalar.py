def init():
    pass

def destroy():
    pass

def process(datablock):
    (rows, cols) = datablock.shape()
    result = []
    for i in range(rows):
        row = []
        for j in range(cols):
            row.append(datablock.data(i, j))
        result.append(repr(row))
    return result
