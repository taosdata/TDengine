def init():
    pass

def process(block):
    (rows, cols) = block.shape()
    result = []
    for i in range(rows):
        r = 2 ** 32 - 1
        for j in range(cols):
            cell = block.data(i,j)
            if cell is None:
                result.append(None)
                break
            else:
                r = r & cell
        else:
            result.append(r)
    return result

def destroy():
    pass
