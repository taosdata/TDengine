def init():
    pass        
def process(block):
    (rows, cols) = block.shape()
    result = []
    for i in range(rows):
        if block.data(i,0) is None:
            result.append(None)
        else:
            result.append(2*block.data(i,0))
    return result
def destroy():
    pass
