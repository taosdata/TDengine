import time
def init():
    pass        
def process(block):
    (rows, cols) = block.shape()
    result = []
    for i in range(rows):
        result.append(block.data(i,0))
    time.sleep(12)
    return result
def destroy():
    pass
