# init
def init():
    pass

# destory
def destory():
    pass

# return origin column one value 
def process(block):
    (rows, cols) = block.shape()
    results = []
    for i in range(rows):
        results.append(block.data(i,0))
    return results
