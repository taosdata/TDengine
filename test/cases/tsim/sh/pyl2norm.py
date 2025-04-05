import json
import math

def init():
    pass

def destroy():
    pass

def start():
    return json.dumps(0.0).encode('utf-8')

def finish(buf):
    sum_squares = json.loads(buf)
    result = math.sqrt(sum_squares)
    return result

def reduce(datablock, buf):
    (rows, cols) = datablock.shape()
    sum_squares = json.loads(buf)
    
    for i in range(rows):
        for j in range(cols):
            cell = datablock.data(i,j)
            if cell is not None:
                sum_squares += cell * cell
    return json.dumps(sum_squares).encode('utf-8') 
