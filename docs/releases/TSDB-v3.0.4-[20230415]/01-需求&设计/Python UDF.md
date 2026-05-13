# Python UDF

### 1. UDF function management

Add language subclause to create a function statement that indicates the programming language. C is the default programming language.  C and Python are supported. Refer to https://docs.taosdata.com/taos-sql/udf/ for OUTPUTTYPE subclause and BUFSIZE subclause. When *OR REPLACE* is added and the function named function_name exists, the function is updated and its version is increased by 1. 
```sql {wrap}
CREATE [OR REPLACE] [AGGREGATE] FUNCTION function_name as library_path OUTPUTTYPE output_type [BUFSIZE buffer_size] [LANGUAGE 'C|Python'|'JavaScript'|'Lua'|'Java']
```

Add column *func_version* and *func_body* to ins_functions system table. The func_version column is initialized to zero and is increased by one after each function update. The func_body column shows the function body of the UDF.

### 2. TDengine type and Python UDF object type mapping

| TDEngine SQL Data Type(s) | Python UDF Object Type |
| --- | --- |
| TINYINT/TINYINT UNSIGNED/ SMALLINT/SMALLINT UNSIGNED/ INT/INT UNSIGNED/ BIGINT/BIGINT UNSIGNED | int |
| FLOAT/DOUBLE | float |
| BOOL | bool |
| BINARY/NCHAR/VARCHAR | bytes |
| TIMESTAMP | int |
| JSON and other data types | Not supported |

### 3. UDF functions

~~The ~~~~UDF~~~~ functions are defined in a python module named ~~~~*function_name*~~

#### 3.1 scalar function

scalar function shall implement a function *process* that accepts a datablock that is like a two dimension matrix of python object and returns a tuple of object of output_type. The functions *init* and *destroy* are defined when needed.
```python
def init():
    # initialization
def destroy():
    # destroy
def process(input: datablock) -> tuple[output_type]:
    # process input datablock, 
    # datablock.data(row, col) is to access the python object in location(row,col)
    # return tuple object consisted of object of type outputtype     

```

##### 3.1.1 Scalar function sample

The following  UDF convert_case takes a string parameter and converts the first letter of each word to its capital letter.
```python
 def init():
     pass
 def destory():
     pass
 def process(block):
     ret = []
     (rows,cols) = block.shape()
     for i in range(rows):
         str = block.data(i,0)
         if str is None:
             ret.append(None)
         else:
             res_str = ''
             words = str.split(' ')
             for w in words:
                 res_str = res_str + w[0:1].upper() + w[1:len(w)] + ' '
             ret.append(res_str)
     return ret
```

#### 3.2 Aggregation function

~~The UdfInterBuf python type is provided in C/C++ and is accessed by python. The state member of type bytes is the serialization of the state.~~ 
```python
class UdfInterBuf:
    def __init__(self, state:bytes, has_result:bool)->None:
        self.state = state;
        self.has_result = has_result
```

Aggregate function shall implement start, reduce, and finish function. The *start* function initializes the state, the *reduce* function accumulates the inputs into the state and returns new state, and the *finish* function generates final output from the accumulated state. The functions *Init* and *destroy* are defined when needed.
```python
def init():
    #initialization
def destroy():
    #destroy
def start() -> bytes:
    #return serialize(init_state)
def reduce(inputs: datablock, buf: bytes) -> bytes
    # deserialize buf to state
    # reduce the inputs and state into new_state. 
    # use inputs.data(i,j) to access python ojbect of location(i,j)
    # serialize new_state into new_state_bytes
    return new_state_bytes   
def merge(buf1:bytes, buf2:bytes) -> bytes
    #return merged buf1 and buf2
def finish(buf: bytes) -> output_type:
    #return obj of type outputtype   
```

##### 3.2.1 Aggregate Function Sample

The following udf computes the average of input values.
```python
import pickle
def init():
    pass
def destroy():
    pass
def start() -> bytes:
    return pickle.dumps([0,0])

def reduce(inputs, buf: bytes)
    state = pickle.loads(buf)
    (rows, cols) = inputs.shape()
    for i in range(rows):
        for j in range(cols):
            e = inputs.data(i,j)
            if e is not None:
                state[0] = state[0] + e
                state[1] = state[1] + 1
    return pickle.dumps(state)  

def finish(buf: bytes):
    state = pickle.loads(buf)
    return state[0]/state[1]
```

### 4. ~~Decorator~~

~~Python decorator shall be used to provide common functionality. One functionality shall be the serialization/deserialiation with python pickle module.~~
```plaintext
def pickle_serde_reduce(func):
    def wrap(inputs: tuple[tuple], buf: bytes) -> bytes:
        state_obj = pickle.loads(buf)
        new_state_obj = reduce(inputs, state_obj)
        new_state = pickle.dumps(new_state_obj)
        return new_state
    return wrap   
```

```plaintext
@pickle_serde_reduce
def reduce(inputs: tuple[tuple], state_obj: object) -> object:
    #reduce that accept inputs and state object and return
```

~~#~~~~ ~~~~TODO: Provoide the decorator definition. ~~

### 5. Deployment

1. Install taospyudf. libtaospyudf.so that executes python UDF script and some common python functions are included. The taospyudf library shall be compiled and installed at */usr/local/lib*
```plaintext
pip install taospyudf
lddconfig
```

1. Modify configuration **udfdLdLibPath **of taos.cfg to include the PYTHONPATH of udf function.
2. Write the python UDF function and create the UDF function with create function statement.
```sql
CREATE FUNCTION convert_case as 'path/to/udf.py' outputtype BINARY(20) LANGUAGE 'Python'
```

1. Use the function with SQL select statement
```sql
SELECT convert_case(column) from table_name
```

#### 5.1 Replace Function

After an UDF function is replaced with a new version of the UDF function,
- The new queries use the new version of  after at most 10s. If there are no existing queries with the UDF function running, the new version is applied immediately.
- Existing queries may use the new function after 10s. It is to achieve balance between performance and consistency and implementation effort.
If the UDF can not support concurrent execution of different versions, replace the function only when all existing queries using the UDF function are finished.
