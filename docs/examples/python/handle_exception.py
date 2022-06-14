import taos

try:
    conn = taos.connect()
    conn.execute("CREATE TABLE 123")  # wrong sql
except taos.Error as e:
    print(e)
    print("exception class: ", e.__class__.__name__)
    print("error number:", e.errno)
    print("error message:", e.msg)
except BaseException as other:
    print("exception occur")
    print(other)

# output:
# [0x0216]: syntax error near 'Incomplete SQL statement'
# exception class:  ProgrammingError
# error number: -2147483114
# error message: syntax error near 'Incomplete SQL statement'
