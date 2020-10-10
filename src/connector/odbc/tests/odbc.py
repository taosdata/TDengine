import pyodbc
cnxn = pyodbc.connect('DSN=TAOS_DSN;UID=root;PWD=taosdata', autocommit=True)
cnxn.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
#cnxn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
#cnxn.setencoding(encoding='utf-8')
cursor = cnxn.cursor()
cursor.execute("SELECT * from db.t")
row = cursor.fetchone() 
while row: 
    print(row)
    row = cursor.fetchone()

