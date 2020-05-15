### How to add a new test case

**1. TSIM test cases:**

TSIM test cases are the testing framework has been used internally. Now it still be used to run the test cases we develop in the past as a legacy system. We are turning to use Python to develop new test case and are abandoning TSIM gradually.

**2. Python test cases:**

**2.1 Please refer to \<TDengine\>/tests/pytest/insert/basic.py to add a new
test case.** The new test case must implement 3 functions, where self.init()
and self.stop() simply copy the contents of insert/basic.py and the test
logic is implemented in self.run(). You can refer to the code in the util
directory for more information.

**2.2 Edit smoketest.sh to add the path and filename of the new test case**

Note: The Python test framework may continue to be improved in the future,
hopefully, to provide more functionality and ease of writing test cases. The
method of writing the test case above does not exclude that it will also be
affected.

**2.3 What test.py does in detail:**

test.py is the entry program for test case execution and monitoring.

test.py has the following functions.

\-f --file, Specifies the test case file name to be executed
-p --path, Specifies deployment path

\-m --master, Specifies the master server IP for cluster deployment 
-c--cluster, test cluster function
-s--stop, terminates all running nodes

\-g--valgrind, load valgrind for memory leak detection test

\-h--help, display help

**2.4 What util/log.py does in detail:**

log.py is quite simple, the main thing is that you can print the output in
different colors as needed. The success() should be called for successful
test case execution and the success() will print green text. The exit() will
print red text and exit the program, exit() should be called for test
failure.

**util/log.py**

...

    def info(self, info):

        printf("%s %s" % (datetime.datetime.now(), info))

 

    def sleep(self, sec):

        printf("%s sleep %d seconds" % (datetime.datetime.now(), sec))

        time.sleep(sec)

 

    def debug(self, err):

        printf("\\033[1;36m%s %s\\033[0m" % (datetime.datetime.now(), err))

 

    def success(self, info):

        printf("\\033[1;32m%s %s\\033[0m" % (datetime.datetime.now(), info))

 

    def notice(self, err):

        printf("\\033[1;33m%s %s\\033[0m" % (datetime.datetime.now(), err))

 

    def exit(self, err):

        printf("\\033[1;31m%s %s\\033[0m" % (datetime.datetime.now(), err))

        sys.exit(1)

 

    def printNoPrefix(self, info):

        printf("\\033[1;36m%s\\033[0m" % (info)

...

**2.5 What util/sql.py does in detail:**

SQL.py is mainly used to execute SQL statements to manipulate the database,
and the code is extracted and commented as follows:

**util/sql.py**

\# prepare() is mainly used to set up the environment for testing table and
data, and to set up the database db for testing. do not call prepare() if you
need to test the database operation command.

def prepare(self):

tdLog.info("prepare database:db")

self.cursor.execute('reset query cache')

self.cursor.execute('drop database if exists db')

self.cursor.execute('create database db')

self.cursor.execute('use db')

...

\# query() is mainly used to execute select statements for normal syntax input

def query(self, sql):

...

\# error() is mainly used to execute the select statement with the wrong syntax
input, the error will be caught as a reasonable behavior, if not caught it will
prove that the test failed

def error()

...

\# checkRows() is used to check the number of returned lines after calling
query(select ...) after calling the query(select ...) to check the number of
rows of returned results.

def checkRows(self, expectRows):

...

\# checkData() is used to check the returned result data after calling
query(select ...) after the query(select ...) is called, failure to meet
expectation is

def checkData(self, row, col, data):

...

\# getData() returns the result data after calling query(select ...) to return
the resulting data after calling query(select ...)

def getData(self, row, col):

...

\# execute() used to execute sql and return the number of affected rows

def execute(self, sql):

...

\# executeTimes() Multiple executions of the same sql statement

def executeTimes(self, sql, times):

...

\# CheckAffectedRows() Check if the number of affected rows is as expected

def checkAffectedRows(self, expectAffectedRows):

...

>   Note: Both Python2 and Python3 are currently supported by the Python test
>   case. Since Python2 is no longer officially supported by January 1, 2020, it
>   is recommended that subsequent test case development be guaranteed to run
>   correctly on Python3. For Python2, please consider being compatible if
>   appropriate without additional
>   burden. <https://nakedsecurity.sophos.com/2020/01/03/python-is-dead-long-live-python/> 

