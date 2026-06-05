### Prepare development environment

1.  sudo apt install
    build-essential cmake net-tools python-pip python-setuptools python3-pip
    python3-setuptools valgrind psmisc curl

2.  git clone <https://github.com/taosdata/TDengine>; cd TDengine

3.  mkdir debug; cd debug; cmake ..; make ; sudo make install

4.  pip install ../src/connector/python ; pip3 install
    ../src/connector/python

5.  pip install numpy; pip3 install numpy (numpy is required only if you need to run querySort.py)  

>   Note: Both Python2 and Python3 are currently supported by the Python test
>   framework. Since Python2 is no longer officially supported by Python Software
>   Foundation since January 1, 2020, it is recommended that subsequent test case
>   development be guaranteed to run correctly on Python3. 

>   For Python2, please consider being compatible if appropriate without 
>   additional burden.
>
>   If you use some new Linux distribution like Ubuntu 20.04 which already do not
>   include Python2, please do not install Python2-related packages.
>
>   <https://nakedsecurity.sophos.com/2020/01/03/python-is-dead-long-live-python/> 

### How to run Python test suite

1.  cd \<TDengine\>/tests/pytest

2.  ./smoketest.sh \# for smoke test

3.  ./smoketest.sh -g \# for memory leak detection test with valgrind

4.  ./fulltest.sh \# for full test

>   Note1: TDengine daemon's configuration and data files are stored in
>   \<TDengine\>/sim directory. As a historical design, it's same place with
>   TSIM script. So after the TSIM script ran with sudo privilege, the directory
>   has been used by TSIM then the python script cannot write it by a normal
>   user. You need to remove the directory completely first before running the
>   Python test case. We should consider using two different locations to store
>   for TSIM and Python script.

>   Note2: if you need to debug crash problem with a core dump, you need
>   manually edit smoketest.sh or fulltest.sh to add "ulimit -c unlimited"
>   before the script line. Then you can look for the core file in
>   \<TDengine\>/tests/pytest after the program crash.


### How to add a new test case

**1. TSIM test cases:**

TSIM was the testing framework has been used internally. Now it still be used to run the test cases we develop in the past as a legacy system. We are turning to use Python to develop new test case and are abandoning TSIM gradually.

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

### CI submission adoption principle.

-   Every commit / PR compilation must pass. Currently, the warning is treated
    as an error, so the warning must also be resolved.

-   Test cases that already exist must pass.

-   Because CI is very important to support build and automatically test
    procedure, it is necessary to manually test the test case before adding it
    and do as many iterations as possible to ensure that the test case provides
    stable and reliable test results when added.

>   Note: In the future, according to the requirements and test development
>   progress will add stress testing, performance testing, code style, 
>   and other features based on functional testing.
