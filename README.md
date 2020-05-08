[![Build Status](https://travis-ci.org/taosdata/TDengine.svg?branch=master)](https://travis-ci.org/taosdata/TDengine)
[![Build status](https://ci.appveyor.com/api/projects/status/kf3pwh2or5afsgl9/branch/master?svg=true)](https://ci.appveyor.com/project/sangshuduo/tdengine-2n8ge/branch/master)

[![TDengine](TDenginelogo.png)](https://www.taosdata.com)

# What is TDengine？

TDengine is an open-sourced big data platform under [GNU AGPL v3.0](http://www.gnu.org/licenses/agpl-3.0.html), designed and optimized for the Internet of Things (IoT), Connected Cars, Industrial IoT, and IT Infrastructure and Application Monitoring. Besides the 10x faster time-series database, it provides caching, stream computing, message queuing and other functionalities to reduce the complexity and cost of development and operation.

- **10x Faster on Insert/Query Speeds**: Through the innovative design on storage, on a single-core machine, over 20K requests can be processed, millions of data points can be ingested, and over 10 million data points can be retrieved in a second. It is 10 times faster than other databases.

- **1/5 Hardware/Cloud Service Costs**: Compared with typical big data solutions, less than 1/5 of computing resources are required. Via column-based storage and tuned compression algorithms for different data types, less than 1/10 of storage space is needed.

- **Full Stack for Time-Series Data**: By integrating a database with message queuing, caching, and stream computing features together, it is no longer necessary to integrate Kafka/Redis/HBase/Spark or other software. It makes the system architecture much simpler and more robust.

- **Powerful Data Analysis**: Whether it is 10 years or one minute ago, data can be queried just by specifying the time range. Data can be aggregated over time, multiple time streams or both. Ad Hoc queries or analyses can be executed via TDengine shell, Python, R or Matlab.

- **Seamless Integration with Other Tools**: Telegraf, Grafana, Matlab, R, and other tools can be integrated with TDengine without a line of code. MQTT, OPC, Hadoop, Spark, and many others will be integrated soon.

- **Zero Management, No Learning Curve**: It takes only seconds to download, install, and run it successfully; there are no other dependencies. Automatic partitioning on tables or DBs. Standard SQL is used, with C/C++, Python, JDBC, Go and RESTful connectors.

# Documentation
For user manual, system design and architecture, engineering blogs, refer to [TDengine Documentation](https://www.taosdata.com/en/documentation/)
 for details. The documentation from our website can also be downloaded locally from *documentation/tdenginedocs-en* or *documentation/tdenginedocs-cn*.

# Building
At the moment, TDengine only supports building and running on Linux systems. You can choose to [install from packages](https://www.taosdata.com/en/getting-started/#Install-from-Package) or from the source code. This quick guide is for installation from the source only.

To build TDengine, use [CMake](https://cmake.org/) 2.8 or higher versions in the project directory. Install CMake for example on Ubuntu:
```
sudo apt-get install -y cmake build-essential
```

To compile and package the JDBC driver source code, you should have a Java jdk-8 or higher and Apache Maven 2.7 or higher installed. 
To install openjdk-8 on Ubuntu:
```
sudo apt-get install openjdk-8-jdk
```
To install Apache Maven on Ubuntu:
```
sudo apt-get install maven
```

Build TDengine:

```
mkdir build && cd build
cmake .. && cmake --build .
```

To compile on an ARM processor (aarch64 or aarch32), please add option CPUTYPE as below:

aarch64:
```cmd
cmake .. -DCPUTYPE=aarch64 && cmake --build .
```

aarch32:
```cmd
cmake .. -DCPUTYPE=aarch32 && cmake --build .
```

# Quick Run
To quickly start a TDengine server after building, run the command below in terminal:
```cmd
./build/bin/taosd -c test/cfg
```
In another terminal, use the TDengine shell to connect the server:
```
./build/bin/taos -c test/cfg
```
option "-c test/cfg" specifies the system configuration file directory. 

# Installing
After building successfully, TDengine can be installed by:
```cmd
make install
```
Users can find more information about directories installed on the system in the [directory and files](https://www.taosdata.com/en/documentation/administrator/#Directory-and-Files) section. It should be noted that installing from source code does not configure service management for TDengine.
Users can also choose to [install from packages](https://www.taosdata.com/en/getting-started/#Install-from-Package) for it.

To start the service after installation, in a terminal, use:
```cmd
taosd
```

Then users can use the [TDengine shell](https://www.taosdata.com/en/getting-started/#TDengine-Shell) to connect the TDengine server. In a terminal, use:
```cmd
taos
```

If TDengine shell connects the server successfully, welcome messages and version info are printed. Otherwise, an error message is shown.

# Try TDengine
It is easy to run SQL commands from TDengine shell which is the same as other SQL databases.
```sql
create database db;
use db;
create table t (ts timestamp, a int);
insert into t values ('2019-07-15 00:00:00', 1);
insert into t values ('2019-07-15 01:00:00', 2);
select * from t;
drop database db;
```

# Developing with TDengine
### Official Connectors

TDengine provides abundant developing tools for users to develop on TDengine. Follow the links below to find your desired connectors and relevant documentation.

- [Java](https://www.taosdata.com/en/documentation/connector/#Java-Connector)
- [C/C++](https://www.taosdata.com/en/documentation/connector/#C/C++-Connector)
- [Python](https://www.taosdata.com/en/documentation/connector/#Python-Connector)
- [Go](https://www.taosdata.com/en/documentation/connector/#Go-Connector)
- [RESTful API](https://www.taosdata.com/en/documentation/connector/#RESTful-Connector)
- [Node.js](https://www.taosdata.com/en/documentation/connector/#Node.js-Connector)

# How to run the test cases and how to add a new test case?

### Prepare development environment

1.  sudo apt install
    build-essential cmake net-tools python-pip python-setuptools python3-pip
    python3-setuptools valgrind

2.  git clone <https://github.com/taosdata/TDengine>; cd TDengine

3.  mkdir debug; cd debug; cmake ..; make ; sudo make install

4.  pip install src/connector/python/linux/python2 ; pip3 install
    src/connector/python/linux/python3

### How to run TSIM test suite

1.  cd \<TDengine\>/tests/script

2.  sudo ./test.sh

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

**1. add a new TSIM test cases:**

TSIM test cases are now included in the new development branch and can be
added to the TDengine/tests/script/test.sh script based on the manual test
methods necessary to add test cases as described above.

**2. add a new Python test cases:**

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

### CI Covenant submission adoption principle.

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

### Third Party Connectors

The TDengine community has also kindly built some of their own connectors! Follow the links below to find the source code for them.

- [Rust Connector](https://github.com/taosdata/TDengine/tree/master/tests/examples/rust)
- [.Net Core Connector](https://github.com/maikebing/Maikebing.EntityFrameworkCore.Taos)

# TDengine Roadmap
- Support event-driven stream computing
- Support user defined functions
- Support MQTT connection
- Support OPC connection
- Support Hadoop, Spark connections
- Support Tableau and other BI tools

# Contribute to TDengine

Please follow the [contribution guidelines](CONTRIBUTING.md) to contribute to the project.

# Join TDengine WeChat Group

Add WeChat “tdengine” to join the group，you can communicate with other users.

