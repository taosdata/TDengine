<center><h1>User's Guide to the Crash_Gen Tool</h1></center>

# Introduction

To effectively test and debug our TDengine product, we have developed a simple tool to 
exercise various functions of the system in a randomized fashion, hoping to expose 
maximum number of problems, hopefully without a pre-determined scenario.

# Preparation

To run this tool, please ensure the followed preparation work is done first.

1. Fetch a copy of the TDengine source code, and build it successfully in the `build/` 
    directory
1. Ensure that the system has Python3.8 or above properly installed. We use 
    Ubuntu 20.04LTS as our own development environment, and suggest you also use such
    an environment if possible.

# Simple Execution

To run the tool with the simplest method, follow the steps below:

1. Open a terminal window, start the `taosd` service in the `build/` directory 
    (or however you prefer to start the `taosd` service)
1. Open another terminal window, go into the `tests/pytest/` directory, and
    run `./crash_gen.sh -p -t 3 -s 10` (change the two parameters here as you wish)
1. Watch the output to the end and see if you get a `SUCCESS` or `FAILURE`

That's it!

# Running Clusters

This tool also makes it easy to test/verify the clustering capabilities of TDengine. You
can start a cluster quite easily with the following command:

```
$ cd tests/pytest/
$ ./crash_gen.sh -e -o 3
```

The `-e` option above tells the tool to start the service, and do not run any tests, while 
the `-o 3` option tells the tool to start 3 DNodes and join them together in a cluster. 
Obviously you can adjust the the number here.

## Behind the Scenes

When the tool runs a cluster, it users a number of directories, each holding the information
for a single DNode, see:

```
$ ls build/cluster*
build/cluster_dnode_0:
cfg  data  log

build/cluster_dnode_1:
cfg  data  log

build/cluster_dnode_2:
cfg  data  log
```

Therefore, when something goes wrong and you want to reset everything with the cluster, simple
erase all the files:

```
$ rm -rf build/cluster_dnode_*
```

## Addresses and Ports

The DNodes in the cluster all binds the the `127.0.0.1` IP address (for now anyway), and
uses port 6030 for the first DNode, and 6130 for the 2nd one, and so on.

## Testing Against a Cluster

In a separate terminal window, you can invoke the tool in client mode and test against
a cluster, such as:

```
$ ./crash_gen.sh -p -t 10 -s 100 -i 3
```

Here the `-i` option tells the tool to always create tables with 3 replicas, and run 
all tests against such tables.

# Additional Features

The exhaustive features of the tool is available through the `-h` option:

```
$ ./crash_gen.sh -h
usage: crash_gen_bootstrap.py [-h] [-a] [-b MAX_DBS] [-c CONNECTOR_TYPE] [-d] [-e] [-g IGNORE_ERRORS] [-i MAX_REPLICAS] [-l] [-n] [-o NUM_DNODES] [-p] [-r]
                              [-s MAX_STEPS] [-t NUM_THREADS] [-v] [-x]

TDengine Auto Crash Generator (PLEASE NOTICE the Prerequisites Below)
---------------------------------------------------------------------
1. You build TDengine in the top level ./build directory, as described in offical docs
2. You run the server there before this script: ./build/bin/taosd -c test/cfg

optional arguments:
  -h, --help            show this help message and exit
  -a, --auto-start-service
                        Automatically start/stop the TDengine service (default: false)
  -b MAX_DBS, --max-dbs MAX_DBS
                        Maximum number of DBs to keep, set to disable dropping DB. (default: 0)
  -c CONNECTOR_TYPE, --connector-type CONNECTOR_TYPE
                        Connector type to use: native, rest, or mixed (default: 10)
  -d, --debug           Turn on DEBUG mode for more logging (default: false)
  -e, --run-tdengine    Run TDengine service in foreground (default: false)
  -g IGNORE_ERRORS, --ignore-errors IGNORE_ERRORS
                        Ignore error codes, comma separated, 0x supported (default: None)
  -i MAX_REPLICAS, --max-replicas MAX_REPLICAS
                        Maximum number of replicas to use, when testing against clusters. (default: 1)
  -l, --larger-data     Write larger amount of data during write operations (default: false)
  -n, --dynamic-db-table-names
                        Use non-fixed names for dbs/tables, useful for multi-instance executions (default: false)
  -o NUM_DNODES, --num-dnodes NUM_DNODES
                        Number of Dnodes to initialize, used with -e option. (default: 1)
  -p, --per-thread-db-connection
                        Use a single shared db connection (default: false)
  -r, --record-ops      Use a pair of always-fsynced fils to record operations performing + performed, for power-off tests (default: false)
  -s MAX_STEPS, --max-steps MAX_STEPS
                        Maximum number of steps to run (default: 100)
  -t NUM_THREADS, --num-threads NUM_THREADS
                        Number of threads to run (default: 10)
  -v, --verify-data     Verify data written in a number of places by reading back (default: false)
  -x, --continue-on-exception
                        Continue execution after encountering unexpected/disallowed errors/exceptions (default: false)
```

