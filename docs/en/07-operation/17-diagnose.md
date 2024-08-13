---
title: Problem Diagnostics
description: This document describes how to diagnose issues with your TDengine cluster.
---

## Network Connection Diagnostics

When a TDengine client is unable to access a TDengine server, the network connection between the client side and the server side must be checked to find the root cause and resolve problems.

Diagnostics for network connections can be executed between Linux/Windows/macOS.

Diagnostic steps:

1. If the port range to be diagnosed is being occupied by a `taosd` server process, please first stop `taosd.
2. On the server side, execute command `taos -n server -P <port> -l <pktlen>` to monitor the port range starting from the port specified by `-P` parameter with the role of "server".
3. On the client side, execute command `taos -n client -h <fqdn of server> -P <port> -l <pktlen>` to send a testing package to the specified server and port.

-l &lt;pktlen&gt;: The size of the testing package, in bytes. The value range is [11, 64,000] and default value is 1,000.
Please note that the package length must be same in the above 2 commands executed on server side and client side respectively.

Output of the server side for the example is below:

```bash
# taos -n server -P 6030 -l 1000
network test server is initialized, port:6030
request is received, size:1000
request is received, size:1000
...
...
...
request is received, size:1000
request is received, size:1000
```

Output of the client side for the example is below:

```bash
# taos -n client -h 172.27.0.7 -P 6000
taos -n client -h v3s2 -P 6030 -l 1000
network test client is initialized, the server is v3s2:6030
request is sent, size:1000
response is received, size:1000
request is sent, size:1000
response is received, size:1000
...
...
...
request is sent, size:1000
response is received, size:1000
request is sent, size:1000
response is received, size:1000

total succ:  100/100	cost:   16.23 ms	speed:    5.87 MB/s
```

The output needs to be checked carefully for the system operator to find the root cause and resolve the problem.

## Server Log

The parameter `debugFlag` is used to control the log level of the `taosd` server process. The default value is 131. For debugging and tracing, it needs to be set to either 135 or 143 respectively.

Once this parameter is set to 135 or 143, the log file grows very quickly especially when there is a huge volume of data insertion and data query requests. Ensure that the disk drive on which logs are stored has sufficient space.

## Client Log

An independent log file, named as "taoslog+&lt;seq num&gt;" is generated for each client program, i.e. a client process. The parameter `debugFlag` is used to control the log level. The default value is 131. For debugging and tracing, it needs to be set to either 135 or 143 respectively.

The default value of `debugFlag` is also 131 and only logs at level of INFO/ERROR/WARNING are recorded. As stated above, for debugging and tracing, it needs to be changed to 135 or 143 respectively, so that logs at DEBUG or TRACE level can be recorded.

The maximum length of a single log file is controlled by parameter `numOfLogLines` and only 2 log files are kept for each `taosd` server process.

Log files are written in an async way to minimize the workload on disk, but the trade off for performance is that a few log lines may be lost in some extreme conditions. You can configure asynclog to 0 when needed for troubleshooting purposes to ensure that no log information is lost.
