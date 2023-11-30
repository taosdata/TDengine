---
toc_max_heading_level: 4
sidebar_label: R
title: Connect with R Language
description: This document describes how to connect to TDengine Cloud using the R client library.
---

## Configuration

After completing the installation of the R programming environment, you need to perform some configuration steps so that the RJDBC library can connect to and access the TDengine time series database correctly.

1. Load the RJDBC library and other necessary libraries in your R script:

```r
library(DBI)
library(rJava)
library(RJDBC)
```

2. Set the JDBC driver path and JDBC URL:

```r
# Set the JDBC driver path (modify it based on where you actually saved it)
driverPath <- "/path/to/taos-jdbcdriver-X.X.X-dist.jar"

# Set the JDBC URL (modify it according to your specific environment)
url <- "<jdbcURL>"
```

3. Load the JDBC driver:

```r
# Load the JDBC driver
drv <- JDBC("com.taosdata.jdbc.rs.RestfulDriver", driverPath)
```

4. Create a connection to the TDengine database:

```r
# Create a database connection
conn <- dbConnect(drv, url)
```

5. Once the connection is successful, you can use the conn object to perform various database operations such as querying data, inserting data, etc.
