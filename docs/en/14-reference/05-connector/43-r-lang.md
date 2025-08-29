---
sidebar_label: R
title: R Client Library
slug: /tdengine-reference/client-libraries/r-lang
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import Rdemo from "../../assets/resources/_connect_r.mdx"

The RJDBC library in R language can enable R language programs to access TDengine data. Here are the installation process, configuration process, and R language example code.

## Installation Process

Before starting, please make sure that the R language environment is already installed. Then follow these steps to install and configure the RJDBC library:

1. Install Java Development Kit (JDK): The RJDBC library depends on the Java environment. Please download the JDK suitable for your operating system from the Oracle official website and follow the installation guide.

1. Install the RJDBC library: Execute the following command in the R console to install the RJDBC library.

```r
install.packages("RJDBC", repos='http://cran.us.r-project.org')
```

:::note

1. The R language software version 4.2 that comes with the Ubuntu system has a non-responsive bug when calling the RJDBC library, please install the installation package from the R language [official website](https://www.r-project.org/).
1. Installing the RJDBC package on Linux may require installing components needed for compilation, for example on Ubuntu execute the command `apt install -y libbz2-dev libpcre2-dev libicu-dev`.
1. On Windows systems, you need to set the JAVA_HOME environment variable.

:::

1. Download the TDengine JDBC driver: Visit the maven.org website and download the TDengine JDBC driver (taos-jdbcdriver-X.X.X-dist.jar).

1. Place the TDengine JDBC driver in an appropriate location: Choose a suitable location on your computer and save the TDengine JDBC driver file (taos-jdbcdriver-X.X.X-dist.jar) there.

## Configuration Process

After completing the installation steps, you need to make some configurations so that the RJDBC library can correctly connect to and access the TDengine time-series database.

1. Load RJDBC and other necessary libraries in the R script:

```r
library(DBI)
library(rJava)
library(RJDBC)
```

1. Set the JDBC driver path and JDBC URL:

```r
# Set the JDBC driver path (modify according to the actual location you saved)
driverPath <- "/path/to/taos-jdbcdriver-X.X.X-dist.jar"

# Set the JDBC URL (modify according to your specific environment)
url <- "jdbc:TAOS://localhost:6030/?user=root&password=taosdata"
```

1. Load the JDBC driver:

```r
# Load the JDBC driver
drv <- JDBC("com.taosdata.jdbc.TSDBDriver", driverPath)
```

1. Create a TDengine database connection:

```r
# Create a database connection
conn <- dbConnect(drv, url)
```

1. After the connection is successful, you can use the conn object for various database operations, such as querying data, inserting data, etc.

1. Finally, do not forget to close the database connection after use:

```r
# Close the database connection
dbDisconnect(conn)
```

## Example R Language Code Using RJDBC

Below is an example code that uses the RJDBC library to connect to the TDengine time-series database and perform a query operation:

<Rdemo/>

Please modify the JDBC driver, JDBC URL, username, password, and SQL query statement according to your actual situation to adapt to your TDengine time-series database environment and requirements.

Through the above steps and example code, you can use the RJDBC library in the R language environment to access the TDengine time-series database for data querying and analysis operations.
