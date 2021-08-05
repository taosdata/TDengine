# Connections with Other Tools

## <a class="anchor" id="grafana"></a> Grafana

TDengine can quickly integrate with [Grafana](https://www.grafana.com/), an open source data visualization system, to build a data monitoring and alarming system. The whole process does not require any code to write. The contents of the data table in TDengine can be visually showed on DashBoard.

### Install Grafana

TDengine currently supports Grafana 5.2.4 and above. You can download and install the package from Grafana website according to the current operating system. The download address is as follows:

https://grafana.com/grafana/download.

### Configure Grafana

TDengine Grafana plugin is in the /usr/local/taos/connector/grafanaplugin directory. 

Taking Centos 7.2 as an example, just copy grafanaplugin directory to /var/lib/grafana/plugins directory and restart Grafana.

```bash
sudo cp -rf /usr/local/taos/connector/grafanaplugin /var/lib/grafana/plugins/tdengine
```

### Use Grafana

#### Configure data source

You can log in the Grafana server (username/password:admin/admin) through localhost:3000, and add data sources through `Configuration -> Data Sources` on the left panel, as shown in the following figure:

![img](page://images/connections/add_datasource1.jpg)

Click `Add data source` to enter the Add Data Source page, and enter TDengine in the query box to select Add, as shown in the following figure:

![img](page://images/connections/add_datasource2.jpg)

Enter the data source configuration page and modify the corresponding configuration according to the default prompt:

![img](page://images/connections/add_datasource3.jpg)

- Host: IP address of any server in TDengine cluster and port number of TDengine RESTful interface (6041), default  [http://localhost:6041](http://localhost:6041/)
- User: TDengine username.
- Password: TDengine user password.

Click `Save & Test` to test. Success will be prompted as follows:

![img](page://images/connections/add_datasource4.jpg)

#### Create Dashboard

Go back to the home  to create Dashboard, and click `Add Query` to enter the panel query page:

![img](page://images/connections/create_dashboard1.jpg)

As shown in the figure above, select the TDengine data source in Query, and enter the corresponding sql in the query box below to query. Details are as follows:

- INPUT SQL: Enter the statement to query (the result set of the SQL statement should be two columns and multiple rows), for example: `select avg(mem_system) from log.dn where ts >= $from and ts < $to interval($interval)` , where `from`, `to` and `interval` are built-in variables of the TDengine plug-in, representing the query range and time interval obtained from the Grafana plug-in panel. In addition to built-in variables, it is also supported to use custom template variables.
- ALIAS BY: You can set alias for the current queries.
- GENERATE SQL: Clicking this button will automatically replace the corresponding variable and generate the final statement to execute.

According to the default prompt, query the average system memory usage at the specified interval of the server where the current TDengine deployed in as follows:

![img](page://images/connections/create_dashboard2.jpg)

> Please refer to Grafana [documents](https://grafana.com/docs/) for how to use Grafana to create the corresponding monitoring interface and for more about Grafana usage.

#### Import Dashboard

A `tdengine-grafana.json` importable dashboard is provided under the Grafana plug-in directory/usr/local/taos/connector/grafana/tdengine/dashboard/.

Click the `Import` button on the left panel and upload the  `tdengine-grafana.json` file:

![img](page://images/connections/import_dashboard1.jpg)

You can see as follows after Dashboard imported.

![img](page://images/connections/import_dashboard2.jpg)

## <a class="anchor" id="matlab"></a> MATLAB

MATLAB can access data to the local workspace by connecting directly to the TDengine via the JDBC Driver provided in the installation package.

### JDBC Interface Adaptation of MATLAB

Several steps are required to adapt MATLAB to TDengine. Taking adapting MATLAB2017a on Windows10 as an example:

- Copy the file JDBCDriver-1.0.0-dist.ja*r* in TDengine package to the directory ${matlab_root}\MATLAB\R2017a\java\jar\toolbox
- Copy the file taos.lib in TDengine package to ${matlab root dir}\MATLAB\R2017a\lib\win64
- Add the .jar package just copied to the MATLAB classpath. Append the line below as the end of the file of ${matlab root dir}\MATLAB\R2017a\toolbox\local\classpath.txt
- ```
  $matlabroot/java/jar/toolbox/JDBCDriver-1.0.0-dist.jar
  ```

- Create a file called javalibrarypath.txt in directory ${user_home}\AppData\Roaming\MathWorks\MATLAB\R2017a_, and add the _taos.dll path in the file. For example, if the file taos.dll is in the directory of C:\Windows\System32，then add the following line in file javalibrarypath.txt:
- ```
  C:\Windows\System32
  ```

- ### Connect to TDengine in MATLAB to get data

After the above configured successfully, open MATLAB.

- Create a connection:

```matlab
conn = database(‘db’, ‘root’, ‘taosdata’, ‘com.taosdata.jdbc.TSDBDriver’, ‘jdbc:TSDB://127.0.0.1:0/’)
```

* Make a query:

```matlab
sql0 = [‘select * from tb’]
data = select(conn, sql0);
```

* Insert a record:

```matlab
sql1 = [‘insert into tb values (now, 1)’]
exec(conn, sql1)
```

For more detailed examples, please refer to the examples\Matlab\TDEngineDemo.m file in the package.

## <a class="anchor" id="r"></a> R 

R language supports connection to the TDengine database through the JDBC interface. First, you need to install the JDBC package of R language. Launch the R language environment, and then execute the following command to install the JDBC support library for R language:

```R
install.packages('RJDBC', repos='http://cran.us.r-project.org')
```

After installed, load the RJDBC package by executing `library('RJDBC')` command.

Then load the TDengine JDBC driver:

```R
drv<-JDBC("com.taosdata.jdbc.TSDBDriver","JDBCDriver-2.0.0-dist.jar", identifier.quote="\"")
```

If succeed, no error message will display. Then use the following command to try a database connection:

```R
conn<-dbConnect(drv,"jdbc:TSDB://192.168.0.1:0/?user=root&password=taosdata","root","taosdata")
```

Please replace the IP address in the command above to the correct one. If no error message is shown, then the connection is established successfully, otherwise the connection command needs to be adjusted according to the error prompt. TDengine supports below functions in *RJDBC* package:

- `dbWriteTable(conn, "test", iris, overwrite=FALSE, append=TRUE)`: Write the data in a data frame iris to the table test in the TDengine server. Parameter overwrite must be false. append must be TRUE and the schema of the data frame iris should be the same as the table test.
- `dbGetQuery(conn, "select count(*) from test")`: run a query command
- `dbSendUpdate (conn, "use db")`: Execute any non-query sql statement. For example, `dbSendUpdate (conn, "use db")`, write data `dbSendUpdate (conn, "insert into t1 values (now, 99)")`, and the like.
- `dbReadTable(conn, "test")`: read all the data in table test
- `dbDisconnect(conn)`: close a connection
- `dbRemoveTable(conn, "test")`: remove table test

The functions below are not supported currently:

- `dbExistsTable(conn, "test")`: if table test exists
- `dbListTables(conn)`: list all tables in the connection