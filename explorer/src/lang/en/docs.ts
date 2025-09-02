import { $IS_COMMUNITY, GRAFANA_GDS } from '@/utils/init';

export default {
  docs: {
    taosxAgent: {
      1: `Taosx-agent is used in some data access scenarios, such as Pi, OPC UA, and OPC DA, where access to data sources is restricted or the network environment is special. Taosx-agent can be deployed in an environment near the data source or even on the same server as the data source. The taosx-agent is responsible for reading the data from the data source and sending it to taosX.<br/><br/>Download  taosx-agent through  link <a href="{linuxDL}">Linux</a> or <a href="{windowDL}">Windows</a> to your local environment.For Linux, please decompress the downloaded file to a specified folder and execute the <code>install.sh</code> file inside the folder. For Windows, please double-click the downloaded file to install the taox-agent and then add <code>C:\\TDengine</code> to the Path variable of the system environments.<br/><br/>Open a shell, please execute the following command to check if taosx-agent is installed successfully.`,
      2: 'Input a unique name for the agent. The system will generate a connection token for the agent.',
      // 3: `IMPORTANT: Please save the endpoint and generated token to a local file BEFORE clicking on the "Next" button. TDengine Cloud does not save the generated token online and once you click "Next" you cannot retrieve this token and will have to create a new agent.<br/><br/>
      // To ensure your TDx agent works correctly you have to make changes to the <code>agent.toml</code> file. This file can be found in the following directory:<br/>
      // Linux: <code>/etc/taos</code><br/>
      3: `IMPORTANT: Please save the endpoint and the generated token to a local file BEFORE clicking on the "Next" button. You can not retrieve them and will have to create a new agent if you lose it.<br/><br/>
      To ensure your agent works, please copy the endpoint and the generated token to the <code>agent.toml</code> file. This file can be found in the following directory:<br/>
      Linux: <code>/etc/taos</code><br/>
Windows: <code>C:\\TDengine\\cfg\\</code>`,
      4: `Execute the following command in the shell.`,
      5: 'Check the agent running status with the following command in the shell.',
      6: `<a target='_blank' href='{agenturl}'>Configure Agent Documentation</a>`,
      7: 'Check Agent Connection',
      8: 'Success',
      9: 'Failed',
      10: 'Checking',
      11: `Please check the agent logs with:`,
      12: 'Check if  you can  fix the issue by yourself. If you can not, please report it to the TDengine team. '
      // 7: `If the agent token is wrong, the service will exit directly, you can check the logs with: `,
      // 8: `Refresh agent status in explorer to check if the agent is connected correctly. The status of an agent will be "Idle" when it has been connected.`
    },
    connector: {
      desc: 'Connect using the {0} to encapsulate SQL as a REST request.',
      bottom1: 'The client connection is then established.',
      bottom2: 'For how to write data and query data, please refer to ',
      bottom2_1: 'Insert Data',
      bottom2_2: 'Query Data',
      bottomand: ' and ',
      bottom3: 'For more details about how to write or query data via REST API, please check ',
      bottom3end: '.',
      java: {
        step1: 'Add Dependency',
        step2: 'Config',
        step3: 'Example',
        step3depdesc: 'In the "pom.xml" file, please add the Spring Boot and TDengine Java connector dependencies:',
        step3confdesc: 'In the "application.yml" file, please add the following configurations:',
        step3mybatisdesc1:
          'Define an interface called "meterMapper", which uses the MyBatis framework to map from TDengine database super table to Java object',
        step3mybatisdesc2:
          'Create a meterMapper.xml file under src/main/resources/mapper, and add the following SQL mapping',
        step3href:
          'For more details about how to write or query data from TDngine instance through Spring, please refer to',
        step3desc:
          'Code bellow get JDBC URL from environment variables first and then create a Connection object, witch is a standard JDBC Connection object.'
      },
      go: {
        step1: 'Initialize Module',
        step1desc: 'You need generate the go example model as the following:',
        step2: 'Add Dependency',
        step2desc: 'Add the driver-go dependency in go.mod file in the go project directory:',
        step3: 'Config',
        step4: 'Connect',
        step4desc: 'Copy code bellow to main.go:',
        step4desc1: 'Then download dependencies by execute command:',
        step4desc2: 'Finally, test the connection:'
      },
      python: {
        step1: 'Install connector',
        step1desc:
          'First, you need to install the latest taospy module which needs Python 3.6+. Run the command below in your terminal:',
        step2: 'Config',
        step3: 'Connect',
        step3desc:
          'Copy code bellow to your editor and run it. If you are using jupyter, assuming you have followed the guide about Jupyter in previous sections, you can copy the code into Jupyter editor in your browser.',
        step41Title: 'Step 1: Install',
        step41Desc:
          'For the users who are familiar with Jupyter to program in Python, both TDengine Python connector and Jupyter need to be ready in your environment. If you have not done yet, please use the commands below to install them.',
        step42Title: 'Step 2: Configure',
        step42Desc:
          'In order for Jupyter to connect to TDengine  instance, before launching Jupypter, the environment setting must be performed. We use Linux bash as example.',
        step43Title: 'Step 3: Connect',
        step43Desc:
          'Once jupyter lab is launched, Jupyter lab service is automatically connected and shown in your browser. You can create a new notebook and copy the sample code below and run it.'
      },
      node: {
        step1: 'Install connector',
        step2: 'Config',
        step3: 'Connect'
      },
      csharp: {
        step1: 'Create Project',
        step11desc: 'Add C# TDengine Driver class lib.',
        step12desc: 'Add following ItemGroup and Task to your project file.',
        step2: 'Config',
        step3: 'Connect',
        step31desc: 'The whole project file:',
        step32desc: 'The whole C# file:'
      },
      rust: {
        desc: 'Connect using the taos connector to encapsulate SQL in a websocket connection.',
        step1: 'Create Project',
        step2: 'Add Dependency',
        step2desc: 'Add dependency to Cargo.toml:',
        step3: 'Config',
        step4: 'Connect',
        step41desc: 'Copy following code to main.rs:',
        step42desc: 'Then you can execute cargo run to test the connection.'
      },
      rest: {
        desc: 'In this section we will explain how to write into TDengine  service using REST API',
        step1: 'Config',
        step2: 'Insert',
        step2desc:
          'Following command below show how to insert data into the table d1001 of the database test via the command line utility curl: ',
        step3: 'Query',
        step3desc:
          'Following command below show how to query data into from table ins_databases of the database information_schema via the command line utility curl.'
      },
      r: {
        step1: 'Install RJDBC',
        step11desc:
          "First of all, RJDBC depends on Java environment, please download the JDK from Oracle's official website that is suitable for your operating system, and follow the installation guide to install it.",
        step12desc: 'Then execute the following command to install RJDBC libaray in the R console:',
        step13desc: 'In the end, download the latest ',
        step13desc1: 'TDengine JDBC Driver',
        step13desc2: ' to a specified local directory:',
        step2: 'Config',
        step21desc: 'Then load RJDBC and other libraries in the R script:',
        step22desc: 'In the end, set the JDBC driver and TDengine JDBC URL:',
        step23desc:
          'Note: Please replace "[path]" with the absolute path of the local system where the actual TDengine JDBC driver is downloaded to, and replace "taos-jdbcdriver-X.X.X-dist.jar" with the full file name of the actual downloaded driver.',
        step3: 'Connect',
        insertdata: 'Insert Data',
        querydata: 'Query Data',
        step31desc: 'First of all, please load JDBC driver as the following:',
        step32desc: 'Then execute the following script to create the connection with TDengine  instance:'
      },
      odbc: {
        desc: 'TDengine ODBC driver is a driver specifically designed for TDengine based on the ODBC standard. It can be used by ODBC based applications on Windows to access  TDengine Cloud instance, like ',
        desc1: '.',
        step1: 'Install',
        step1full: 'Install ODBC connector',
        step11desc1: 'Only support Windows operation system. And you need to install ',
        step11desc2: 'VC Runtime Library',
        step11desc3: ' first. If already installed, please ignore this step.',
        step12desc1: 'Install ',
        step12desc2: 'TDengine Windows client installation package',
        step12desc3: ' .',
        step2: 'Configure',
        step2full: 'Configure ODBC DataSource',
        step21desc:
          'Click the "Start" Menu, and Search for "ODBC", and choose "ODBC Data Source (64-bit)" (Note: Don\'t choose 32-bit).',
        step22desc: 'Select the "User DSN" tab, and click "Add" button to enter the page for "Create Data Source".',
        step23desc:
          'Choose the data source to be added, here we choose "TDengine" and click "Finish", and enter the configuration page for "TDengine ODBC Data Source", fill in required fields as the following:',
        step23desc1: '[DSN]:',
        step23desc2: 'Data Source Name, required field, such as "MyTDengine"',
        step23desc3: '[Connection Type]:',
        step23desc4: 'required field, we choose "WebSocket"',
        step23desc5: '[URL]:',
        step23desc6: '[Database]:',
        step23desc7: 'optional field, the default database to access, such as "test"',
        step23desc8: '[URL]:',
        step23desc9:
          'Enter the service address of TDengine, for example, 192.168.1.100:6041 (Cloud services are not supported).',
        step23desc10: '[UserID]:',
        step23desc11: 'Enter the user name. If this parameter is not specified, the user name is root by default',
        step23desc12: '[Password]:',
        step23desc13: 'Enter the user password. If not, the default is taosdata',
        step24desc:
          'Click "Test Connection" to test whether the data source can be connectted; if successful, it will prompt "Successfully connected to {0}".'
      }
    },
    party: {
      prometheus: {
        title: 'Prometheus',
        desc: 'Configure Prometheus to write and read data from TDengine .',
        totaldesc1:
          'Prometheus is a widespread open-source monitoring and alerting system. Prometheus joined the Cloud Native Computing Foundation (CNCF) in 2016 as the second incubated project after Kubernetes, which has a very active developer and user community.',
        totaldesc2:
          "Prometheus provides `remote_write` interface to leverage other database products as its storage engine. To enable users of the Prometheus ecosystem to take advantage of TDengine's efficient writing, TDengine also provides support for this interface so that Prometheus data can be stored in TDengine via the `remote_write` interface with proper configuration to take full advantage of TDengine's efficient storage performance and clustering capabilities for time-series data.",
        step1: 'Prerequisites',
        step1desc:
          "In your TDengine  instance, click 'Explorer' on the left panel, then click '+' besides Databases, to create a new database named as 'prometheus_data'. Then execute `show databases` to confirm the database has been created successfully.",
        step2: 'Install Prometheus',
        step2desc: 'Supposed that you use Linux system with architecture amd64:',
        step21: 'Download',
        step22: 'Decompress and rename',
        step23: 'Change to directory prometheus',
        step2end:
          'Then Prometheus is installed in current directory. For more installation options, please refer to the',
        step2doc: 'official documentation',
        step3: 'Configure',
        step3desc:
          'Configuring Prometheus is done by editing the Prometheus configuration file `prometheus.yml` (If you followed previous steps, you can find prometheus.xml in current directory).',
        step3desc1:
          'The resulting configuration will collect data about prometheus itself from its own HTTP metrics endpoint, and store data to TDengine .',
        step4: 'Start Prometheus',
        step4desc: 'Prometheus should start up. It also started a web server at',
        step4desc1:
          '. If you want to access the web server from a browser which is not running on the same host as Prometheus, please change `localhost` to correct hostname, FQDN or IP address, depending on your network environment.',
        step5: 'Verify Remote Write',
        step5desc:
          "Log in TDengine , click 'Explorer' on the left navigation bar. You will see metrics collected by prometheus.",
        step5desc1: 'TDengine will automatically create unique IDs for sub-table names by the rule.'
      },
      telegraf: {
        title: 'Telegraf',
        desc: 'Configure Telegraf to write metrics to TDengine .',
        totaldesc1:
          'Telegraf is an open-source, metrics collection software. Telegraf can collect the operation information of various components without having to write any scripts to collect regularly, reducing the difficulty of data acquisition.',
        totaldesc2:
          "Telegraf's data can be written to TDengine by simply adding the output configuration of Telegraf to the URL corresponding to taosAdapter and modifying several configuration items. The presence of Telegraf data in TDengine can take advantage of TDengine's efficient storage query performance and clustering capabilities for time-series data.",
        step1: 'Prerequisites',
        step2: 'Install Telegraf',
        step2desc:
          "Before telegraf can write data into TDengine  service, you need to firstly manually create a database. Log in TDengine , click 'Explorer' on the left navigation bar, then click the '+' button besides 'Databases' to add a database named as 'telegraf' using all default parameters.",
        step2desc1: 'Supposed that you use Ubuntu system:',
        step2desc2: 'After installation, telegraf service should have been started. Lets stop it:',
        step2end: 'For installation instructions on other platforms please refer to the',
        step2doc: 'official documentation',
        step3: 'Configure',
        step3desc: 'Run this command in your terminal to save TDengine  token and URL as zariables:',
        step3desc1: 'Then run this command to generate new telegraf.conf.',
        step3desc2: "Edit section 'outputs.http'.",
        step3desc3:
          "The resulting configuration will collect CPU and memory data and sends it to TDengine database named 'telegraf'. Database 'telegraf' will be created automatically if it dose not exist in advance.",
        step4: 'Start Telegraf',
        step4desc: 'Start telegraf using new generated telegraf.conf file.',
        step5: 'Verify',
        step5desc: 'Check `weather` database `telegraf` exist by executing:',
        step5desc1: 'Check `weather` super table cpu and mem exist:',
        step5desc2: 'Telegraf collects the running status measurements of current system. You can enable',
        step5desc2input: 'input plugins',
        step5desc2insert: 'to insert',
        step5desc2format: 'other formats',
        step5desc2end: 'data to Telegraf then forward to TDengine.',
        step5desc3:
          "TDengine take influxdb format data and create unique ID for table names by the rule. The user can configure `smlChildTableName` parameter to generate specified table names if he/she needs. And he/she also need to insert data with specified data format. For example, Add `smlChildTableName=tname` in the taos.cfg file. Insert data `st,tname=cpu1,t1=4 c1=3 1626006833639000000` then the table name will be cpu1. If there are multiple lines has same tname but different tag_set, the first line's tag_set will be used to automatically creating table and ignore other lines. Please refer to",
        step5desc3end: 'TDengine Schemaless'
      },
      influxdb: {
        title: 'InfluxDB Line Protocol',
        desc: 'In this section we will explain how to write into TDengine  service using schemaless {0} over REST interface.',
        step1: 'Config',
        step1desc: 'Run this command in your terminal to save the TDengine  token and URL as variables:',
        step2: 'Insert',
        step2desc:
          'You can use any client that supports the http protocol to access the RESTful interface address `<_url>/influxdb/v1/write` to write data in InfluxDB compatible format to TDengine. The EndPoint is as follows:',
        step2desc1: 'Support InfluxDB query parameters as follows.',
        step2desc2: '`db` specifies the database name used by TDengine',
        step2desc3: '`precision` the time precision used by TDengine',
        step2desc3ns: 'nanoseconds',
        step2desc3u: 'microseconds',
        step2desc3ms: 'milliseconds',
        step2desc3s: 'seconds',
        step2desc3m: 'minutes',
        step2desc3h: 'hours',
        step3: 'Examples',
        step31: 'Insert Example',
        step32: 'Query Example with SQL',
        step32desc: '`measurement` is the super table name.',
        step32desc1: "you can filter data by tag, like:`where host='host1'`."
      },
      opentsdbjson: {
        title: 'OpenTSDB JSON Protocol',
        step1: 'Config',
        step2: 'Insert',
        step2desc:
          'You can use any client that supports the http protocol to access the RESTful interface address `<_url>/opentsdb/v1/put` to write data in OpenTSDB compatible format to TDengine. The EndPoint is as follows:',
        step3: 'Examples',
        step31: 'Insert Example',
        step32: 'Query Example with SQL',
        step32desc: '`meter_current` is the super table name.',
        step32desc1: 'you can filter data by tag, like:`where groupid=2`.'
      },
      opentsdbtelnet: {
        title: 'OpenTSDB Telnet Protocol',
        step1: 'Config',
        step2: 'Insert',
        step3: 'Examples',
        step31: 'Insert Example',
        step32: 'Query Example with SQL',
        step32desc: '`sys` is the super table name.',
        step32desc1: 'you can filter data by tag, like:`where host="web01"`.'
      }
    },
    dataout: {
      dump: {
        desc: 'Create serialized data backups.',
        step1: 'Introduction',
        step1desc:
          'taosdump is a tool that supports backing up data from a running TDengine cluster and restoring the backed up data to the same, or another running TDengine cluster.',
        step1desc1:
          'taosdump can back up a database, a super table, or a normal table as a logical data unit or backup data records in the database, super tables, and normal tables. When using taosdump, you can specify the directory path for data backup. If you do not specify a directory, taosdump will back up the data to the current directory by default.',
        step1desc2:
          'If the specified location already has data files, taosdump will prompt the user and exit immediately to avoid data overwriting. This means that the same path can only be used for one backup.',
        step1desc3:
          'Please be careful if you see a prompt for this and please ensure that you follow best practices and relevant SOPs for data integrity, backup and data security.',
        step1desc4:
          'Users should not use taosdump to back up raw data, environment settings, hardware information, server configuration, or cluster topology. taosdump uses ',
        step1desc5: ' as the data file format to store backup data.',
        step2: 'Installation',
        step2desc: 'To use taosdump, you need to download and install (',
        step2desc1: '. Before installing taosTools, please firstly download and install',
        step2desc2: 'Decompress the package and install.',
        step2desc3: 'Set environment variable.',
        step3: 'Common usage scenarios',
        step31: 'taosdump backup data',
        step31desc: 'backing up all databases: specify `-A` or `-all-databases` parameter.',
        step31desc1: 'backup multiple specified databases: use `-D db1,db2,... ` parameters;',
        step31desc2:
          'back up some super or normal tables in the specified database: use `dbname stbname1 stbname2 tbname1 tbname2 ...` parameters. Note that the first parameter of this input sequence is the database name, and only one database is supported. The second and subsequent parameters are the names of super or normal tables in that database, separated by spaces.',
        step31desc3:
          'back up the system log database: TDengine clusters usually contain a system database named `log`. The data in this database is the data that TDengine runs itself, and the taosdump will not back up the log database by default. If users need to back up the log database, users can use the `-a` or `-allow-sys command-line parameter.',
        step31desc4:
          "Loose mode backup: taosdump version 1.4.1 onwards provides `-n` and `-L` parameters for backing up data without using escape characters and 'loose' mode, which can reduce the number of backups if table names, column names, tag names do not use escape characters. This can also reduce the backup data time and backup data footprint. If you are unsure about using `-n` and `-L` conditions, please use the default parameters for 'strict' mode backup. See the",
        step31desc5: 'official documentation',
        step31desc6: ' for a description of escaped characters.',
        step32: 'taosdump recover data',
        step32desc:
          'Restore the data file in the specified path: use the `-i` parameter plus the path to the data file. You should not use the same directory to backup different data sets, and you should not backup the same data set multiple times in the same path. Otherwise, the backup data will cause overwriting or multiple backups.',
        step4: 'Detailed command-line parameter list',
        step4desc: 'The following is a detailed list of taosdump command-line arguments.'
      }
    },
    virtual: {
      grafana: {
        desc: `${GRAFANA_GDS} can be integrated with the open-source data visualization system  Grafana  to build a data monitoring and alerting system seamlessly without a line of code.  And you can visualize the data stored inside ${GRAFANA_GDS} on a dashboard. Learn more about using the ${GRAFANA_GDS} plugin on GitHub.`,
        topdesc: `${GRAFANA_GDS} can be integrated with the open-source data visualization system`,
        topdesc1: ` to build a data monitoring and alerting system seamlessly without a line of code.  And you can visualize the data stored inside ${GRAFANA_GDS} on a dashboard.Learn more about using the ${GRAFANA_GDS} plugin on  `,
        topdesc2: '.',
        step1: 'Prerequisites',
        step1desc: `Please make sure that Grafana has been installed. ${GRAFANA_GDS} currently supports Grafana versions 7.5 and above. Please refer to (`,
        step1desc1: ').',
        step2: `Install ${GRAFANA_GDS} plugin`,
        step2link: 'https://www.tdengine.com/assets-download/grafana-plugin/tdengine-datasource.zip',
        step2desc: 'Use the grafana-cli command line tool to install the plugin. After installation, Grafana needs to be restarted. On Linux or macOS, run the following command in your terminal:',
        step2desc1: 'On Windows, first ensure that the plugin installation directory exists (by default, it is located in the data/plugins subdirectory of your Grafana installation directory). Then, run the following command in the bin directory of the Grafana installation path using an administrator account:',
        script1: `If you can access Github easily, please run below script from Linux terminal to install ${GRAFANA_GDS} Datasource plugin.`,
        script2: `After that completed, please restart grafana-server.`,
        step3: 'Add Data Source',
        step3desc: `Inside Grafana data source configuration page, copy the host and user shown below and past them into the corresponding input boxes. `,
        step3desc1: 'Host:',
        step3desc2: 'User:',
        step3desc3: `Then click "Save & Test" button to verify if ${GRAFANA_GDS} data source works. `,
        step3username: 'UserName',
        ste3pwd: 'Password',
        step4: 'Use Grafana',
        step4desc: `Please add new dashboard or import exist dashboard to explore the data stored in the ${GRAFANA_GDS}.`,
        step4desc1: 'You can refer to the ',
        step4desc2: 'documentation',
        step4desc3: 'for more details.'
      },
      gds: {
        desc: `Looker Studioquickly access ${GRAFANA_GDS} and create interactive reports and dashboards using its web-based reporting features.The whole process does not require any code development.`,
        topdesc: 'Using its ',
        topconnector: 'partner connector',
        topdesc1: ` , Looker Studio can quickly access ${GRAFANA_GDS} and create interactive reports and dashboards using its web-based reporting features.The whole process does not require any code development. Share your reports and dashboards with individuals, teams, or the world. Collaborate in real time. Embed your report on any web page.`,
        topdesc2: 'Refer to ',
        topdesc3: `for additional information on utilizing the Looker Studio with ${GRAFANA_GDS}.`,
        step1: 'Choose Data Source',
        step1desc: 'The current ',
        step1desc1: 'connector',
        step1desc2: `supports two different types of data sources: ${GRAFANA_GDS} Server and ${GRAFANA_GDS} . Select '${GRAFANA_GDS} ' and then click 'NEXT'.`,
        step2: 'Connector Configuration',
        step21: 'Mandatory Config',
        step21desc: `${GRAFANA_GDS}  URL:`,
        step211: `${GRAFANA_GDS}  Token:`,
        step212: 'database',
        step212desc:
          'The database name that contains the table(no matter if it is a normal table, a super table or a child table) is the one you want to query for data and make reports on.',
        step213: 'table',
        step213desc: 'The name of the table that you wish to connect to in order to query its data and run a report.',
        step213desc1: 'Notice',
        step213desc2: ' The maximum amount of records that may currently be retrieved is 1000000 rows.',
        step22: 'Optional config',
        step221: 'Query range start date & end date',
        step221desc:
          "The page where we configure our connector has two text boxes.These two date filter conditions are used to limit the amount of data that will be retrieved, and the date should be entered in the format 'YYYY-MM-DD HH:MM:SS.' e.g.",
        step221desc1:
          "The query result's start timestamp is defined by the `start date`. To put it another way, records from before this `start date` won't be received.",
        step221desc2:
          "The `end time` indicates the query result's end timestamp. Therefore, records that were written after this end date cannot be retrieved. These conditions are utilized in the where clause in SQL statements, such as:",
        step221desc3: 'In fact, you can speed up the data loading in your report by using these filters.',
        step221desc4: `Click 'CONNECT' once configuration is complete, then you can connect to your '${GRAFANA_GDS} ' with the given database and table.`,
        step3: 'Connector Configuration',
        step3desc: `Unlock the power of your data with interactive dashboards and beautiful reports with the data stored in ${GRAFANA_GDS}.`,
        step3desc1: 'And refer to',
        step3desc2: 'documentation',
        step3desc3: 'for more details.'
      }
    },
    tool: {
      cli: {
        desc: 'The interactive shell for operating on TDengine',
        topdesc:
          'The TDengine command-line interface (hereafter referred to as `TDengine CLI`) is the most simplest way for users to manipulate and interact with TDengine instances.',
        step1: 'Installation',
        step1desc: 'To run TDengine CLI to access TDengine , please install ',
        step1desc1: 'TDengine client',
        step1desc2: 'installation package (',
        step1desc3: ', ',
        step1desc4: ') first.',
        step2: 'Config',
        step2desc: 'Run this command in your Linux terminal to save  DSN as variable:',
        step2desc1: 'Run this command in your Windows CMD prompt to save  DSN as variable:',
        step2desc2: 'Or run this command in your Windows PowerShell environment to save  DSN as variable:',
        step2desc3: 'Run this command in your Mac terminal to save  DSN as variable:',
        step3: 'Connect',
        step3desc: 'To access the TDengine , you can execute `taos` if you already set the environment variable.',
        step3desc1:
          'To access the TDengine , you can execute below command if you already set the environment variable:',
        step4: 'Using TDengine CLI',
        step4desc:
          'TDengine CLI will display a welcome message and version information if it successfully connected to the TDengine service. If it fails, TDengine CLI will print an error message. The TDengine CLI prompts as follows:',
        step4desc1:
          'After entering the TDengine CLI, you can execute various SQL commands, including inserts, queries, or administrative commands. ',
        step4desc2: 'official document',
        step4desc3: 'for more details.'
      },
      benchmark: {
        desc: 'The tool for benchmark testing of inserting or querying data.',
        step1: 'Introduction',
        step1desc:
          "taosBenchmark (formerly taosdemo ) is a tool for testing the performance of TDengine products. taosBenchmark can test the performance of TDengine's insert, query, and subscription functions and simulate large amounts of data generated by many devices. taosBenchmark can be configured to generate user defined databases, supertables, subtables, and the time series data to populate these for performance benchmarking. taosBenchmark is highly configurable and some of the configurations include the time interval for inserting data, the number of working threads and the capability to insert disordered data. The installer provides taosdemo as a soft link to taosBenchmark for compatibility with past users.",
        step1desc1:
          "Please be noted that in the context of TDengine  service, non privileged user can't create database using any tool, including taosBenchmark. The database needs to be firstly created in the data explorer in TDengine  service console. For any content about creating database in this document, the user needs to ignore and create the database manually inside TDengine  service.",
        step2: 'Installation',
        step2desc: 'To use taosBenchmark, you need to download and install(',
        step2desc1: ' or downlaod and install ',
        step2desc2: 'TDengine client installtion package',
        step2desc3: 'Decompress the package and install.',
        step3: 'Run',
        step31: 'Configuration and running methods',
        step31desc: 'Run this command in your Linux terminal to save  DSN as variable:',
        step31desc1: 'Users can use `-f json-file` to specify a configuration file.',
        step31desc2:
          'taosBenchmark supports the complete performance testing of TDengine by providing functionally to write, query, and subscribe. These three functions are mutually exclusive, users can only select one of them each time taosBenchmark runs. The query and subscribe functionalities are only configurable using a json configuration file by specifying the parameter `filetype`, while write can be performed through both the command-line and a configuration file. If you want to test the performance of queries configure taosBenchmark with the configuration file. You can modify the value of the `filetype` parameter to specify the function that you want to test.',
        step31desc3: 'Make sure that the TDengine cluster is running correctly before running taosBenchmark.',
        step32: 'Run with the configuration file',
        step32desc:
          'A sample configuration file is provided in the taosBenchmark installation package under `<install_directory>/examples`.',
        step32desc1:
          '  Use the following command-line to run taosBenchmark and control its behavior via a configuration file.',
        step33: 'Sample configuration files',
        step34: 'Insert Scenario JSON Profile Example',
        step35: 'Query Scenario JSON Profile Example',
        step4: 'Configuration file parameters',
        step41: 'General configuration parameters',
        step41desc: 'The parameters listed in this section apply to all function modes.',
        step41desc1:
          ': The function to be tested, with optional values `insert`, `query`. These correspond to the insert and query, respectively. Users can specify only one of these in each configuration file.',
        step41desc2: ": specify the TDengine cluster configuration file's directory. The default path is /etc/taos.",
        step41desc3: ': Specify the FQDN of the TDengine server to connect. The default value is `localhost`.',
        step41desc4: ': The port number of the TDengine server to connect to, the default value is `6030`.',
        step41desc5: ': The user name of the TDengine server to connect to, the default is `root`.',
        step41desc6: ': The password to connect to the TDengine server, the default value is `taosdata`.',
        step42: 'Insert scenario configuration parameters',
        step42desc:
          '`filetype` must be set to `insert` in the insertion scenario. See [General Configuration Parameters](#General Configuration Parameters)',
        step43: 'Stream processing related configuration parameters',
        step43desc:
          'The parameters for creating streams are configured in `stream` in the json configuration file, as shown below.',
        step43desc1: ': Name of the stream. Mandatory.',
        step43desc2: ': Name of the supertable for the stream. Mandatory.',
        step43desc3: ': SQL statement for the stream to process. Mandatory.',
        step43desc4: ': Triggering mode for stream processing. Optional.',
        step43desc5: ': Watermark for stream processing. Optional.',
        step43desc6: ': Whether to create the stream. Specify yes to create the stream or no to not create the stream.',
        step44: 'Super table related configuration parameters',
        step44desc:
          'The parameters for creating super tables are configured in `super_tables` in the json configuration file, as shown below.',
        step44desc1: ': Super table name, mandatory, no default value.',
        step44desc2:
          ": whether the child table already exists, default value is 'no', optional value is 'yes' or 'no'.",
        step44desc3: ': The number of child tables, the default value is 10.',
        step44desc4: ': The prefix of the child table name, mandatory configuration item, no default value.',
        step44desc5:
          ": specify the super table and child table names containing escape characters. The value can be 'yes' or 'no'. The default is 'no'.",
        step44desc6:
          ": only when insert_mode is taosc, rest, stmt, and childtable_exists is 'no'. 'yes' means taosBenchmark will automatically create non-existent tables when inserting data; 'no' means that taosBenchmark will create all tables before inserting.",
        step44desc7:
          ': the number of tables per batch when creating sub-tables, default is 10. Note: the actual number of batches may not be the same as this value. If the executed SQL statement is larger than the maximum length supported, it will be automatically truncated and re-executed to continue creating.',
        step44desc8:
          ": specify the source of data-generation. Default is taosBenchmark randomly generated. Users can configure it as 'rand' and 'sample'. When 'sample' is used, taosBenchmark will use the data in the file specified by the `sample_file` parameter.",
        step44desc9:
          ': insertion mode with options taosc, rest, stmt, sml, sml-rest, corresponding to normal write, restful interface write, parameter binding interface write, schemaless interface write, restful schemaless interface write (provided by taosAdapter). The default value is taosc.',
        step44desc10:
          ": Specify whether to keep writing. If 'yes', insert_rows will be disabled, and writing will not stop until Ctrl + C stops the program. The default value is 'no', i.e., taosBenchmark will stop the writing after the specified number of rows are written. Note: insert_rows must be configured as a non-zero positive integer even if it fails in continuous write mode.",
        step44desc11:
          ': Insert data using line protocol. Only works when insert_mode is sml or sml-rest. The value can be `line`, `telnet`, or `json`.',
        step44desc12:
          ': Communication protocol in telnet mode only takes effect when insert_mode is sml-rest, and line_protocol is telnet. If not configured, the default protocol is http.',
        step44desc13: ': The number of inserted rows per child table, default is 0.',
        step44desc14:
          ': Effective only if childtable_exists is yes, specifies the offset when fetching the list of child tables from the super table, i.e., starting from the first child table.',
        step44desc15:
          ': Effective only when childtable_exists is yes, specifies the upper limit for fetching the list of child tables from the super table.',
        step44desc16:
          ': Enables interleaved insertion mode and specifies the number of rows of data to be inserted into each child table at a time. Staggered insertion mode means inserting the number of rows specified by this parameter into each sub-table and repeating the process until all sub-tables have been inserted. The default value is 0, i.e., data is inserted into one sub-table before the next sub-table is inserted.',
        step44desc17:
          ': Specifies the insertion interval in ms for interleaved insertion mode. The default value is 0. It only works if `-B/--interlace-rows` is greater than 0. After inserting interlaced rows for each child table, the data insertion thread will wait for the interval specified by this value before proceeding to the next round of writes.',
        step44desc18:
          ': If this value is a positive number n, only the first n columns are written to, only if insert_mode is taosc and rest, or all columns if n is 0.',
        step44desc19:
          ': Specifies the percentage probability of disordered (i.e. out-of-order) data in the value range [0,50]. The default is 0, which means there is no disorder data.',
        step44desc20:
          ': Specifies the timestamp fallback range for the disordered data. The disordered timestamp is generated by subtracting a random value in this range, from the timestamp that would be used in the non-disorder case. Valid only if the percentage of disordered data specified by `-O/--disorder` is greater than 0.',
        step44desc21:
          ': The timestamp step for inserting data in each child table, in units consistent with the `precision` of the database. For e.g. if the `precision` is milliseconds, the timestamp step will be in milliseconds. The default value is 1.',
        step44desc22: ': The timestamp start value of each sub-table, the default value is now.',
        step44desc23: ": The type of the sample data file; for now only 'csv' is supported.",
        step44desc24:
          ': Specify a CSV format file as the data source. It only works when data_source is a sample. If the number of rows in the CSV file is less than or equal to prepared_rand, then taosBenchmark will read the CSV file data cyclically until it is the same as prepared_rand; otherwise, taosBenchmark will read only the rows with the number of prepared_rand. The final number of rows of data generated is the smaller of the two.',
        step44desc25:
          ': effective only when data_source is `sample`, indicates whether the CSV file specified by sample_file contains the first timestamp column. Default is no. If set to yes, the first column of the CSV file is used as `timestamp`. Since the timestamp of the same sub-table cannot be repeated, the amount of data generated depends on the same number of rows of data in the CSV file, and insert_rows will be invalidated.',
        step44desc26:
          ': only works when insert_mode is taosc, rest. The final tag value is related to the childtable_count. Suppose the tag data rows in the CSV file are smaller than the given number of child tables. In that case, taosBenchmark will read the CSV file data cyclically until the number of child tables specified by childtable_count is generated. Otherwise, taosBenchmark will read the childtable_count rows of tag data only. The final number of child tables generated is the smaller of the two.',
        step45: 'TSMA configuration parameters',
        step45desc: 'The configuration parameters for specifying TSMAs are in `tsmas` in `super_tables`.',
        step45desc1: ': Specifies TSMA name. Mandatory.',
        step45desc2: ': Specifies TSMA function. Mandatory.',
        step45desc3: ': Specifies TSMA interval. Mandatory.',
        step45desc4: ': Specifies time offset for TSMA window. Mandatory.',
        step45desc5: ': Specifies custom configurations to attach to the end of the TSMA creation statement. Optional.',
        step45desc6:
          ': Specifies the number of inserted rows after which TSMA is started. Optional. The default value is 0.',
        step46: 'Tag and Data Column Configuration Parameters',
        step46desc:
          'The configuration parameters for specifying super table tag columns and data columns are in `columns` and `tag` in `super_tables`, respectively.',
        step46desc1:
          ': Specify the column type. For optional values, please refer to the data types supported by TDengine. Note: JSON data type is unique and can only be used for tags. When using JSON type as a tag, there is and can only be this one tag. At this time, `count` and `len` represent the meaning of the number of key-value pairs within the JSON tag and the length of the value of each KV pair. Respectively, the value is a string by default.',
        step46desc2:
          ': Specifies the length of this data type, valid for NCHAR, BINARY, and JSON data types. If this parameter is configured for other data types, a value of 0 means that the column is always written with a null value; if it is not 0, it is ignored.',
        step46desc3:
          ": Specifies the number of consecutive occurrences of the column type, e.g., 'count': 4096 generates 4096 columns of the specified type.",
        step46desc4:
          ": The name of the column, if used together with count, e.g. 'name': 'current', 'count':3, then the names of the 3 columns are current, current_2. current_3.",
        step46desc5: ': The minimum value of the column/label of the data type.',
        step46desc6: ': The maximum value of the column/label of the data type.',
        step46desc7:
          ': The value field of the nchar/binary column/label, which will be chosen randomly from the values.',
        step46desc8: ': Insert the column into the BSMA. Enter `yes` or `no`. The default is `no`.',
        step47: 'Insertion behavior configuration parameters',
        step47desc: ': specify the number of threads to insert data. Default is 8.',
        step47desc1: ': The number of threads to build the table, default is 8.',
        step47desc2:
          ': The number of pre-established connections to the TDengine server. If not configured, it is the same as number of threads specified.',
        step47desc3: ': The path to the result output file, the default value is . /output.txt.',
        step47desc4:
          ': The switch parameter requires the user to confirm after the prompt to continue. The default value is false.',
        step47desc5:
          ': Enables interleaved insertion mode and specifies the number of rows of data to be inserted into each child table at a time. Staggered insertion mode means inserting the number of rows specified by this parameter into each sub-table and repeating the process until all sub-tables have been inserted. The default value is 0, i.e., data is inserted into one sub-table before the next sub-table is inserted. This parameter can also be configured in `super_tables`, and if so, the configuration in `super_tables` takes precedence and overrides the global setting.',
        step47desc6:
          ': Specify the insert interval in `ms` for interleaved insert mode. The default value is 0. It only works if `-B/--interlace-rows` is greater than 0. After inserting interlaced rows for each child table, the data insertion thread will wait for the interval specified by this value before proceeding to the next round of writes. This parameter can also be configured in `super_tables`, and if so, the configuration in `super_tables` takes precedence and overrides the global setting.',
        step47desc7:
          ': Writing the number of rows of records per request to TDengine, the default value is 30000. When it is set too large, the TDengine client driver will return the corresponding error message, so you need to lower the setting of this parameter to meet the writing requirements.',
        step47desc8:
          ': The number of unique values in the generated random data. A value of 1 means that all data are equal. The default value is 10000.',
        step48: 'Query scenario configuration parameters',
        step48desc:
          '`filetype` must be set to `query` in the query scenario. See [General Configuration Parameters](#General Configuration Parameters) for details of this parameter and other general parameters',
        step49: 'Configuration parameters for executing the specified query statement',
        step49desc:
          'The configuration parameters for querying the sub-tables or the normal tables are set in `specified_table_query`.',
        step49desc1: ': The query interval in seconds, the default value is 0.',
        step49desc2: ': The number of threads to execute the query SQL, the default value is 1.',
        step49desc3: ': the SQL command to be executed.',
        step49desc4:
          ': the file to save the query result. If it is unspecified, taosBenchmark will not save the result.',
        step410: 'Configuration parameters of query super table',
        step410desc: 'The configuration parameters of the super table query are set in `super_table_query`.',
        step410desc1: ': Specify the name of the super table to be queried, required.',
        step410desc2: ': The query interval in seconds, the default value is 0.',
        step410desc3: ': The number of threads to execute the query SQL, the default value is 1.',
        step410desc4:
          ": The SQL command to be executed. For the query SQL of super table, keep 'xxxx' in the SQL command. The program will automatically replace it with all the sub-table names of the super table. Replace it with all the sub-table names in the super table.",
        step410desc5: ': The file to save the query result. If not specified, taosBenchmark will not save result.'
      }
    },
    topic: {
      topdesc:
        'You can follow the following steps to consume the topic `{2}` from the selected instance `{1}` of the organization `{0}`.',
      python: {
        step1: 'Install Module',
        step1desc:
          'First, you need to install the `taos-ws-py` module version >= `0.2.1`. Run the command below in your terminal.',
        step1desc1: "You'll need to have Python3 installed."
      },
      go: {
        step1: 'Initialize',
        step1desc: 'You need generate the go example model and the `driver-go` dependency:'
      },
      rust: {
        step1: 'Create Project',
        step1desc: 'You can create the Rust project:',
        step1desc1: 'Then add the dependency to the `Cargo.toml` file:'
      },
      createProject: 'Create Project',
      step1desc: 'You can create the {0} project:',
      step1desc1: 'Then add the dependency to the `{0}` file:',
      step2: 'Configuration',
      step3: 'Create Consumer',
      step3desc: 'You can create a consumer as the following code:',
      step4: 'Subscribe Topic',
      step4desc: 'You can subscribe the shared topic `{0}` as the following code:',
      step5: 'Close Consumer',
      step5desc:
        'You can close the consume if you want to unsubscribe the messages sent by the shared topic `{0}` as the following code:',
      step6: 'Full Example',
      step6desc: 'The following are full sample codes about how to consume the shared topic `{0}`:',
      enddesc: 'For more details about data subscription, please refer to',
      enddesc1: '.',
      enddesc2: 'Data Subscription',
      defaultTopic: 'Topic'
    },
    dashboard: {
      desc: `To monitor ${GRAFANA_GDS} running status and get alerts if something goes wrong, please use Grafana. ${GRAFANA_GDS} can be integrated with Grafana smoothly without a line of code. `,
      topdesc: `To monitor ${GRAFANA_GDS} running status and get alerts if something goes wrong, please use `,
      topdesc1: ` . ${GRAFANA_GDS} can be integrated with Grafana smoothly without a line of code. `,
      topdesc2: ``,
      topdesc3: '.',
      step1: 'Install Grafana',
      step1desc: `${GRAFANA_GDS} currently supports Grafana versions 7.5 and above. Please go to the Grafana official website to download the installation package`,
      pluginsdesc: `Open Grafana from browser, click the three horizontal bar icon, click <code>Connections</code>, inside the search bar, search ${GRAFANA_GDS}, then "${GRAFANA_GDS} Data Source" should pop up. Click <code>Install</code> to install the ${GRAFANA_GDS} plugin. Once it's installed, you can add ${GRAFANA_GDS} data source right away.`,
      plugin1desc: `1. Open Grafana from browser, click the three horizontal bar icon, then <code>Connections</code>.`,
      plugin2desc: `2. Inside the search bar, search TDengine, then <code>TDengine Data Source</code> should pop up. `,
      plugin3desc: `3. Click <code>Install</code> to install the TDengine plugin. `,
      plugin4desc: `4. Once it's installed, you can add TDengine data source right away.`,
      script1: `If you can access Github easily, please run below script from Linux terminal to install ${GRAFANA_GDS} Datasource plugin.`,
      script2: `After that completed, please restart grafana-server.`,
      step2: `Install ${GRAFANA_GDS} plugin`,
      step2desc: `Please copy the following shell commands to export \`${GRAFANA_GDS}_URL\` and  \`${GRAFANA_GDS}_TOKEN\` for the data source installation.`,
      step2desc1: `Run below script from Linux terminal to install ${GRAFANA_GDS} data source plugin.`,
      step2desc2: 'After that completed, please restart grafana-server.',
      step3: 'Add Data Source',
      step3desc1: 'Host:',
      step3desc2: 'User:',
      step3username: 'UserName',
      ste3pwd: 'Password',
      step3desc3: `Input your password to login to TDengine, then click <code>Save & Test</code> button to verify if ${GRAFANA_GDS} data source works. `,
      step3desc: `    Inside Grafana data source configuration page, copy the host and user shown below and past them into the corresponding input boxes: `,
      // step4: "Use Grafana",
      // step4desc: `Please add new dashboard or import exist dashboard to explore the data stored in the ${ GRAFANA_GDS}.`,
      // step4desc1: "You can refer to the ",
      // step4desc2: "documentation",
      // step4desc3: "for more details.",
      monitortip: 'You can use Grafana to monitor the TDengine running status, please follow the steps below:',
      dashboarddesc: `We recommend using the latest<a href='https://grafana.com/'> Grafana</a> version 8 or 9 here.You can install Grafana on any<a href='https://grafana.com/docs/grafana/latest/setup-grafana/installation/#supported-operating-systems'> supported operating system</a> by following the <a href='https://grafana.com/docs/grafana/latest/setup-grafana/installation/'>official Grafana documentation Instructions </a>.`,

      step5: 'Add Dashboard',
      desc51: `1. Once the data source works, click the <code>Dashboards</code> tab on the data source configuration page.`,
      desc52: `2. Choose <code>TDinsight for 3.x</code> and click import.`,
      desc53: `3. Click the three horizontal bar icon, then <code>Dashboards</code>, search <code>TDinsight</code>, and click it.`,
      desc54: `4. Now, you can see the nice dashboard.`,
      tab2: 'Install Grafana on CentOS / RHEL',
      tab1: 'Installing Grafana on Debian / Ubuntu',
      tab2sub: 'Or install it with RPM package.',
      pluginname2: 'Set up TDinsight manually',
      pluginname1: 'Set up TDinsight automatically',
      plugin1: 'Install the latest version of the TDengine Data Source plugin from GitHub.',
      plugin2: `We provide an installation script <code>TDinsight.sh</code> to allow users to configure the installation automatically and quickly.<br/>

      You can download the script via wget or other tools:`,
      pluginsub2: `This script will automatically download the latest <a href='https://github.com/taosdata/grafanaplugin/releases/tag/v3.3.2'>Grafana TDengine data source plugin</a> and <a href='https://github.com/taosdata/grafanaplugin/blob/master/dashboards/TDinsightV3.json'>TDinsight dashboard</a> with configurable parameters for command-line options to the <a href='https://grafana.com/docs/grafana/latest/administration/provisioning/'>Grafana Provisioning</a> configuration file to automate deployment and updates, etc.With the alert setting options provided by this script, you can also get built-in support for AliCloud SMS alert notifications.`,

      logingrafana: `Open the default Grafana URL in a web browser:<code>http://localhost:3000</code>. The default username/password is  <code>admin</code>.Grafana will require a password change after the first login.`,

      nav: `Point to the <strong>Configurations -> Data Sources</strong> menu, and click the <strong>Add data source</strong> button.`,
      subsearch: 'Search for and select<strong> TDengine</strong>。',
      settingtd: `Configure the TDengine datasource. For e.g.<code>http://localhost:6041</code>.`,
      savetest: "Save and test. It will report 'TDengine Data source is working' under normal circumstances.",

      import: `In the page of configuring data source, click<strong> Dashboards</strong> tab.`,
      cont1: 'Choose <code>TDengine for 3.x</code>and click <code>import</code>.',
      cont2: `After the importing is done, <code>TDinsight for 3.x</code> dashboard is available on the page of <code>search dashboards by name</code>.`,
      cont3:
        'In the <code>TDinsight for 3.x</code> dashboard, choose the database used by taosKeeper to store monitoring data. ',
      cont4: 'You can see the monitoring result.'
    },
    tools: {
      is: ' is ',
      seeq: {
        desc: 'Designed specifically for analyzing process data, Seeq works across all verticals with time series data in historians or other storage platforms.',
        topdesc: 'Designed specifically for analyzing process data, ',
        topdesc1:
          ' works across all verticals with time series data in historians or other storage platforms. TDengine can be added as a data source into Seeq via JDBC connector. Once data source is configured, Seeq can read data from TDengine and offers functionalities such as data visualization, analysis, and forecasting.',
        step1: 'Prerequisite',
        step1desc: 'Install Seeq Server and Seeq Data Lab software (check ',
        step1desc1: ').',
        step2: 'Install TDengine Java Connector',
        step2desc: 'Get Seeq data location configuration. For Linux, execute the command below:',
        step2desc11: 'Download the latest TDengine Java connector from ',
        step2desc12: ' (current version is ',
        step2desc13: '), and copy the JAR file into the_directory_found_in_step_1/plugins/lib/ .',
        step2desc2: 'Restart Seeq server. For Linux, execute the command below:',
        step3: 'Add TDengine Data Source',
        step3full: "Add TDengine into Seeq's data source",
        step3desc: 'Open Seeq, login as admin, go to Administration, click "Add Data Source"',
        step3desc1: 'For connector, choose SQL connector v2',
        step3desc2: 'Inside the "Additional Configuration" input box, copy and paste the following:',
        step3desc3: 'For the "QueryDefintions", please follow the examples below to write your own.',
        step4: 'Smart Meter Example',
        step4full: 'Import a large number of time series: smart meter example',
        step4desc:
          'TDengine has its own unique data model. It requires creating a table for each data collection point by using a super table as its template. Each table can be associated with up to 128 labels (static attributes). A database may contain one million or even one billion tables. Through variables in Seeq, you can import all the time series (tables) under a super table into Seeq by querying a super table instead of an individual table. In addition, you can import the labels associated with tables stored inside TDengine into Seeq, so you can find a time series easily by searching those labels.',
        step4desc1:
          'Based on the classical smart meter example in the TDengine document, the following configuration can be used to retrieve all the time series under super table meters.',
        step4desc2: 'In the above example, tablename, location and groupid are retrieved via SQL: ',
        step4desc3:
          'The query results are assigned to variable tablename, location and groupid. Based on the query results, Seeq will expand this query configuration into many time series. ',
        step4desc4:
          'TDengine supports multiple columns, and you can use Seeq variables to generate a time series for each column. For more information about Seeq variables, please check ',
        step4desc41: 'Seeq documentation',
        step4desc42: '.'
      },
      powerbi: {
        desc: ' is a business analytics tool provided by Microsoft. With TDengine ODBC driver, PowerBI can access time series data stored in the TDengine. You can import tag data, original time series data, or aggregated data into Power BI from a TDengine, to create reports or dashboard without any coding effort.',
        step1: 'Prerequisite',
        step1full: 'Prerequisite',
        step1desc:
          'Power BI Desktop has been installed and running (If not, please download and install the latest Windows X64 version from ',
        step1desc1: 'PowerBI',
        step1desc2: ').',
        step1desc3: 'TDengine server software is installed and running.',
        step2: 'Install ODBC',
        step2full: 'Install ODBC connector',
        step3: 'Configure ODBC',
        step3full: 'Configure ODBC DataSource',
        step4: 'Import Data',
        step4full: 'Import Data from TDengine to Power BI',
        step4desc:
          'Open Power BI and logon, add data source following steps "Home" -> "Get data" -> "Other" -> "ODBC" -> "Connect".',
        step4desc1:
          'Choose the created data source name, such as "MyTDengine", then click "OK" button to open the "ODBC Driver" dialog. In the dialog, select "Default or Custom" left menu and then click "Connect" button to connect to the configured data source. After go to the "Nativator", browse tables of the selected database and load data.',
        step4desc2:
          'If you want to input some specific SQL, click "Advanced Options", and input your SQL in the open dialogue box and load the data.',
        step4desc3:
          'To better use Power BI to analyze the data stored in TDengine, you need to understand the concepts of dimension, metric, time series, correlation, and use your own SQL to import data:',
        step4desc4:
          "Dimension: it's normally category (text) data to describe such information as device, collection point, model. In the supertable template of TDengine, we use tag columns to store the dimension information. You can use SQL like select distinct tbname, tag1, tag2 from supertable to get dimensions.",
        step4desc5:
          'Metric: quantitive (numeric) fileds that can be calculated, like SUM, AVERAGE, MINIMUM. If the collecting frequency is 1 second, then there are 31,536,000 records in one year, it will be too low efficient to import so big data into Power BI. In TDengine, you can use data partition query, window partition query, in combination with pseudo columns related to window, to import downsampled data into Power BI. For more details, please refer to ',
        step4desc6: 'TDengine Specialized Queries',
        step4desc7: '.',
        step4desc8:
          'Window partition query: for example, thermal meters collect one data per second, but you need to query the average temperature every 10 minutes, you can use window subclause to get the downsampling data you need. The corresponding SQL is like select tbname, _wstart date，avg(temperature) temp from table interval(10m), in which _wstart is a pseudo column indicating the start time of a window, 10m is the duration of the window, avg(temperature) indicates the aggregate value inside a window.',
        step4desc9:
          'Data partition query: If you want to get the aggregate value of a lot of thermal meters, you can first partition the data and then perform a series of calculation in the partitioned data spaces. The SQL you need to use is partitoned by part_list. The most common of data partition usage is that when querying a supertable, you can partition data by subtable according to tags to form the data of each subtable into a single time serie to facilitate analytical processing of time series data.',
        step4desc10:
          'Time Serie: When curve plotting or aggregating data based on time lines, date is normally required. Data or time can be imported from Excel, or retrieved from TDengine using SQL statement like select _wstart date, count(*) cnt from test.meters where ts between A and B interval(1d) fill(0), in which the fill() subclause indicates the fill mode when there is data missing, pseudo column _wstart indicates the date to retrieve.',
        step4desc11:
          'Correlation: Indicates how to correlate data. Dimensions and Metrics can be correlated by tbname, dates and metrics can be correlated by date. All these can cooperate to form visual reports.',
        step5: 'Example',
        step5full: 'Example - Meters',
        step5desc:
          'TDengine has its own specific data model, which uses supertable as template and creates a specific table for each device. Each table can have maximum 4,096 data columns and 128 tags. In ',
        step5desc0:
          ', assume each meter generates one record per second, then there will be 86,400 records each day and 31,536,000 records every year, then only 1,000 meters will occupy 500GB disk space. So, the common usage of Power BI should be mapping tags to dimension columns, mapping the aggregation of data columns to metric columns, to provide indicators for decision makers.',
        step5desc1:
          'Import Dimensions: Import the tags of tables in PowerBI, and name as "tags", the SQL is as the following:',
        step5desc2:
          'Import Metrics: In Power BI, import the average current, average voltage, average phase with 1 hour window, and name it as "data", the SQL is as the following:',
        step5desc3:
          'Correlate Dimensions and Metrics: In Power BI, open model view, correlate "tags" and "data", and set "tabname" as the correlation column, then you can use the data in histogram, pie chart, etc. For more information about building visual reports in PowerBI, please refer to ',
        step5desc4: 'Power BI',
        step5desc5: '.'
      },
      yonghongbi: {
        name: 'Yonghong BI',
        desc: 'Yonghong one-stop big data BI platform',
        desc1:
          ' to provide enterprises of all sizes with flexible and easy-to-use whole-business chain big data analysis solutions, so that every user can use this platform to easily discover the value of big data and obtain deep insight. TDengine can be added to Yonghong BI as a data source via a JDBC connector. Once the data source is configured, Yonghong BI can read data from TDengine and provide functions such as data presentation, analysis and prediction.',
        step1: 'Prerequisite',
        step11desc: 'Yonghong Desktop Basic is installed and running (if not,please go to',
        step11desc1: ' official download page of Yonghong Technology',
        step11desc2: ' download).',
        step12desc:
          'The TDengine is installed and running, and ensure that the taosadapter service is started on the TDengine server side.',
        step2: 'Install JDBC',
        step2full: 'Install JDBC Connector',
        // Download the latest TDengine JDBC connector from maven.org
        step2desc: 'Go to ',
        step2desc1: ' download the latest TDengine JDBC connector (current version ',
        step2desc2: ') and install it on the machine where the BI tool is running.',
        step3: 'Configure JDBC',
        step3full: 'Configure JDBC DataSource',
        step31desc:
          'In the Yonghong Desktop BI tool, click "Add data source" and select the "GENERIC" type in the SQL data source.',
        step32desc:
          'Click "Select Custom Driver", in the "Driver Management" dialog box, click "+" next to "Driver List", enter the name "MyTDengine". Then click the "upload file" button to upload just download TDengine JDBC connector file "taos - jdbcdriver - 3.2.7 - dist. Jar", and select "com. Taosdata. JDBC. Rs. RestfulDriver" drive, Finally, click the "OK" button to complete the driver addition.',
        step33desc: 'Then copy the following into the "URL" field.',
        step34desc: 'Then select "No identity Authentication" under "Authentication Mode".',
        step35desc:
          'In the advanced Settings of the data source, change the value of the Quote symbol to the backquote "`".',
        step36desc:
          'Click "Test connection" and the dialog box "Test success" will pop up. Click the "Save" button and enter "tdengine" to save the TDengine data source.',
        step4: 'Create data set',
        step4full: 'Create TDengine datasets',
        step41desc:
          'Click "Add Data Set" in the BI tool, expand the data source you just created, and browse the super table in TDengine.',
        step42desc:
          'You can load all the data of the super table into the BI tool, or you can import some data through custom SQL statements.',
        step43desc:
          'When "Computation in Database" is selected, the BI tool will no longer cache TDengine timing data and will send SQL requests to TDengine for direct processing when processing queries.',
        step44desc:
          'When data is imported, the BI tool automatically sets the numeric type to the "metric" column and the text type to the "dimension" column. In TDengine super tables, ordinary columns are used as data metrics and label columns are used as data dimensions, so you may need to change the properties of some columns when you create a dataset. On the basis of supporting standard SQL, TDengine also provides a series of special query syntax to meet the requirements of time series business scenarios, such as data segmentation query, window segmentation query, etc., for',
        step44desc1: ' TDengine Specialized Queries',
        step44desc2:
          '.By using these featured queries, BI tools can greatly improve data access speed and reduce network transmission bandwidth when they send SQL queries to TDengine databases. ',
        step45desc:
          'In BI tools, you can create "parameters" and use them in SQL statements, which can be dynamically executed manually and periodically to achieve a visual report refresh effect.The following SQL statement: ',
        step45desc0: 'Data can be read in real time from TDengine, where: ',
        step45desc1: ': Indicates the start time of the time window.',
        step45desc2: ': Indicates the aggregate value in the time window.',
        step45desc3:
          ': Indicates that the parameter interval is introduced into the SQL statement. When the BI tool queries data, it assigns a value to the parameter interval. If the value is 1m, the sampling data is reduced based on a 1-minute time window.',
        step45desc4:
          ': This parameter is used to specify the name of the data table to be queried. When the ID of a drop-down parameter component is set as metric in the BI tool, the selected items of the drop-down parameter component are bound to this parameter to achieve dynamic selection.',
        step45desc5:
          ': These two parameters are used to represent the time range of the query data set and can be bound with the Text Parameter Component.',
        step45desc6:
          'You can modify the data type, data range, and default values of parameters in the "Edit Parameters" dialog box of the BI tool, and dynamically set the values of these parameters in the "Visual Report". ',
        step5: 'Make a report',
        step5full: 'Create a visual report',
        step51desc: 'Click "Make Report" in Yonghong BI tool to create a canvas.',
        step52desc: 'Drag visual components, such as Table Components, onto the canvas.',
        step53desc:
          'Select the data set to be bound in the Data Set sidebar, and bind Dimensions and Measures in the data column to Table Components as needed.',
        step54desc: 'Click "Save" to view the report.',
        step55desc: 'For more information about Yonghong BI tools, please consult them ',
        step55desc1: ' Help document',
        step55desc2: ' .'
      },
      superset: {
        name: 'Superset',
        desc: 'a modern enterprise level business intelligence (BI) web application primarily used for data exploration and visualization. It is supported by the Apache Software Foundation and is an open source project with an active community and rich ecosystem. Apache Superset provides an intuitive user interface that makes creating, sharing, and visualizing data simple, while supporting multiple data sources and rich visualization options.',
        topdesc: '',
        topdesc1: ' Through the Python connector of TDengine, Superset can support TDengine data sources and provide functions such as data presentation and analysis.',
        step1: 'Prerequisite',
        step1full: 'Prerequisite',
        step1desc: 'Apache Superset version 2.1.0 or above is already installed, If not installed, please download and install it, for specific instructions, please refer to ',
        step1desc1: 'Superset official documentation',
        step1desc2: '.',
        step2: 'Install TDengine Python Connector',
        step2full: 'Install TDengine Python Connector',
        step2desc: 'The Python connector of TDengine comes with a connection driver that supports Superset in versions 2.1.18 and later, which will be automatically installed in the Superset directory and provide data source services.',
        step2desc1: 'The connection uses the WebSocket protocol, so it is necessary to install the `taos-ws-py`(version >= 0.3.8) component of TDengine separately. The complete installation script is as follows: ',
        step3: 'Configure TDengine Data Source',
        step3full: 'Configure TDengine Data Source',
        step3desc: 'After starting the Superset service, access the service address (e.g., http://localhost:8088) in a browser and log in, for specific instructions, please refer to ',
        step31desc1: 'Superset install documentation',
        step31desc2: '.',
        step32desc1: 'On the Superset browser page, click "Setting" → "Database Connections" → "+DATABASE" on the right side. (If the "TDengine" option is not available in the dropdown list, please confirm the installation order and ensure that Superset is installed before the TDengine Python connector.)',
        step33desc: 'In the popped-up "Connect a database" dialog box, fill in the following necessary information:',
        step33desc1: '[Display Name]:',
        step33desc2: 'Data Source Name, required field, such as "MyTDengine"',
        step33desc3: '[SQLAlchemy URI]:',
        step34desc: 'Click "TEST CONNECTION" to test if the connection can be successful. After passing the test, click the "CONNECT" button to complete the connection.',
        step4: 'Import Data',
        step4full: 'Import Data from TDengine to Superset',
        step4desc: 'There is no difference in the use of TDengine data source compared to other data sources. Here is a brief introduction to basic data queries:',
        step4desc1: 'On the Superset Web page, click the “+” button in the upper-right corner, select “SQL query”, and then enter the query page.',
        step4desc2: 'On the query page, select the previously created data source, such as "MyTDengine", from the "DATABASE" dropdown list in the upper-left corner.',
        step4desc3: 'Select the name of the database to be operated on from the drop-down list of "SCHEMA" (system libraries are not displayed).',
        step4desc4: '"SEE TABLE SCHEMA" select the name of the super table or regular table to be operated on (sub tables are not displayed).',
        step4desc5: 'In the SQL editor area above, you can enter SQL statements conforming to TDengine syntax, and then click the "Run" button to execute them.',
        step4desc6: 'After clicking the "v" button next to the "Sava" button in the SQL editor area above, select the "Sava dataset" button to save.',
        step5: 'Example',
        step5full: 'Data Analysis',
        step5desc1: 'Click on the "Datasets" menu on the Superset web page to open the "Datasets" page.',
        step5desc2: 'Click on the saved Dataset on the "Dataset" page to open the "Chart" page.',
        step5desc3: 'Select the horizontal and vertical coordinate fields in the second column on the left side of the "Chart" page.',
        step5desc4: 'After selecting, click "Update CHART" and the chart will be generated.',
        step5desc5: 'For more information about the Superset tool, please refer to the',
        step5desc6: ' Superset documentation',
        step5desc7: '.'
      },
      tableau: {
        name: 'Tableau',
        desc: 'a well-known business intelligence tool that supports multiple data sources, making it easy to connect, import, and integrate data. And through an intuitive user interface, users can create rich and diverse visual charts, with powerful analysis and filtering functions, providing strong support for data decision-making. Users can import tag data, raw time-series data, or time-series data aggregated over time from TDengine into Tableau via the TDengine ODBC Connector to create reports or dashboards, and no code writing is required throughout the entire process.',
        step1: 'Prerequisite',
        step1full: 'Prerequisite',
        step1desc: 'Tableau Desktop has been installed and running. You can download and install the latest version for Windows X64 from ',
        step1desc1: 'Tableau',
        step1desc2: '.',
        step2: 'Install ODBC',
        step2full: 'Install ODBC Connector',
        step3: 'Configure ODBC',
        step3full: 'Configure ODBC DataSource',
        step23desc1: 'the default database to access, required field, such as "test"',
        step4: 'Import Data',
        step4full: 'Import Data from TDengine to Tableau',
        step4desc: 'Start Tableau in the Windows system environment, then search for "ODBC" on its connection page and select "Other Databases (ODBC)".',
        step4desc1:
          'Click the "DSN" radio button, then select the configured data source (such as MyTDengine), and click the Connect button. After the connection is successful, delete the content of the string attachment, and finally click the Sign In button.',
        step4desc2: 'On the workbook page, select the connected data source, then click on the database dropdown list and choose the database that requires data analysis.',
        step4desc3:
          'Click the "Find" button in the table options, and all the tables in the database will be displayed. Drag the table you need to analyze to the right - hand area, and the table structure will be shown.',
        step4desc4: 'Click the "Update Now" button below, and the data in the table will be displayed.',
        step5: 'Example',
        step5full: 'Data Analysis',
        step5desc1: 'On the workbook page, click "Worksheet", and the "Data Analysis" page will pop up.',
        step5desc2: 'All the fields of the table will be displayed in the sidebar of the "Data Analysis" page.',
        step5desc3: 'Drag the fields classified as "Dimensions" and "Measures" onto the "Table Component" in the right - hand rows and columns, and a chart will be displayed below.',
        step5desc4: 'For more information about the Tableau tool, please refer to the',
        step5desc5: ' Tableau documentation',
        step5desc6: '.'
      },
      excel: {
        name: 'Excel',
        desc: 'a powerful and widely-used spreadsheet software developed by Microsoft Corporation. By configuring the use of the ODBC connector, Excel can quickly access data from TDengine. Users can import tag data, raw time-series data, or time-aggregated time series data from TDengine into Excel to create reports or dashboards, all without the need for any coding.',
        step1: 'Prerequisite',
        step1full: 'Prerequisite',
        step1desc: 'Excel has been installed and running, If not installed, please download and install it, for specific instructions, please refer to ',
        step1desc1: 'Microsoft\'s official documentation',
        step1desc2: '.',
        step2: 'Install ODBC',
        step2full: 'Install ODBC Connector',
        step3: 'Configure ODBC',
        step3full: 'Configure ODBC DataSource',
        step4: 'Import Data',
        step4full: 'Import Data from TDengine to Excel',
        step4desc: 'Start Excel in the Windows system environment, then select "Data" -> "Get Data" -> "From Other Sources" -> "From ODBC".',
        step4desc1: 'In the pop-up window, select the data source you need to connect to from the drop-down list of "Data source name (DSN)", and then click the "OK" button.',
        step4desc2: 'In the popped-up "ODBC Driver" window, select the "Default or Custom" menu and then click the "Connect" button.',
        step4desc3: 'In the pop-up "Navigator" dialog box, select the database tables you want to load, and then click "Load" to complete the data loading.',
        step5: 'Example',
        step5full: 'Data Analysis',
        step5desc1: 'In the Excel worksheet where data has been imported, select the desired data range.',
        step5desc2: 'In the Excel menu bar, find and click the "Insert" tab, then select the desired chart type.',
        step5desc3: 'Excel will immediately generate a chart based on the selected data in the worksheet.',
        step5desc4: 'For more information about the Excel, please refer to the',
        step5desc5: ' Excel documentation',
        step5desc6: '.'
      },
      nodered: {
        desc: 'Node-RED is a powerful low-code visual programming tool for IoT.',
        brief1: 'is an open-source visual programming tool developed by IBM based on Node.js. It enables users to assemble and connect various nodes via a graphical interface to create connections for IoT devices, APIs, and online services. It supports multiple protocols and is cross-platform, has an active community, and is ideal for event-driven application development in smart home, industrial automation, and other scenarios. Its main strengths are low-code and visual programming.',
        brief2: 'The deep integration between TDengine and Node-RED provides a comprehensive solution for industrial IoT scenarios. Node-RED MQTT/OPC UA/Modbus protocol nodes​ enable ​millisecond-latency data collection​ from PLCs, sensors, and other devices. Real-time queries in TDengine can trigger physical control actions such as relay operations and valve switching, enabling immediate command execution.',
        brief3: 'node-red-node-tdengine is the official plugin developed by TDengine for Node-RED. It is composed of two nodes:',
        briefitem1: 'tdengine-operator: Provides SQL execution capabilities for data writing/querying and metadata management.',
        briefitem2: 'tdengine-consumer: Offers data subscription and consumption capabilities from specified subscription servers and topics.',

        endmark: '.',

        step1: 'Prerequisites',
        step1pre1: 'Prepare the following environment components:',

        step1item1: 'Node-RED version >=3.0.0,',
        step1item2: 'Node.js versoin >=3.1.8,',
        step1item3: 'node-red-node-tdengine latest version, ',
        step11link1: 'Node-RED setup',
        step12link1: 'npmjs.com setup',
        step13link1: 'npmjs.com setup',

        step2: 'Configuring Data Source',
        step2pre1: 'Plugin data sources are configured in the node properties using the Node.js connector:',

        step21: 'Start Node-RED service and access the Node-RED homepage in a browser.',
        step22: 'Drag the tdengine-operator or tdengine-consumer node from the left node palette to the workspace canvas.',
        step23: 'Double-click the selected node on the canvas. In the ​Database Connection URI​ field that opens, enter the following content:',
        step24: 'After configuration, click the "Deploy" button in the upper right. Green node status indicates successful connection.',

        step3: 'Usage Examples',

        step31: 'Scenario Preparation',

        step311: 'Scenario Overview',
        step311pre1: 'In a production workshop with multiple smart meters, where each meter generates one data record per second to be stored in the TDengine database, it is required to real-time output the average current, voltage, and power consumption of each smart meter per minute, and simultaneously alarm for equipment with excessive load when current > 25A or voltage > 230V.',
        step311pre2: 'Implementation uses Node-RED + TDengine:  ',
        step311item1: 'Inject + function nodes simulate devices. ',
        step311item2: 'tdengine-operator writes data.',
        step311item3: 'Real-time queries via tdengine-operator.',
        step311item4: 'Overload alerts via tdengine-consumer subscription.',
        step311sec1: 'Assumptions:',
        step311secitem1: 'TDengine: already has a cloud service account',
        step311secitem2: 'Simulated devices: d0, d1, d2.',

        step312: 'Data Modeling',
        step312pre1: 'Use taos-CLI to manually create the data model:',
        step312item1: 'Super table "meters". ',
        step312item2: 'Child tables d0, d1, d2. ',
        step312pre2: 'SQL:',

        step32: 'Business Processing',

        step321: 'Data Collection',
        step321pre1: 'This example uses randomly generated numbers to simulate real device data. The tdengine-operator node is configured with TDengine data source connection information, writes data to TDengine, and uses the debug node to monitor the number of successfully written records displayed on the interface.',
        step321pre2: 'Steps',

        step3211: '- Add Writer Node',
        step3211item1: 'Select the tdengine-operator node in the node palette and drag it to the canvas.',
        step3211item2: 'Double-click the node to open property settings, fill in the name as "td-writer", and click the "+" icon to the right of the database field.',
        step3211item3: 'In the pop-up window, fill in the name "db server", select the connection type to use string connection, and enter the following content:',
        step3211item4: 'Click "Add" and return.',

        step3212: '- Simulate Device Data',
        step3212item1: 'Select the "function" node from the palette and drag it before "td-writer" on the canvas.',
        step3212item2: 'Double click the node to open the property settings, fill in the name "write d0", select the "run function" tab below, fill in the following content, save and return to the canvas.',
        step3212item3: 'Drag an "inject" node before "write d0".',
        step3212item4: 'Configure the inject node: Name: “inject1”, Trigger: "Repeat", Interval: 1 second.',
        step3212item5: 'Repeat steps 1-4 for other devices (d1, d2).',

        step3213: '- Add Output Monitor',
        step3213item1: 'Drag a "debug" node after "td-writer".',
        step3213item2: 'Configure it, node status set checked and select "message count" from the drop-down list.',

        step321secpre1: 'After adding all nodes, connect them in sequence to form a pipeline. ',
        step321secpre2: 'Click "Deploy" to publish changes. When running successfully:',
        step321secitem1: '"td-writer" turns green.',
        step321secitem2: '"debug1" shows data count.',
        step321secpre3: 'Successful write output (exceptions thrown on failure):',

        step322: 'Data Query',
        step322pre1: 'The data query workflow consists of three nodes (inject/tdengine-operator/debug) designed to calculate the average current, voltage, and power consumption per minute for each smart meter. The inject node triggers the query request every minute. The results are sent to the downstream debug node, which displays the count of successful query executions.',
        step322pre2: 'Steps:',
        step322item1: 'Drag an inject node to the canvas, set name to "query", set msg.topic:',
        step322item2: 'Drag the tdengine-operator node onto the canvas, double-click the node to set its properties, select the previously created data source "db-server" for the "Database" field, save the settings, and return to the canvas.',
        step322item3: 'Drag the debug node onto the canvas, double-click the node to set its properties, checked the "Node status", select "message count" from the dropdown list, save the settings, and return to the canvas.',
        step322item4: 'Connect nodes sequentially → Click "Deploy".',
        step322pre3: 'When the flow is successfully started:',
        step322secitem1: '"td-reader" turns green.',
        step322secitem2: 'Debug node shows result count.',
        step322pre4: 'Output from "td-reader" (exceptions thrown on failure):',

        step323: 'Data Subscription',
        step323pre1: 'The data subscription workflow consists of two nodes (tdengine-consumer/debug) that provide equipment overload alert functionality.The debug node visually displays the count of subscription messages pushed downstream. In production, replace it with functional nodes to process the subscription data.',
        step323pre2: 'Steps',
        step323item1: 'Manually create a subscription topic “topic_overload” using taos-CLI:',
        step323item2: 'Drag tdengine-consumer node to canvas, double click node and set:',
        step323item2opt1: 'Name: td-consumer',
        step323item2opt2: 'Subscription Server(URI):',
        step323item2opt3: 'User: not filled in',
        step323item2opt4: 'Password: not filled in',
        step323item2opt5: 'Topics: topic_overload',
        step323item2opt6: 'Offset reset: latest',
        step323item2opt7: 'Other settings: default.',
        step323item3: 'Drag debug node to canvas and configure it: name: "debug3", node status: checked, select "message count" from the drop-down list.',
        step323item4: 'Connect nodes sequentially → Click "Deploy".',
        step323pre3: 'After the process is successfully started, you can see that the td consumer node status changes to "green" to indicate that the process is working properly, and the debug node number represents the number of consumption times.',
        step323pre4: 'If the overload device warning message pushed to downstream nodes fails, an exception will be thrown:',

        step33: 'Error Handling',
        step33pre1: 'Errors in data collection, querying, and subscription workflows are routed to catch nodes for handling in Node-RED. To implement error monitoring:',
        step33item1: 'Drag a "catch" node to the canvas.  ',
        step33item2: 'Configure the node attributes, name: "catch all except"，scope: "All nodes".',
        step33item3: 'Drag debug node to canvas.',
        step33item4: 'Configure it, name: "debug4", node status: checked, select "message count" from the drop-down list.',
        step33item5: 'Connect nodes by order and deploy. ',
        step33pre2: 'When errors occur: ',
        step33secitem1: 'Debug node shows error count.',
        step33secitem2: 'View details in Node-RED logs. ',

        step4: 'Summary',
        step4pre1: 'This article demonstrates, through an industrial monitoring scenario:',
        step4item1: 'Three integration patterns between Node-RED and TDengine:',
        step4item1opt1: 'Data collection (tdengine-operator writes).',
        step4item1opt2: 'Real-time queries (tdengine-operator queries).',
        step4item1opt3: 'Event-driven architecture (tdengine-consumer subscriptions).',
        step4item2: 'Complete error handling mechanisms.',
        step4item3: 'Production-ready deployment reference architecture.',

        docend: 'This article focuses on an example-based introduction. For complete documentation, refer to online document for the Node-RED node.'
      }
    },
    connectorTip: `Use the programming language of your choice to <a target='_blank' href='${$IS_COMMUNITY ? 'https://docs.tdengine.com' : '/docs-en'}/taos-sql/select/'>query data using SQL</a>`,
    docConfig: {
      title: 'Config',
      content: 'Run this command in your terminal to save TDengine  {0} as variables:',
      url: 'URL and token',
      dsn: 'DSN connection string',
      tmq: 'TMQ connection string',
      endpoint: 'endpoint and token',
      bottom: "Alternatively, you can also set environment variables in your IDE's run configurations."
    }
  }
};
