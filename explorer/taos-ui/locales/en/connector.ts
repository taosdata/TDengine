export default {
  desc: 'Connect using the {0} to encapsulate SQL as a REST request.',
  bottom1: 'The client connection is then established.',
  bottom2: 'For how to write data and query data, please refer to ',
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
      'For more details about how to write or query data from TDngine Cloud instance through Spring, please refer to',
    step3desc:
      'In the following code example, get the JDBC URL from environment variable "TDENGINE_JDBC_URL" first and then create a Connection object, which is a standard JDBC Connection object.'
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
      'In order for Jupyter to connect to TDengine Cloud instance, before launching Jupypter, the environment setting must be performed. We use Linux bash as example.',
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
    step31desc: 'First of all, please load JDBC driver as the following:',
    step32desc: 'Then execute the following script to create the connection with TDengine Cloud instance:'
  },
  rest: {
    desc: 'In this section we will explain how to write into TDengine Cloud instance using REST API',
    step1: 'Config',
    step2: 'Insert',
    step2desc:
      'Following command below show how to insert data into the table d1001 of the database test via the command line utility curl: ',
    step3: 'Query',
    step3desc:
      'Following command below show how to query data into from table ins_databases of the database information_schema via the command line utility curl.'
  },
  odbc: {
    desc: 'The TDengine ODBC driver is a driver specifically designed for TDengine based on the ODBC standard. It can be used by ODBC based applications, like ',
    desc1: ', on Windows, to access an instance in the TDengine Cloud service.',
    desc2:
      'TDengine ODBC provides two kinds of connections, native connection and WebSocket connection. But you must use WebSocket to access an instance in the TDengine Cloud service.',
    step1: 'Install',
    step1full: 'Install ODBC Connector',
    step11desc1:
      'TDengine ODBC driver only supports the Windows platform. To run on Windows, the Microsoft Visual C++ Runtime library is required. If the Microsoft Visual C++ Runtime Library is missing on your platform, you can download and install it from ',
    step11desc2: 'VC Runtime Library',
    step11desc3: '. If already installed, please ignore this step.',
    step12desc1: 'Install ',
    step12desc2: 'TDengine Windows client installation package',
    step12desc3:
      ' . The client package includes both the TDengine ODBC driver and some other necessary libraries that will be used in either native connection or WebSocket connection.',
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
    step24desc:
      'Click "Test Connection" to test whether the connection to the data source is successful; if successful, it will prompt "Successfully connected to {0}".',
    step3: 'Example',
    step31desc:
      'As an example, you can use Power BI, which invokes TDengine ODBC driver, to access an instance in the TDengine Cloud service, please refer to the "Power BI" page under the "Tools" menu for more details.'
  }
};
