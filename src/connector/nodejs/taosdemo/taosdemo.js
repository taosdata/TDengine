// const taosc = require('td2')
// const taoRest = require('restConnect')

const argNameMapping = {
  f: 'file',
  u: 'user',
  p: 'password',
  c: 'cfgDir',
  h: 'host',
  P: 'port',
  I: 'interface',
  d: 'database',
  a: 'replica',
  m: 'tablePrefix',
  s: 'sqlFile',
  N: 'normalTable',
  o: 'outFile',
  q: 'queryMode',
  b: 'dataType',
  w: 'width',
  T: 'numOfThread',
  i: 'insertInterval',
  S: 'timestampInterval',
  B: 'interlaceRow',
  r: 'recordPerReq',
  t: 'numOfTable',
  n: 'numOfRecord',
  x: 'noInsertFlag',
  y: 'answer',
  O: 'disorderRatio',
  R: 'disorderRange',
  g: 'debugFlag',
  V: 'version',
  M: 'random',
  l: 'numOfColumn'
}

function getArgName(argLable) {
  return argNameMapping[argLable]
}

const argValue = {
  file: './meta.jason',
  user: 'root',
  password: 'taosdata',
  cfgDir: '/ect/taos',
  host: '127.0.0.1',
  port: 0,
  interface: 'taosc',
  database: 'test',
  replica: 1,
  tablePrefix: 'd',
  sqlFile: null,
  normalTable: false,
  outFile: './output.txt',
  queryMode: 0,
  dataType: 'default FLOAT,INT,FLOAT',
  width: 64,
  numOfThread: 10,
  insertInterval: 0,
  timestampInterval: 1,
  interlaceRow: 0,
  recordPerReq: 30000,
  numOfTable: 10000,
  numOfRecord: 10000,
  random: false,
  noInsertFlag: false,
  answer: 'yes',
  disorderRatio: 0,
  disorderRange: 0,
  debugFlag: false,
  usage: "Give short usage message",
  version: "1.0.0.0",
  numOfColumn: 1,
}
function getArgValue(argName){
  return argValue[argName]
}
function setArgValue(name,value){
  argValue[name] = value
}

function printUsage() {
  console.log(" Usage: taosdemo [-f JSONFILE] [-u USER] [-p PASSWORD] [-c CONFIG_DIR]\n" +
    "                    [-h HOST] [-P PORT] [-I INTERFACE] [-d DATABASE] [-a REPLICA]\n" +
    "                    [-m TABLEPREFIX] [-s SQLFILE] [-N] [-o OUTPUTFILE] [-q QUERYMODE]\n" +
    "                    [-b DATATYPES] [-w WIDTH_OF_BINARY] [-l COLUMNS] [-T THREADNUMBER]\n" +
    "                    [-i SLEEPTIME] [-S TIME_STEP] [-B INTERLACE_ROWS] [-t TABLES]\n" +
    "                    [-n RECORDS] [-M] [-x] [-y] [-O ORDERMODE] [-R RANGE] [-a REPLIcA][-g]\n" +
    "                    [--help] [--usage] [--version]")
}

function printHelp() {
  console.log(printEmpty(10) + 'Usage: taosdemo [OPTION...]')
  console.log(printEmpty(10) + '--help show usage')
  console.log(printPretty('-f <FILE>', 'The meta file to the execution procedure. Default is ./meta.json.'))
  console.log(printPretty('-u <USER>', 'The user name to use when connecting to the server.'))
  console.log(printPretty('-p <PASSWORD>', 'The password to use when connecting to the server.'))
  console.log(printPretty('-c <CONFIG_DIR>', 'Configuration directory.'))
  console.log(printPretty('-h <HOST>', 'TDengine server FQDN to connect,default localhost.'))
  console.log(printPretty('-P <PORT>', 'The TCP/IP port number to use for the connection.'))
  console.log(printPretty('-I <INTERFACE>', 'The interface (taosc, rest, and stmt) taosdemo uses,default taosc'))
  console.log(printPretty('-d <DATABASE>', 'TDestination database,default is \'test\'.'))
  console.log(printPretty('-a <REPLICA>', 'Set the replica parameters of the database, default 1, min: 1, max: 3.'))
  console.log(printPretty('-m <TABLE-PREFIX>', 'Table prefix name,default is \'d\'.'))
  console.log(printPretty('-s <SQL-FILE>', 'The select sql file.'))
  console.log(printPretty('-N <NORMAL-TABLE>', 'Use normal table flag.'))
  console.log(printPretty('-o <OUTPUT-FILE>', 'Direct output to the named file,default is \'./output.txt\'.'))
  console.log(printPretty('-q <QUERY_MODE>', 'Query mode -- 0: SYNC, 1: ASYNC. Default is SYNC.'))
  console.log(printPretty('-b <DATATYPE>', 'The data_type of columns, default: FLOAT, INT, FLOAT.'))
  console.log(printPretty('-w <WIDTH>', 'The width of data_type \'BINARY\' or \'NCHAR\'. Default is 64'))
  console.log(printPretty('-T <THREADS>', 'he number of threads,default is 10.'))
  console.log(printPretty('-i <INTERVAL>', 'The sleep time (ms) between insertion,default is 0.'))
  console.log(printPretty('-S <TIME_STEP>', 'The timestamp step between insertion,default is 1.'))
  console.log(printPretty('-B <INTERLACE-ROWS>', 'The interlace rows of insertion,default is 0.'))
  console.log(printPretty('-r <REC-PER-REQ>', 'The number of records per request,default is 30000.'))
  console.log(printPretty('-t <TABLES>', 'The number of records per request,default is 30000.'))
  console.log(printPretty('-n <RECORDS>', 'The number of records per table. Default is 10000.'))
  console.log(printPretty('-x <NO-INSERT>', 'No-insert flag.'))
  console.log(printPretty('-y <ANSWER>', 'No-insert flag.'))
  console.log(printPretty('-O <DIS-RATIO>', 'Insert order mode--0: In order, 1 ~ 50: disorder ratio. Default is in order.'))
  console.log(printPretty('-R <DIS-RANGE>', 'Disorder data\'s range, ms, default is 1000.'))
  console.log(printPretty('-g <DEBUG>', 'Print debug info.'))
  console.log(printPretty('-? <HELP>', 'Give this help list.'))
  console.log(printPretty('-V <VERSION>', 'Print program version.'))
  console.log(printPretty('-M <RANDOM>', 'The value of records generated are totally random.The default is to simulate power equipment scenario.'))
  console.log(printPretty('-l <COLUMNS>', 'The number of columns per record. Demo mode by default is 1 (float, int, float). Max values is 4095'))
  console.log(printPretty("", 'All of the new column(s) type is INT. If use -b to specify column type, -l will be ignored.'))

}

function printEmpty(num) {
  let str = ""
  for (let i = 1; i <= num; i++) {
    str += " "
  }
  return str
}

function printPretty(str1, str2) {
  let length1 = str1.length
  let resStr = printEmpty(10) + str1 + printEmpty(30 - 10 - length1) + str2
  return resStr
}


function readAgument() {
  for (var i = 2; i < global.process.argv.length; i += 2) {
    let key = global.process.argv[i].slice(1);
    let value = global.process.argv[i + 1];
    console.log("key:" + key + " value:" + value)
    if (getArgName(key) === undefined) {
      console.log("-" + key + " <" + getArgName(key) + "> is undefined.")
      printUsage()
    } else if (value === undefined) {
      console.log("-" + key + " " + getArgName(key) + "'s value has not settled.")
    } else if (key === '--help') {
      printHelp()
    } else {
      argValue[getArgName(key)] = value
    }
  }
  //para()
}

function para() {
  for (arg in argNameMapping) {
    let line = ""

     line += printEmpty(10) + getArgName(arg) + printEmpty(30 -10 - getArgName(arg).length) + getArgValue(getArgName(arg))
      console.log(line)
  }
}

// }
// readAgument()
// printHelp()
para()


function getConn(interfaceType){
  if(interfaceType == 'taosc' ){
    console.log('taosc interface')
    // return taosc.connect({host:argValue["host"],port:argValue["port"],user:argValue["user"], password:argValue["password"], config:argValue["cfgDir"],})
  }else if(interfaceType == 'restful'){
    console.log('restful api')
    // return taoRest.connect({host:argValue["host"],port:argValue["port"],user:argValue["user"], password:argValue["password"]})
  }else{
    console.log('stmt is not supported now')
  }
}


function createDB(){
  let dropDB = "drop database if  exists " + argValue["database"]
  let createDB = "create database if not exists " + argValue["database"] + argValue["replica"]+"keep 36500";
  let conn = getConn(argValue["interface"])
  console.log("dropping database " + argValue["database"])
  //conn.cursor(dropDB)
  console.log("creating database " + argValue["database"])
  //conn.cursor(createDB)
}


function createTable(){
  let conn = getConn(argValue)
  let createStable = "create table if not exists meters"
}
function main() {
  //read argument init argument
  //main action model

}


function mainModel() {
  //if income value == help or --?
  //call printHelp()

  //if income value == insert
  //call create table()
  //call insert

  //if income value == query

}

