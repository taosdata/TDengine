install build environment
===
/usr/bin/python3 -m pip install -r requirements.txt

run python version taosdemo
===
Usage: ./taosdemo.py [OPTION...]

Author: Shuduo Sang <sangshuduo@gmail.com>

	-H, --help                        Show usage.

	-N, --native                      flag, Use native interface if set. Default is using RESTful interface.
	-h, --host <hostname>             host, The host to connect to TDengine. Default is localhost.
	-p, --port <port>                 port, The TCP/IP port number to use for the connection. Default is 0.
	-u, --user <username>             user, The user name to use when connecting to the server. Default is 'root'.
	-P, --password <password>         password, The password to use when connecting to the server. Default is 'taosdata'.
	-l, --colsPerRec <number>         num_of_columns_per_record, The number of columns per record. Default is 3.
	-d, --dbname <dbname>             database, Destination database. Default is 'test'.
	-a, --replica <replications>      replica, Set the replica parameters of the database, Default 1, min: 1, max: 5.
	-m, --tbname <table prefix>       table_prefix, Table prefix name. Default is 't'.
	-M, --stable                      flag, Use super table. Default is no
	-s, --stbname <stable prefix>     stable_prefix, STable prefix name. Default is 'st'
	-Q, --query <DEFAULT | NO | command>   query, Execute query command. set 'DEFAULT' means select * from each table
	-T, --threads <number>            num_of_threads, The number of threads. Default is 1.
	-C, --processes <number>          num_of_processes, The number of threads. Default is 1.
	-r, --batch <number>              num_of_records_per_req, The number of records per request. Default is 1000.
	-t, --numOfTb <number>            num_of_tables, The number of tables. Default is 1.
	-n, --numOfRec <number>           num_of_records_per_table, The number of records per table. Default is 1.
	-c, --config <path>               config_directory, Configuration directory. Default is '/etc/taos/'.
	-x, --inserOnly                   flag, Insert only flag.
	-O, --outOfOrder                  out of order data insert, 0: In order, 1: Out of order. Default is in order.
	-R, --rateOOOO <number>           rate, Out of order data's rate--if order=1 Default 10, min: 0, max: 50.
	-D, --deleteMethod <number>       Delete data methods 0: don't delete, 1: delete by table, 2: delete by stable, 3: delete by database.
	-v, --verbose                     Print verbose output
	-g, --debug                       Print debug output
	-y, --skipPrompt                  Skip read key for continous test, default is not skip

