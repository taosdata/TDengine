install build environment
===
/usr/bin/python3 -m pip install -r requirements.txt

run python version taosdemo
===
Usage: ./taosdemo.py [OPTION...]

        --help                   Show usage.

        -h <hostname>            host, The host to connect to TDengine. Default is localhost.
        -p <port>                port, The TCP/IP port number to use for the connection. Default is 0.
        -u <username>            user, The user name to use when connecting to the server. Default is 'root'.
        -P <password>            password, The password to use when connecting to the server. Default is 'taosdata'.
        -d <dbname>              database, Destination database. Default is 'test'.
        -a <replications>        replica, Set the replica parameters of the database, Default 1, min: 1, max: 5.
        -m <table prefix>        table_prefix, Table prefix name. Default is 't'.
        -M                       stable, Use super table.
        -s <stable prefix>       stable_prefix, STable prefix name. Default is 'st'
        -Q <DEFAULT | command>   query, Execute query command. set 'DEFAULT' means select * from each table
        -T <number>              num_of_threads, The number of threads. Default is 10.
        -r <number>              num_of_records_per_req, The number of records per request. Default is 1000.
        -t <number>              num_of_tables, The number of tables. Default is 1.
        -n <number>              num_of_records_per_table, The number of records per table. Default is 1.
        -c <path>                config_directory, Configuration directory. Default is '/etc/taos/'.
        -x                       flag, Insert only flag.
        -O                       order, Insert mode--0: In order, 1: Out of order. Default is in order.
        -R <number>              rate, Out of order data's rate--if order=1 Default 10, min: 0, max: 50.
        -D <number>              Delete data methods 0: don't delete, 1: delete by table, 2: delete by stable, 3: delete by database.
        -v                       Print verbose output
        -g                       Print debug output
        -y                       Skip read key for continous test, default is not skip
