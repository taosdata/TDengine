install build environment
===
yum/apt install mono-complete

build C# version taosdemo
===
mcs -out:taosdemo *.cs

run C# version taosdemo
===
Usage: mono taosdemo.exe [OPTION...]

        --help            Show usage.

        -h                host, The host to connect to TDengine. Default is localhost.
        -p                port, The TCP/IP port number to use for the connection. Default is 0.
        -u                user, The user name to use when connecting to the server. Default is 'root'.
        -P                password, The password to use when connecting to the server. Default is 'taosdata'.
        -d                database, Destination database. Default is 'test'.
        -a                replica, Set the replica parameters of the database, Default 1, min: 1, max: 5.
        -m                table_prefix, Table prefix name. Default is 't'.
        -M                stable, Use super table.
        -s                stable_prefix, STable prefix name. Default is 'st'
        -T                num_of_threads, The number of threads. Default is 10.
        -r                num_of_records_per_req, The number of records per request. Default is 1000.
        -t                num_of_tables, The number of tables. Default is 1.
        -n                num_of_records_per_table, The number of records per table. Default is 1.
        -c                config_directory, Configuration directory. Default is '/etc/taos/'.
        -x                flag, Insert only flag.
        -O                order, Insert mode--0: In order, 1: Out of order. Default is in order.
        -R                rate, Out of order data's rate--if order=1 Default 10, min: 0, max: 50.
        -D                Delete data methods 0: don't delete, 1: delete by table, 2: delete by stable, 3: delete by database.
        -v                Print verbose output
        -y                Skip read key for continous test, default is not skip
