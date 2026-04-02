import taosws


def test_meters():
    conn = taosws.connect()
    cursor = conn.cursor()
    statements = [
        "drop topic if exists test",
        "drop database if exists test",
        "create database test wal_retention_period 3600",
        "use test",
        "create table meters(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(16),\
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
            tags(t1 int)",
        "create table tb0 using meters tags(1000)",
        "create table tb1 using meters tags(NULL)",
        """insert into tb0 values(now, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL)
            tb1 values(now, true, -2, -3, -4, -5, 
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',
            254, 65534, 1, 1)""",
    ]
    for statement in statements:
        cursor.execute(statement)

    cursor.execute("select * from test.meters")

    # PEP-249 fetchone() method
    row = cursor.fetchone()

    # PEP-249 fetchmany([size = Cursor.arraysize]) method()
    # 1. fetch one block by default, the block size is not predictable.
    many_dynamic = cursor.fetchmany()
    # 2. fetch exact (maximum) size of rows, the result may be less than the size limit.
    many_fixed = cursor.fetchmany(10000)

    cursor.execute("select * from test.meters")
    # all rows in a sequence of tuple
    all = cursor.fetchall()

    # by dict
    cursor.execute("select * from test.meters limit 1")
    all_dict = cursor.fetch_all_into_dict()  # same to cursor.fetchallintodict()
    # [{'ts': datetime.datetime(2017, 7, 14, 2, 40, tzinfo=datetime.timezone(datetime.timedelta(seconds=28800))),
    #  'current': 9.880000114440918,
    #  'voltage': 114,
    #  'phase': 0.3027780055999756,
    #  'groupid': 8,
    #  'location': 'California.Campbell'}]

    # by iterator.

    # by fetchone loop
    cursor.execute("select * from test.meters")
    while True:
        row = cursor.fetchone()
        if row:
            print(row)
        else:
            break

    cursor.execute("select * from test.meters")
    results = cursor.fetchall()
    for row in results:
        print(row)

    cursor.execute("drop database if exists test")
