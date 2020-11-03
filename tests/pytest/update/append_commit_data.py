# clear env set up

create database db update 1
$t0 = 1604298064000

## Step 1
create table t1 (ts timestamp, a int);

for ($i = 0; $i < 200; $i++) {
  insert into t1 values ($t0 + $i, 1);
}

restart to commit
check query result

for ($k = 1; $k <= 100; $k++) {
  for ($i = 0; $i < 200; $i++) {
    insert into t1 values ($t0 + $k * 200 $i, 1);
  }

  restart to commit
  check query result
}

## Step 2
create table t2 (ts timestamp, a int);

for ($i = 0; $i < 20000; $i++) {
  insert into t2 values ($t0 + $i, 1);
}

restart to commit
check query result