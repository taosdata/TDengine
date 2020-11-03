# clear env set up

create database db update 1;

## Step 1
$loops = 1000
#t0 = 1604298064000

create table t1 (ts timestamp, a int);
for ($i = 0; $i < $loops; $i++) {
  insert into t1 values ($t0 + $i, 1);
  restart to commit
  check query result
}

## Step 2
create table t2 (ts timestamp, a int);

insert into t2 ($t0, 1);
restart to commit
check query result

for ($i = 1; $i <= 150; $i++) {
  insert into t2 values ($t0 + $i, 1);
}
restart to commit
check query result

## Step 3
create table t3 (ts timestamp, a int);

insert into t3 ($t0, 1);
restart to commit
check query result

for ($i = 0; $i < 8; $i++) {
  for ($j = 1; $j <= 10; $j++) {
    insert into t3 values ($t0 + $i * 10 + $j , 1);
  }
}
restart to commit
check query result