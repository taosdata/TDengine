# clear env set up

$t0 = 1603152000000

create database db update 1 days 30;

## Step 1. UPDATE TWO WHOLE DATA BLOCK REPEATEDLY
create table t1 (ts timestamp, a int);
for ($i = 0; $i < 200; $i++) {
  insert into t1 values ($t0 + $i, 1);
}

restart to commit
check query result

for ($i = 0; $i < 200; $i++) {
  insert into t1 values ($t0 + 5000 + $i, 1);
}

check query result
restart to commit
check query result

for ($i = 0; $i < 200; $i++) {
  insert into t1 values ($t0 + $i, 2);
}
for ($i = 0; $i < 200; $i++) {
  insert into t1 values ($t0 + 5000 + $i, 1);
}
check query result
restart to commit
check query result

## Step 2. INSERT IN MIDDLE LITTLE DATA REPEATEDLY
create table t2 (ts timestamp, a int);
for ($i = 0; $i < 200; $i++) {
  insert into t2 values ($t0 + $i, 1);
}
restart to commit
for ($i = 0; $i < 200; $i++) {
  insert into t2 values ($t0 + 5000 + $i, 1);
}
restart to commit

for ($k = 0; $k < 10; $k++) {
  for ($i = 0; $i < 10; $i++) {
    insert into t2 values ($t0 + 200 + $k * 10 + $i, 1);
  }
  check query result
  restart to commit
  check query result
}

## Step 3. INSERT IN MIDDLE AND UPDATE TWO BLOCKS
create table t3 (ts timestamp, a int);
for ($i = 0; $i < 200; $i++) {
  insert into t3 values ($t0 + $i, 1);
}
restart to commit
for ($i = 0; $i < 200; $i++) {
  insert into t3 values ($t0 + 5000 + $i, 1);
}
restart to commit

for ($i = 0; $i < 5200; $i++) {
  insert into t3 values ($t0 + $i, 2);
}
check query result
restart to commit
check query result

## Step 4. INSERT IN MIDDLE AND UPDATE FIRST HALF OF TWO BLOCKS
create table t4 (ts timestamp, a int);
for ($i = 0; $i < 200; $i++) {
  insert into t4 values ($t0 + $i, 1);
}
restart to commit
for ($i = 0; $i < 200; $i++) {
  insert into t4 values ($t0 + 5000 + $i, 1);
}
restart to commit

for ($i = 0; $i < 100; $i++) {
  insert into t4 values ($t0 + $i, 2);
}
for ($i = 200; $i < 5000; $i++) {
  insert into t4 values ($t0 + $i, 2);
}
for ($i = 0; $i < 100; $i++) {
  insert into t4 values ($t0 + 5000 + $i, 2);
}
check query result
restart to commit
check query result

## Step 5. INSERT IN MIDDLE AND UPDATE last HALF OF TWO BLOCKS
create table t5 (ts timestamp, a int);
for ($i = 0; $i < 200; $i++) {
  insert into t5 values ($t0 + $i, 1);
}
restart to commit
for ($i = 0; $i < 200; $i++) {
  insert into t5 values ($t0 + 5000 + $i, 1);
}
restart to commit

for ($i = 100; $i < 200; $i++) {
  insert into t5 values ($t0 + $i, 2);
}
for ($i = 200; $i < 5000; $i++) {
  insert into t5 values ($t0 + $i, 2);
}
for ($i = 100; $i < 200; $i++) {
  insert into t5 values ($t0 + 5000 + $i, 2);
}
check query result
restart to commit
check query result

## Step 6. INSERT MASSIVE DATA AND UPDATE ALL BLOCKS
create table t6 (ts timestamp, a int);
for ($i = 0; $i < 200; $i++) {
  insert into t6 values ($t0 + $i, 1);
}
restart to commit
for ($i = 0; $i < 200; $i++) {
  insert into t6 values ($t0 + 5000 + $i, 1);
}
restart to commit

for ($i = -1000; $i < 10000; $i++) {
  insert into t6 values ($t0 + $i, 2);
}
check query result
restart to commit
check query result

## Step 7. SPLIT DATA BLOCK
create table t7 (ts timestamp, a int);
for ($i = 0; $i < 200; $i++) {
  insert into t7 values ($t0 + $i, 1);
}
for ($i = 0; $i < 200; $i++) {
  insert into t7 values ($t0 + 5000 + $i, 1);
}
restart to commit

for ($i = -1000; $i < 10000; $i++) {
  insert into t7 values ($t0 + $i, 2);
}
check query result
restart to commit
check query result