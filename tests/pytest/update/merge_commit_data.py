# clear env set up

$t0 = 1603152000000

create database db update 1 days 30;

## Step 1. UPDATE THE WHOLE DATA BLOCK REPEATEDLY
create table t1 (ts timestamp, a int);
for ($i = 0; $i < 200; $i++) {
  insert into t1 values ($t0 + $i, 1);
}

restart to commit
check query result

for ($k = 0; $k < 10; $k++) {
  for ($i = 0; $i < 200; $i++) {
    insert into t1 values ($t0 + $i, 1);
  }

  check query result
  restart to commit
  check query result
}

## Step 2. PREPEND DATA
create table t2 (ts timestamp, a int);
for ($i = 0; $i < 200; $i++) {
  insert into t2 values ($t0 + $i, 1);
}

restart to commit
check query result

for ($i = -100; $i < 0; $i++) {
  insert into t2 values ($t0 + $i, 1);
}

check query result
restart to commit
check query result

## Step 3. PREPEND MASSIVE DATA
create table t3 (ts timestamp, a int);
for ($i = 0; $i < 200; $i++) {
  insert into t3 values ($t0 + $i, 1);
}

restart to commit
check query result

for ($i = -6000; $i < 0; $i++) {
  insert into t3 values ($t0 + $i, 1);
}

check query result
restart to commit
check query result

## Step 4. APPEND DATA
create table t4 (ts timestamp, a int);
for ($i = 0; $i < 200; $i++) {
  insert into t4 values ($t0 + $i, 1);
}

restart to commit
check query result

for ($i = 0; $i < 100; $i++) {
  insert into t4 values ($t0 + 200 + $i, 1);
}

check query result
restart to commit
check query result

## Step 5. APPEND MASSIVE DATA
create table t5 (ts timestamp, a int);
for ($i = 0; $i < 200; $i++) {
  insert into t5 values ($t0 + $i, 1);
}

restart to commit
check query result

for ($i = 0; $i < 6000; $i++) {
  insert into t5 values ($t0 + 200 + $i, 1);
}

check query result
restart to commit
check query result

## Step 6. UPDATE BLOCK IN TWO STEP
create table t6 (ts timestamp, a int);
for ($i = 0; $i < 200; $i++) {
  insert into t6 values ($t0 + $i, 1);
}

restart to commit
check query result

for ($i = 0; $i < 100; $i++) {
  insert into t6 values ($t0 + $i, 2);
}

check query result
restart to commit
check query result

for ($i = 100; $i < 200; $i++) {
  insert into t6 values ($t0 + $i, 2);
}

check query result
restart to commit
check query result

## Step 7. UPDATE LAST HALF AND INSERT LITTLE DATA
create table t7 (ts timestamp, a int);
for ($i = 0; $i < 200; $i++) {
  insert into t7 values ($t0 + $i, 1);
}

restart to commit
check query result

for ($i = 100; $i < 300; $i++) {
  insert into t7 values ($t0 + $i, 2);
}

check query result
restart to commit
check query result

## Step 8. UPDATE LAST HALF AND INSERT MASSIVE DATA
create table t8 (ts timestamp, a int);
for ($i = 0; $i < 200; $i++) {
  insert into t8 values ($t0 + $i, 1);
}

restart to commit
check query result

for ($i = 100; $i < 6000; $i++) {
  insert into t8 values ($t0 + $i, 2);
}

check query result
restart to commit
check query result

## Step 9. UPDATE FIRST HALF AND PREPEND LITTLE DATA
create table t9 (ts timestamp, a int);
for ($i = 0; $i < 200; $i++) {
  insert into t9 values ($t0 + $i, 1);
}

restart to commit
check query result

for ($i = -100; $i < 100; $i++) {
  insert into t9 values ($t0 + $i, 2);
}

check query result
restart to commit
check query result

## Step 10. UPDATE FIRST HALF AND PREPEND MASSIVE DATA
create table t10 (ts timestamp, a int);
for ($i = 0; $i < 200; $i++) {
  insert into t10 values ($t0 + $i, 1);
}

restart to commit
check query result

for ($i = -6000; $i < 100; $i++) {
  insert into t10 values ($t0 + $i, 2);
}

check query result
restart to commit
check query result

## Step 11. UPDATE FIRST HALF AND APPEND MASSIVE DATA
create table t11 (ts timestamp, a int);
for ($i = 0; $i < 200; $i++) {
  insert into t11 values ($t0 + $i, 1);
}

restart to commit
check query result

for ($i = 0; $i < 100; $i++) {
  insert into t11 values ($t0 + $i, 2);
}

for ($i = 200; $i < 6000; $i++) {
  insert into t11 values ($t0 + $i, 2);
}

check query result
restart to commit
check query result