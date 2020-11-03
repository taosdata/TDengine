# clear env set up

$t0 = 1603152000000

create database db update 1 days 30;

## STEP 1: UPDATE THE LAST RECORD REPEATEDLY
create table t1 (ts timestamp, a int);

for ($i = 0; $i < 100; $i++) {
  insert into t1 values ($t0, $i);

  restart to commit
  check query result
}

## STEP 2: UPDATE THE WHOLE LAST BLOCK
create table t2 (ts timestamp, a int);
for ($i = 0; $i < 50; $i++) {
  insert into t2 values ($t0 + $i, 1);
}
restart to commit
check query result

for ($i = 0; $i < 50; $i++) {
  insert into t2 values ($t0 + $i, 2);
}

check query result
restart to commit
check query result

## STEP 3: UPDATE PART OF THE LAST BLOCK
create table t3 (ts timestamp, a int);
for ($i = 0; $i < 50; $i++) {
  insert into t3 values ($t0 + $i, 1);
}
restart to commit
check query result

for ($i = 0; $i < 25; $i++) {
  insert into t3 values ($t0 + $i, 2);
}

check query result
restart to commit
check query result

for ($i = 25; $i < 50; $i++) {
  insert into t3 values ($t0 + $i, 2);
}

check query result
restart to commit
check query result

## STEP 4: UPDATE AND INSERT APPEND AT END OF DATA
create table t4 (ts timestamp, a int);
for ($i = 0; $i < 50; $i++) {
  insert into t4 values ($t0 + $i, 1);
}
restart to commit
check query result

for ($i = 0; $i < 25; $i++) {
  insert into t4 values ($t0 + $i, 2);
}

for ($i = 50; $i < 60; $i++) {
  insert into t4 values ($t0 + $i, 2);
}

check query result
restart to commit
check query result

## STEP 5: UPDATE AND INSERT PREPEND SOME DATA
create table t5 (ts timestamp, a int);
for ($i = 0; $i < 50; $i++) {
  insert into t5 values ($t0 + $i, 1);
}
restart to commit
check query result

for ($i = -10; $i < 0; $i++) {
  insert into t4 values ($t0 + $i, 2);
}

for ($i = 0; $i < 25; $i++) {
  insert into t5 values ($t0 + $i, 2);
}

check query result
restart to commit
check query result

for ($i = -10; $i < 0; $i++) {
  insert into t4 values ($t0 + $i, 3);
}

for ($i = 25; $i < 50; $i++) {
  insert into t5 values ($t0 + $i, 3);
}

check query result
restart to commit
check query result

## STEP 6: INSERT AHEAD A LOT OF DATA
create table t6 (ts timestamp, a int);
for ($i = 0; $i < 50; $i++) {
  insert into t6 values ($t0 + $i, 1);
}
restart to commit
check query result

for ($i = -1000; $i < 0; $i++) {
  insert into t6 values ($t0 + $i, 2);
}

check query result
restart to commit
check query result

## STEP 7: INSERT AHEAD A LOT AND UPDATE
create table t7 (ts timestamp, a int);
for ($i = 0; $i < 50; $i++) {
  insert into t7 values ($t0 + $i, 1);
}
restart to commit
check query result

for ($i = -1000; $i < 25; $i++) {
  insert into t7 values ($t0 + $i, 2);
}

check query result
restart to commit
check query result

## STEP 8: INSERT AHEAD A LOT AND UPDATE
create table t8 (ts timestamp, a int);
for ($i = 0; $i < 50; $i++) {
  insert into t8 values ($t0 + $i, 1);
}
restart to commit
check query result

for ($i = 25; $i < 6000; $i++) {
  insert into t8 values ($t0 + $i, 2);
}

check query result
restart to commit
check query result

## STEP 9: UPDATE ONLY MIDDLE
create table t9 (ts timestamp, a int);
for ($i = 0; $i < 50; $i++) {
  insert into t9 values ($t0 + $i, 1);
}
restart to commit
check query result

for ($i = 20; $i < 30; $i++) {
  insert into t9 values ($t0 + $i, 2);
}

check query result
restart to commit
check query result

## STEP 10: A LOT OF DATA COVER THE WHOLE BLOCK
create table t10 (ts timestamp, a int);
for ($i = 0; $i < 50; $i++) {
  insert into t10 values ($t0 + $i, 1);
}
restart to commit
check query result

for ($i = -4000; $i < 4000; $i++) {
  insert into t10 values ($t0 + $i, 2);
}

check query result
restart to commit
check query result