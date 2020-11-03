# For step 1, we set all the values as 1
# for step 2, we set all the values as 2
# and so on
# check query result here means not

# clear env set up

# create database db update 1

# create table t (ts timestamp, a int)

# set an init time (such as $t0 = 1604295582000)

# Step 1
insert into t values ($t0, 1);
insert into t values ($t0 - 3, 1);
insert into t values ($t0 + 3, 1);

check query result

# Step 2
insert into t values ($t0, 2);
insert into t values ($t0 - 3, 2);
insert into t values ($t0 + 3, 2);

check query result

# Step 3
insert into t values ($t0 - 4, 3);
insert into t values ($t0 - 2, 3);
insert into t values ($t0 + 2, 3);
insert into t values ($t0 + 4, 3);

check query result

# Step 4
insert into t values ($t0 - 4, 4);
insert into t values ($t0 - 2, 4);
insert into t values ($t0 + 2, 4);
insert into t values ($t0 + 4, 4);

check query result

# Step 4
insert into t values ($t0 - 1, 5);
insert into t values ($t0 + 1, 5);

check query result

# Step 4
insert into t values ($t0 - 4, 6);
insert into t values ($t0 - 3, 6);
insert into t values ($t0 - 2, 6);
insert into t values ($t0 - 1, 6);
insert into t values ($t0, 6);
insert into t values ($t0 + 1, 6);
insert into t values ($t0 + 2, 6);
insert into t values ($t0 + 3, 6);
insert into t values ($t0 + 4, 6);

check query result

# Step 4

restart to commit

check query result