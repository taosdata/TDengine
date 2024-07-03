---
toc_max_heading_level: 4
title: "IP White List"
sidebar_label: "IP White List"
---

## Introduction

Since TDengine 3.2.0.0, the DBA can use white list to control each user can only access the TDengine service from specified IP addresses, and this feature impacts native connection, RESTful access and websocket connection. If any user tries to access from an IP address not in the white list of this user, the connection request will be refused. This feature is available in TDengine enterprise only.

## Configuration

1. Server side: a new parameter `enableWhiteList` needs to be added in taos.cfg to enable this feature, and it must be consistent across all data dnodes, i.e. either all data nodes enable this feature or all data nodes disable this feature, otherwise dnode will fail to start. If this feature is enabled, only (user, IP) in the white list can access the database. 

2. If this feature is enabled, (root, dnode1) to (root, dnoden) will be added in the whilte list automatically. That means, by default, root user can access the system from the machine where any dnode is running on. 

3. Assuming a user `userA` wants to access the database from IP` through taosAdapter which is running on IP2, the DBA needs to add both (userA, IP1) and (userA, IP2) in the white list.

4. User can turn on or turn off the white list from the client side dynamically using following commands
   ```sql
   alter all dnodes 'enableWhiteList 1' # turn on white list
   alter all dnodes 'enableWhiteList 0' # turn off white list
   ```
   If the white list has been turned on before, then turned off, and then turned on again, the white list added before is still valid. That is to say, once the user white list is added, it is always valid unless explicitly deleted. The switch here only determines whether to use the white list or not.

## Privilege

Only the root user can add, delete or modify white list. Non-root user can only query the white list.

## Create White List

```sql
CREATE USER user_name PASS password [SYSINFO value] [HOST host_name1[,host_name2]]     
```

You can add one or more IP or IP range in the white list of a user when creating the user.

Parameters:
- user_name: a new user name; if the user name already exists, this command will fail and propmpt error.
- host_nameX: IP or IP range specified by subnet mask

Example:
```sql
CREATAE USER test PASS 'a' HOST "127.0.0.0/24"，"192.168.0.23"
```

## Add IP in White List

```sql
ALTER USER user_name ADD HOST host_name1    
```

If you didn't add any IP in the white list of a user when creating it, you can also add using `alter user`.

Parameters:
- user_name: An already existing user name; if the user doesn't exist, the command will fail and prompt error.
- host_name1: IP or IP range specified by subnet mask.

Example:
```sql
ATLER USER root ADD HOST "127.0.0.0/24"
```

## Drop IP from White List

```sql
ALTER USER user_name DROP HOST host_name1
```

You can remove an IP or IP range from the white list of a user.
Parameters:
- user_name: An already existing user name; if the user doesn't exist, the command will fail and prompt error.
- host_name1: IP or IP range specified by subnet mask. This IP or IP range should have been added in the white list before, otherwise the command will fail and prompt error.

Example:
```sql
alter user root drop host "127.0.0.5"
```

## Drop User

```sql
drop user <user_name>
```

If a user is removed, then the white list of the user will be removed together.

## Error Code

The error codes that may be generated when operating whilte list are summarized below. 

1. TSDB_CODE_MND_USER_HOST_EXIST "Host already exist in ip white list" : Same IP or IP range is added twice
2. TSDB_CODE_MND_USER_HOST_NOT_EXIST,      "Host not exist in ip white list: Try to drop an IP or IP range not existing in the whilte list
3. TSDB_CODE_MND_TOO_MANY_USER_HOST,       "Too many host in ip white list": The number of IP or IP ranges in the white list of a user has reached the upper limit, i.e. 2048
4. TSDB_CODE_MND_USER_LOCAL_HOST_NOT_DROP,  "Host can not be dropped": Try to drop an IP that can't be removed
5. TSDB_CODE_IP_NOT_IN_WHITE_LIST， "Not allowed to connect": The user is trying to access from an IP not in the white list

## More Explanation

1. The IP address of each taosd will be added by default in the white list of root. 
2. If taosAdapter is running on a machine different from the machines where taosd are running, the IP of taosAdapter needs to be explicitly added in the white list of the users which want to access the database using either RESTful or websocket.
3. Parameter `enableWhiteList` must be consistent across all dnodes, other the cluster can't start. 
4. The change on white list will be effective in 2 seconds.
5. If one IP range is a subset of another one, like192.168.1.1/16  and 192.168.1.1/24, they will not be merged. Only two IP ranges that are exactly same will be merged. 
6. When droping an IP or IP range from the white list of a user, it must be exactly same as the IP or IP range added.\
7. Only root user can operate IP white list. 
8. x.x.x.x/32 and x.x.x.x are same IP range and will be shown as x.x.x.x .
9. If the client gets 0.0.0.0/0, it means IP white list is not enabled.
10. For each single user, up to 2048 IP or IP range can be added in its white list.
