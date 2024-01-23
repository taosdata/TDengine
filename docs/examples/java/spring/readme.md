# TDEngine Cloud example

## Introduction

Use Spring Boot to build a simple web application, and deploy it to connect to TDEngine Cloud.

## Usage

1. build the project

```
mvn clean package -Dmaven.test.skip=true
```

2. replace the current url with the database configuration in cloud page `Programming -> Java -> Config`, and run the project

```
java -jar target/example-1.0-SNAPSHOT.jar --spring.datasource.url="jdbc:TAOS-RS://xxxxx"
```

### Check the result

3. open the browser and visit the following url

```
http://localhost:8080/meter/list
```

5. create sub-table `cloud_demo` of meters, sub_table name is only allowed to contain letters, numbers, and underscores.

```aidl
http://localhost:8080/meter/cloud_demo/create
```

6. insert data to sub-table `cloud_demo` of meters

```aidl
http://localhost:8080/meter/cloud_demo/insert
```

7. query last row from sub-table `cloud_demo` of meters

```aidl
http://localhost:8080/meter/cloud_demo/last_row
```
