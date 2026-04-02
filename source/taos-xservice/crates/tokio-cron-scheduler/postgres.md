# PostgreSQL Persistent Storage

## Setup

### PostgreSQL

You'll need a running instance of PostgreSQL. You'll be able to run one in Docker.

```shell
docker run --rm -it -p 5432:5432 -e POSTGRES_USER="postgres" -e POSTGRES_PASSWORD="" -e POSTGRES_HOST_AUTH_METHOD="trust" postgres:14.1
```

### Connectivity options

#### Using environmental variables

 Variable          | Default   | Description                                                                                                                                
-------------------|-----------|--------------------------------------------------------------------------------------------------------------------------------------------
 POSTGRES_URL      |           | URL as per [docs](https://docs.rs/postgres/latest/postgres/config/struct.Config.html). Other DB connection setup variables ignored if set. 
 POSTGRES_HOST     | localhost | Host to connect to                                                                                                                         
 POSTGRES_PORT     | 5432      | Port to connect to                                                                                                                         
 POSTGRES_DB       | postgres  | Database name                                                                                                                              
 POSTGRES_USERNAME | postgres  | Username                                                                                                                                   
 POSTGRES_PASSWORD |           | Password                                                                                                                                   
 POSTGRES_APP_NAME |           | Application name to register on PostgreSQL server                                                                                          

#### Provide own instance

Both PostgresMetadataStore and PostgresNotificationStore encapsulates a PostgresStore, which in
turn encapsulates a Tokio Postgres Client. Override accordingly.

### Other options

 Environment Variable               | Default            | Description                                                                                                           
------------------------------------|--------------------|-----------------------------------------------------------------------------------------------------------------------
 POSTGRES_INIT_METADATA             |                    | If set to 'true', the metadata table will be created on PostgresMetadataStore initialization.                         
 POSTGRES_METADATA_TABLE            | job                | The metadata table name used by the PostgresMetadataStore.                                                            
 POSTGRES_INIT_NOTIFICATIONS        |                    | If set to 'true', the notification tables will be created on PostgresNotificationStore initizalization.               
 POSTGRES_NOTIFICATION_TABLE        | notification       | The table to hold the main notification data used by PostgresNotificationStore                                        
 POSTGRES_NOTIFICATION_STATES_TABLE | notification_state | The table to hold the states types vs notification id table. A 1:N relationship with the POSTGRES_NOTIFICATION_TABLE. 

