# NATS Persistent Storage

## Setup

### NATS
You'll need a running instance of NATS that is running with Jetstream. You'll be able to run one using Docker.

From https://docs.nats.io/running-a-nats-service/introduction/running/nats_docker/jetstream_docker :

```bash
docker run --rm -it -p 4222:4222 -p 6222:6222 -p 7222:7222 -p 8222:8222 nats -js -DV
```

### Connectivity options

#### Using environmental variables

The default struct constructor for both the Metadata storage and the Notification storage uses environmental variables to set up a NatsStore.


 Variable               | Default              | Description 
 ---------------------- | -------------------- |-------------
 NATS_HOST              | nats://localhost     | Nats Host to connect to
 NATS_APP               | Unknown Nats app     | User presented name of the app connecting
 NATS_USERNAME          |                      | User name to connect with. Both this and password needs to be set otherwise it is ignored.
 NATS_PASSWORD          |                      | Password to connect with. Both this and username needs to be set otherwise it is ignored.
NATS_BUCKET_NAME        | tokiocron            | Key/Value bucket to store values in
NATS_BUCKET_DESCRIPTION | Tokio Cron Scheduler | key/Value bucket description.


#### Provide own Jetstream instance
Both NatsMetadataStore and NatsNotificationStore encapsulates a NatsStore that in turn encapsulates a Jetstream instance usin . Provide it accordingly. See https://github.com/nats-io/nats.rs .


