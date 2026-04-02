# Migration

## 0.4, 0.5 ➡ 0.6

Architecturally 0.6 is much different from the previous versions. If you didn't implement your own scheduler, this version's only big change is the adding a reference of the scheduler when creating/removing notifications of a job.

### Removals

#### JobScheduler::new_with_scheduler()
The JobSchedulerWithoutSync trait has been removed. The new_with_scheduler method accordingly also. Replaced with new_with_storage_and_code().

#### JobStore
JobStore trait has been removed. Replaced by MetadataStore and NotificationStore traits.

#### JobSchedulerWithoutSync
No longer used.

#### SimpleJobScheduler
JobSchedulerWithoutSync trait deletion and default implementation SimpleJobScheduler removed.

#### SimpleJobStore
Removed as JobStore trait has been removed.

### API changes
#### Add &scheduler to notification add / removal

The first parameter to all of these functions on the JobLocked type needs to pass a reference to the scheduler as a first parameter.

Affected methods:

 Affected |                                                                        |
----------------------------------------- | -------------------------------------- |
JobLocked::on_notifications_add           |                                        |
JobLocked::on_start_notification_add      | JobLocked::on_done_notification_add    |
JobLocked::on_removed_notification_add    | JobLocked::on_stop_notification_add    |
JobLocked::on_notification_removal        |                                        |
JobLocked::on_start_notification_remove   | JobLocked::on_done_notification_remove |
JobLocked::on_removed_notification_remove | JobLocked::on_stop_notification_remove |



### Additions
#### JobScheduler::new_with_storage_and_code()
Custom job metadata, job notification, job code and notification providers with the scheduler.

#### MetaDataStore
Trait needed by the scheduler to schedule jobs.

#### NotificationStore
Trait needed by the scheduler to run notifications on job start/scheduled/stop/removals.

#### ToCode
Generic trait that provides a PinnedGetFuture for a UUID. 

#### JobCode
Trait that provides the runnable closures for the scheduler. Specific type of ToCode. Default implementation SimpleJobCode.

#### NotificationCode
Trait that provides the runnable notification closures for the scheduler. Specific type of ToCode. Default implementation SimpleNotificationCode.

#### SimpleMetadataStore
Default implementation for the MetadataStore.

#### SimpleNotificationStore
Default implementation for the NotificationStore.

#### PostgresMetadataStore
Postgres implementation of the MetadataStore. Needs postgres_storage feature.

#### PostgresNotificationStore
Postgres implementation of the NotificationStore. Needs postgres_storage feature.

#### NatsMetadataStore
Nats implementation of the MetadataStore. Needs nats_storage feature.

#### NatsNotificationStore
Nats implementation of the NotificationStore. Needs nats_storage feature.