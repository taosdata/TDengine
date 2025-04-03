---
title: TDengine 2.x
slug: /advanced-features/data-connectors/tdengine-2
---

import Image from '@theme/IdealImage';
import imgStep1 from '../../assets/tdengine-2-01.png';
import imgStep2 from '../../assets/tdengine-2-02.png';
import imgStep3 from '../../assets/tdengine-2-03.png';
import imgStep4 from '../../assets/tdengine-2-04.png';

import Enterprise from '../../assets/resources/_enterprise.mdx';

<Enterprise/>

This section describes how to create a data migration task through the Explorer interface to migrate data from the old version of TDengine2 to the current cluster.

## Feature Overview

taosX migrates data by querying the source cluster and writing the results to the target database. Specifically, taosX uses the data of a subtable over a period of time as the basic unit of query, and writes the data to be migrated to the target database in batches.

taosX supports three migration modes:

1. **history** mode. This refers to migrating data within a specified time range. If no time range is specified, it migrates all data up to the time the task was created. The task stops once migration is complete.
2. **realtime** mode. It synchronizes data from the time the task is created onwards. The task will continue to run unless manually stopped.
3. **both** mode. It first executes in history mode, then in realtime mode.

Under each migration mode, you can specify whether to migrate the table structure. If "always" is selected, the structure of the table is synchronized to the target database before migrating data. This process may take longer if there are many subtables. If it is certain that the target database already has the same table interface as the source database, it is recommended to choose "none" to save time.

The task saves progress information to the disk during operation, so if the task is paused and then restarted, or if it automatically recovers from an anomaly, the task will not start over from the beginning.

For more options, it is recommended to read the description of each form field on the task creation page in detail.

## Specific Steps

First, click on the "Data Writing" menu on the left, then click the "Add Data Source" button on the right.

<figure>
<Image img={imgStep1} alt="Add data source"/>
<figcaption>Figure 1. Add a data source</figcaption>
</figure>

Then enter the task name, such as "migrate-test", and finally select the type "TDengine2". At this point, the form switches to a form dedicated to migrating data from TDengine2, containing a large number of options, each with detailed explanations, as shown in the images below.

<figure>
<Image img={imgStep2} alt="Add data source"/>
<figcaption>Figure 2. Add a data source</figcaption>
</figure>

<figure>
<Image img={imgStep3} alt="Add data source"/>
<figcaption>Figure 3. Add a data source</figcaption>
</figure>

<figure>
<Image img={imgStep4} alt="Add data source"/>
<figcaption>Figure 4. Add a data source</figcaption>
</figure>

After clicking the "Submit" button to submit the task, return to the "Data Source" task list page to monitor the status of the task.
