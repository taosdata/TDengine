---
title: Stream Processing Engine
slug: /inside-tdengine/stream-processing-engine
---

import Image from '@theme/IdealImage';
import imgStep01 from '../assets/stream-processing-engine-01.png';
import imgStep02 from '../assets/stream-processing-engine-02.png';
import imgStep03 from '../assets/stream-processing-engine-03.png';

## Stream Computing Architecture

The architecture of TDengine's stream computing is illustrated in the figure below. When a user inputs SQL to create a stream, the SQL is first parsed on the client side to generate the logical execution plan and its related attribute information necessary for stream computing execution. Next, the client sends this information to the mnode. The mnode dynamically converts the logical execution plan into a physical execution plan using the vgroups information from the database where the data source (super) table resides and further generates a Directed Acyclic Graph (DAG) for the stream task. Finally, the mnode initiates a distributed transaction, distributing the tasks to each vgroup, thereby starting the entire stream computing process.

<figure>
<Image img={imgStep01} alt="Stream processing architecture"/>
<figcaption>Figure 1. Stream processing architecture</figcaption>
</figure>

The mnode contains the following four logical modules related to stream computing:

- Task scheduling, responsible for converting logical execution plans into physical execution plans and distributing them to each vnode.
- Meta store, responsible for storing metadata related to stream computing tasks and the corresponding DAG information of stream tasks.
- Checkpoint scheduling, responsible for regularly generating checkpoint transactions and distributing them to each source task.
- Exec monitoring, responsible for receiving reported heartbeats, updating the execution status of each task in the mnode, and periodically monitoring checkpoint execution status and DAG change information.

Additionally, the mnode plays an important role in issuing control commands to stream computing tasks, including but not limited to pausing, resuming execution, deleting stream tasks, and updating upstream and downstream information of stream tasks.

At each vnode, at least two stream tasks are deployed: one is the source task, which is responsible for reading data from the WAL (and from TSDB when necessary) for subsequent processing and distributing the processing results to downstream tasks; the other is the sink task (write-back task), which is responsible for writing the received data into the corresponding vnode. To ensure the stability of the data flow and the scalability of the system, each sink task is equipped with traffic control functionality to adjust the data writing speed based on actual conditions.

## Basic Concepts

### Stateful Stream Computing

The stream computing engine possesses powerful scalar function computation capabilities, where the data processed is mutually independent on the time series, without needing to retain intermediate states of computation. This means that for all input data, the engine can perform fixed transformation operations, such as simple numerical addition, and directly output the results.

At the same time, the stream computing engine also supports aggregation calculations, which require maintaining intermediate states during execution. For example, when calculating the daily operating time of a device, since the statistical period may span multiple days, the application must continuously track and update the current operating status until the statistical period ends to derive accurate results. This is a typical application scenario for stateful stream computing, which requires the engine to maintain tracking and management of intermediate states during execution to ensure the accuracy of the final results.

### Write-Ahead Log

When data is written to TDengine, it is first stored in the WAL file. Each vnode has its own WAL file, storing data in the order in which time series data arrives. Since the WAL file retains the order of data arrival, it becomes an important data source for stream computing. Additionally, the WAL file has its own data retention policy controlled by database parameters, where data exceeding the retention period will be cleared from the WAL file. This design ensures data integrity and system reliability while providing a stable data source for stream computing.

### Event-Driven

An event in the system refers to a change or transition of state. In the stream computing architecture, the event that triggers the stream computing process is the write message of (super) table data. At this stage, the data may not be fully written to the TSDB but is undergoing negotiation among multiple replicas before reaching consensus.

Stream computing executes in an event-driven mode, where its data source does not come directly from the TSDB but from the WAL. Once data is written to the WAL file, it will be extracted and added to a pending processing queue, awaiting further processing by the stream computing task. This way of immediately triggering the stream computing engine execution upon data arrival ensures that data is processed promptly and the results are stored in the target table in the shortest time.

### Time

In the field of stream computing, time is a crucial concept. TDengine's stream computing involves three key time concepts: event time, ingestion time, and processing time.

- Event Time: This is the main timestamp for each record in the time series data (also known as the Primary Timestamp), typically provided by the sensors generating the data or the gateways reporting the data, used to accurately mark the time of record generation. Event time is a decisive factor for updating and pushing the results of stream computing.
- Ingestion Time: This refers to the time when a record is written to the database. Ingestion time and event time are usually independent; in general, ingestion time is later than or equal to event time (unless the user intentionally writes data for a future time).
- Processing Time: This is the time point when the stream computing engine begins processing the data written in the WAL file. For scenarios that have set the max_delay option to control the result return strategy of stream computing, processing time directly determines when results are returned. Notably, under the at_once and window_close triggering modes, once data arrives at the WAL file, it will be immediately written to the input queue of the source task and calculations will begin.

The distinction of these time concepts ensures that stream computing can accurately handle time series data and adopt corresponding processing strategies based on the characteristics of different time points.

### Time Window Aggregation

TDengine's stream computing function allows data to be divided into different time windows based on the event time of the records. By applying specified aggregation functions, the aggregation results for each time window are computed. When new records arrive in the window, the system triggers an update of the corresponding window's aggregation result and passes the updated results to downstream stream computing tasks according to the pre-set push strategy.

When the aggregation results need to be written to the preset supertable, the system first generates the corresponding subtable name based on grouping rules and then writes the results into the corresponding subtable. It is worth mentioning that the time window division strategy in stream computing is consistent with the window generation and division strategy in batch queries, ensuring consistency and efficiency in data processing.

### Out-of-Order Processing

Due to complex factors such as network transmission and data routing, the data written to the database may not maintain the monotonic increasing characteristic of event time. This phenomenon, where non-monotonic increasing data occurs during the writing process, is called out-of-order writing.

Whether out-of-order writing will affect the stream computing results of the corresponding time window depends on the watermark parameter set when creating the stream computing task and whether the ignore expired parameter is configured. These two parameters jointly determine whether to discard these out-of-order data or to include them and incrementally update the computation results of the belonging time window. In this way, the system can flexibly handle out-of-order data while maintaining the accuracy of the stream computing results, ensuring data integrity and consistency.

## Stream Computing Tasks

Each activated stream computing instance consists of multiple stream tasks distributed across different vnodes. These stream tasks exhibit similarities in the overall architecture, including an in-memory input queue and output queue for executing time series data, as well as a storage system for maintaining local state, as shown in the figure below. This design ensures high performance and low latency for stream computing tasks while providing good scalability and fault tolerance.

<figure>
<Image img={imgStep02} alt="Composition of stream processing tasks"/>
<figcaption>Figure 2. Composition of stream processing tasks</figcaption>
</figure>

According to the different tasks undertaken by stream tasks, they can be classified into three categories—source task, agg task, and sink task.

### Source Task

The data processing in stream computing begins with reading data from the local WAL file, followed by local computation at the node. The source task adheres to the natural order of data arrival, sequentially scanning the WAL file and filtering out data that meets specific requirements. Subsequently, the source task processes this time series data sequentially. Therefore, regardless of how many vnodes the data source (super) table is distributed across, an equal number of source tasks will be deployed in the cluster. This distributed processing approach ensures parallel processing of data and efficient utilization of cluster resources.

### Agg Task

The downstream task of the source task is to receive the results aggregated by the source task and further summarize these results to generate the final output. In clusters configured with snode, agg tasks are preferentially scheduled to run on snode to leverage its storage and processing capabilities. If there is no snode in the cluster, the mnode will randomly select a vnode to schedule the execution of the agg task. It is worth noting that agg tasks are not required in all cases. For stream computing scenarios that do not involve window aggregation (for example, stream computations containing only scalar operations or aggregation stream computations when there is only one vnode in the database), agg tasks will not occur. In such cases, the topology of stream computing will be simplified to only include two levels of stream tasks: the source task and the downstream task that directly outputs results.

### Sink Task

The sink task is responsible for receiving the output results from agg tasks or source tasks and writing them into the local vnode to complete the data write-back process. Similar to source tasks, each result (super) table distributed across the vnodes will be equipped with a dedicated sink task. Users can adjust the throughput of sink tasks through configuration parameters to meet different performance needs.

The three types of tasks each perform their respective roles in the stream computing architecture, distributed across different levels. Clearly, the number of source tasks directly depends on the number of vnodes, with each source task independently responsible for processing data in its respective vnode, without interference from other source tasks and without constraints of order. However, it is noteworthy that if the final stream computing result is aggregated into a single table, only one sink task will be deployed on the vnode where that table resides. The cooperative relationships among these three types of tasks are illustrated in the figure below, collectively forming the complete execution process of stream computing tasks.

<figure>
<Image img={imgStep02} alt="Relationships between tasks"/>
<figcaption>Figure 3. Relationships between tasks</figcaption>
</figure>

## Stream Computing Nodes

The snode is an independent taosd process dedicated to stream computing, specifically for deploying agg tasks. The snode not only has local state management capabilities but also includes the function of remotely backing up data. This allows the snode to collect and store checkpoint data dispersed across various vgroups and, when needed, remotely download this data to the node restarting the stream computing, thereby ensuring the consistency of the stream computing state and the integrity of the data.

## State and Fault Tolerance Handling

### Checkpoints

During the stream computing process, the system adopts a distributed checkpoint mechanism to periodically (default every 3 minutes) save the internal operator states of each task during computation. These state snapshots are referred to as checkpoints. The creation of checkpoints is only associated with processing time and not with event time.

Assuming all stream tasks are running normally, the mnode will periodically initiate transactions to create checkpoints and distribute these transactions to the top-level tasks of each stream. The messages responsible for processing these transactions will then enter the data processing queue.

The method for generating checkpoints in TDengine is consistent with mainstream stream computing engines, where each generated checkpoint contains complete operator state information. For each task, checkpoint data is retained as a single copy only on the node where the task is running, independent of the replication settings of the time series data storage engine. It is worth noting that the metadata of stream tasks also employs a multi-replica saving mechanism, which is managed within the scope of the time series data storage engine. Therefore, when executing stream computing tasks on a multi-replica cluster, the metadata will also achieve multi-replica redundancy.

To ensure the reliability of checkpoint data, TDengine's stream computing engine provides the capability to remotely back up checkpoint data, supporting asynchronous uploads of checkpoint data to remote storage systems. In this way, even if the locally retained checkpoint data is damaged, it can be downloaded from the remote storage system, and the stream computing can be restarted on a new node, continuing the execution of computation tasks. This measure further enhances the fault tolerance and data security of the stream computing system.

### State Storage Backend

The computation state information of operators in stream tasks is persistently stored on local hard drives in file format.

## Memory Management

Each non-sink task is equipped with corresponding input and output queues, while sink tasks have only input queues without output queues. The maximum data capacity limit for both types of queues is set to 60MB, dynamically occupying storage space based on actual needs. When the queue is empty, it does not occupy any storage space. Additionally, the agg task also consumes some memory space when internally saving computation states. This memory usage can be adjusted by setting the parameter streamBufferSize, which controls the size of the cached window state in memory, with a default value of 128MB. The parameter maxStreamBackendCache is used to limit the maximum occupied storage space of the backend in memory, also defaulting to 128MB. Users can adjust this value to any value between 16MB and 1024MB as needed.

## Traffic Control

The stream computing engine implements a traffic control mechanism in sink tasks to optimize data writing performance and prevent excessive resource consumption. This mechanism primarily controls traffic through the following two metrics.

- The number of write operation calls per second: The sink task is responsible for writing processing results into its corresponding vnode. The upper limit for this metric is fixed at 50 calls per second to ensure the stability of write operations and the rational allocation of system resources.
- The throughput of data written per second: By configuring the parameter streamSinkDataRate, users can control the amount of data written per second. This parameter can be adjusted between 0.1MB/s and 5MB/s, with a default value of 2MB/s. This means that for a single vnode, each sink task can write a maximum of 2MB of data per second.

The traffic control mechanism of the sink task not only prevents synchronization negotiation buffer overflow caused by high-frequency writes in multi-replica scenarios but also avoids excessive data accumulation in the write queue, which consumes a large amount of memory space. This effectively reduces the memory occupancy of the input queue. Thanks to the backpressure mechanism applied in the overall computing framework, the sink task can directly feedback the effects of traffic control to the uppermost tasks, thereby reducing the occupancy of computing resources by stream computing tasks, avoiding excessive resource consumption, and ensuring the overall stability and efficiency of the system.

## Backpressure Mechanism

TDengine's stream computing framework is deployed across multiple computing nodes. To coordinate the execution progress of tasks on these nodes and prevent the continuous influx of data from upstream tasks from overloading downstream tasks, the system implements a backpressure mechanism both within tasks and between upstream and downstream tasks.

Within tasks, the backpressure mechanism is implemented by monitoring the output queue's full condition. Once a task's output queue reaches its storage limit, the current computing task will enter a waiting state, pausing the processing of new data until there is sufficient space in the output queue to accommodate new computation results, after which the data processing flow will resume.

In the case of upstream and downstream tasks, the backpressure mechanism is triggered through message passing. When the input queue of a downstream task reaches its storage limit (i.e., the amount of data flowing into the downstream continuously exceeds the downstream task's maximum processing capacity), the upstream task will receive a signal from the downstream task indicating that the input queue is full. At this point, the upstream task will pause its computation processing until the downstream task has completed its processing and allows data to continue being distributed, at which point the upstream task will resume computation. This mechanism effectively balances the data flow between tasks, ensuring the stability and efficiency of the entire stream computing system.
