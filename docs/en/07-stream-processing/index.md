---
sidebar_label: Stream Processing
title: Stream Processing
description: Overview of TDengine stream processing, trigger-compute decoupling, and capability extensions
---

In time-series data processing, there are many common stream processing requirements, such as:

- **Tiered storage and intelligent downsampling:** Industrial equipment may generate tens of thousands of raw data points per second. Storing everything in full causes storage costs to surge, query efficiency to drop, and historical trend analysis to respond slowly.
- **Precomputation to accelerate real-time decisions:** When users query full datasets, the system may need to scan tens of billions of records, making it nearly impossible to return results in real time. This leads to lag in dashboards and reports.
- **Anomaly detection and low-latency alerts:** Monitoring and alerting require retrieving specific data with very low latency based on predefined rules. Traditional batch processing often has delays on the order of minutes.

In traditional time-series solutions, Kafka, Flink, and other stream processing systems are often deployed. However, the complexity of these systems brings high development and operations costs. The stream processing engine in TDengine TSDB provides the capability to process incoming data streams in real time. Using SQL, users can define real-time transformations. Once data is written into the source table of a stream, it is automatically processed as defined, and results are pushed to target tables according to the trigger mode. This offers a lightweight alternative to complex stream processing systems, while still delivering millisecond-level result latency even under high-throughput data ingestion.

Compared with traditional stream processing, TDengine TSDB’s stream processing adopts a trigger–compute decoupling strategy, still operating on continuous unbounded data streams, but with the following enhancements:

- **Extended processing targets:** In traditional stream processing, the event trigger source and the computation target are usually the same — events and computations are both generated from the same dataset. TDengine TSDB's stream processing allows the trigger source (event driver) and the computation source to be separated. The trigger table and the computation source table can be different, and a trigger table may not be required at all. The processed dataset can vary in terms of columns and time ranges.
- **Extended triggering mechanisms:** In addition to the standard “data write” trigger, TDengine TSDB's stream processing supports more trigger modes. With window-based triggers, users can flexibly define and use various windowing strategies to generate trigger events, choosing to trigger on window open, window close, or both. Beyond event-time-driven triggers linked to a trigger table, time-independent triggers are also supported, such as scheduled triggers. Before an event is triggered, TDengine can pre-filter trigger data so that only data meeting certain conditions proceeds to trigger evaluation.
- **Extended computation scope:** Computations can be performed on the trigger table or on other databases and tables. The computation type is unrestricted — any query statement is supported. The application of computation results is flexible: results can be sent as notifications, written to output tables, or both.

TDengine TSDB’s stream processing engine also offers additional usability benefits. For varying requirements on result latency, it allows users to balance between result timeliness and resource load. For different needs in out-of-order write scenarios, it enables users to flexibly choose appropriate handling methods and strategies.

**Note:** The new stream processing feature is supported starting from v3.3.7.0.

This chapter next covers [Stream Syntax](./01-syntax.md), [Operations and Limits](./02-instructions.md), and [Deployment and Design](./03-best-practices.md).
