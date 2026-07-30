---
sidebar_label: Stream Processing
title: Stream Processing
description: Overview of TDengine stream processing, trigger-compute decoupling, and capability extensions
---

Compared with traditional stream processing, TDengine TSDB’s stream processing extends both functionality and boundaries. Traditionally, stream processing is defined as a real-time computing paradigm focused on low latency, continuity, and event-time-driven processing of unbounded data streams. TDengine TSDB’s stream processing adopts a trigger–compute decoupling strategy, still operating on continuous unbounded data streams, but with the following enhancements:

- **Extended processing targets:** In traditional stream processing, the event trigger source and the computation target are usually the same — events and computations are both generated from the same dataset. TDengine TSDB's stream processing allows the trigger source (event driver) and the computation source to be separated. The trigger table and the computation source table can be different, and a trigger table may not be required at all. The processed dataset can vary in terms of columns and time ranges.
- **Extended triggering mechanisms: In addition to the standard “data write” trigger, TDengine TSDB's stream processing supports more trigger modes. With window-based triggers, users can flexibly define and use various windowing strategies to generate trigger events, choosing to trigger on window open, window close, or both. Beyond event-time-driven triggers linked to a trigger table, time-independent triggers are also supported, such as scheduled triggers. Before an event is triggered, TDengine can pre-filter trigger data so that only data meeting certain conditions proceeds to trigger evaluation.
- **Extended computation scope:** Computations can be performed on the trigger table or on other databases and tables. The computation type is unrestricted — any query statement is supported. The application of computation results is flexible: results can be sent as notifications, written to output tables, or both.

TDengine TSDB’s stream processing engine also offers additional usability benefits. For varying requirements on result latency, it allows users to balance between result timeliness and resource load. For different needs in out-of-order write scenarios, it enables users to flexibly choose appropriate handling methods and strategies.

**Note:** The new stream processing feature is supported starting from v3.3.7.0.

This chapter next covers [Stream Syntax](./01-syntax.md), [Operations and Limits](./02-instructions.md), and [Deployment and Design](./03-best-practices.md).
