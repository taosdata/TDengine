---
sidebar_label: Data Publisher
title: Data Publisher
toc_max_heading_level: 4
---

TDengine provides a data publishing feature that supports real-time data push to various stream processing and message queue systems. This greatly enhances data mobility and system integration capabilities, meeting the diverse needs of IoT, big data analytics, real-time monitoring, and more.

Currently, TDengine supports data publishing to the following mainstream platforms:

- **MQTT**  
   TDengine can be configured to push data in real time to an MQTT server. MQTT is a lightweight messaging protocol widely used for communication between IoT devices. With TDengine's MQTT publishing feature, users can easily distribute sensor data, device status, and other information to various endpoints, enabling efficient data sharing and interaction.

- **Kafka**  
   TDengine supports publishing data to Kafka clusters. Kafka, as a high-throughput distributed message queue system, is commonly used in big data collection, log aggregation, and stream processing scenarios. By integrating with Kafka, TDengine can seamlessly connect real-time data to big data platforms, supporting data analytics, monitoring, alerting, and other applications.

- **Flink**  
   TDengine can also stream data to Flink. Flink is a high-performance stream processing engine suitable for real-time data analytics and complex event processing. With TDengine's Flink publishing capability, users can build end-to-end real-time data processing pipelines, meeting requirements for low latency and high reliability.

Through the data publishing feature, TDengine achieves deep integration with mainstream stream processing and message queue systems, helping users build flexible and efficient data distribution and processing architectures. Whether for IoT data collection, real-time monitoring, or big data analytics, this feature enables efficient data flow and value extraction.

For detailed configuration methods, usage steps, and precautions, please refer to the following sections.
