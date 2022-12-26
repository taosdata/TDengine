import * as mdx from "./mdx";
export default [
  {
    name: "Prometheus",
    desc: "Configure Prometheus to write and read data from TDengine Cloud.",

    docs: {
      zh: "",
      en: mdx.prometheusEN,
    },
    steps: [
      { title: "Install Prometheus", dom: "install-prometheus" },
      { title: "Configure", dom: "configure" },
      { title: "Start Prometheus", dom: "start-prometheus" },
      { title: "Verify Remote Write", dom: "verify-remote-write" },
      { title: "Verify Remote Read", dom: "verify-remote-read" },
    ],
  },
  {
    name: "Telegraf",
    desc: "Configure Telegraf to write metrics to TDengine Cloud.",

    docs: {
      zh: "",
      en: mdx.telegrafEN,
    },
    steps: [
      { title: "Install Telegraf", dom: "install-telegraf" },
      { title: "Configure", dom: "configure" },
      { title: "Start Telegraf", dom: "start-telegraf" },
      { title: "Verify", dom: "verify" },
    ],
  },
  {
    name: "InfluxDB Line Protocol",
    icon: "influxDB.svg",
    desc: "In this section we will explain how to write into TDengine cloud service using schemaless InfluxDB line protocols over REST interface",
    docs: {
      zh: "",
      en: mdx.influxDB,
    },
    steps: [
      { title: "Config", dom: "config" },
      { title: "Insert", dom: "insert" },
      { title: "Examples", dom: "insert-example" },
    ],
  },
  {
    name: "OpenTSDB JSON Protocol",
    desc: "In this section we will explain how to write into TDengine cloud service using schemaless OpenTSDB JSON protocols over REST interface",
    docs: {
      zh: "",
      en: mdx.opentsJSON,
    },
    steps: [
      { title: "Config", dom: "config" },
      { title: "Insert", dom: "insert" },
      { title: "Examples", dom: "insert-example" },
    ],
  },
  {
    name: "OpenTSDB Telnet Protocol",
    desc: "In this section we will explain how to write into TDengine cloud service using schemaless OpenTSDB Telnet protocols over REST interface",
    docs: {
      zh: "",
      en: mdx.opentsTelnet,
    },
    steps: [
      { title: "Config", dom: "config" },
      { title: "Insert", dom: "insert" },
      { title: "Examples", dom: "insert-example" },
    ],
  },
  // {
  //   name: "Collectd",
  //   desc:
  //     "collectd is a daemon used to collect system performance metric data. collectd provides various storage mechanisms to store different values. It periodically counts system performance statistics while the system is running and storing information. You can use this information to help identify current system performance bottlenecks and predict future system load.",
  //
  // },
  // {
  //   name: "StatsD",
  //   desc:
  //     "StatsD is a simple daemon for aggregating application metrics, which has evolved rapidly in recent years into a unified protocol for collecting application performance metrics.",
  //
  // },
  // {
  //   name: "icinga2",
  //   desc:
  //     "icinga2 is an open-source, host and network monitoring software initially developed from the Nagios network monitoring application. Currently, icinga2 is distributed under the GNU GPL v2 license.",
  //
  // },
  // {
  //   name: "TCollector",
  //   desc: "TCollector is part of openTSDB and collects client computer's logs to send to the database.",
  //
  // },
  // {
  //   name: "EMQX Broker",
  //   desc: `MQTT is a popular IoT data transfer protocol. EMQX is an open-source MQTT Broker software. You can write MQTT data directly to TDengine without any code. You only need to setup "rules" in EMQX Dashboard to create a simple configuration. EMQX supports saving data to TDengine by sending data to a web service and provides a native TDengine driver for direct saving in the Enterprise Edition. Please refer to the EMQX official documentation for details on how to use it.).`,
  //
  // },
  // {
  //   name: "HiveMQ Broker",
  //   desc:
  //     "HiveMQ is an MQTT broker that provides community and enterprise editions. HiveMQ is mainly for enterprise emerging machine-to-machine M2M communication and internal transport, meeting scalability, ease of management, and security features. HiveMQ provides an open-source plug-in development kit. MQTT data can be saved to TDengine via TDengine extension for HiveMQ. Please refer to the HiveMQ extension - TDengine documentation for details on how to use it.",
  //
  // },
  // {
  //   name: "Kafka",
  //   desc:
  //     "TDengine Kafka Connector contains two plugins: TDengine Source Connector and TDengine Sink Connector. Users only need to provide a simple configuration file to synchronize the data of the specified topic in Kafka (batch or real-time) to TDengine or synchronize the data (batch or real-time) of the specified database in TDengine to Kafka.",
  //   logo: "https://img1.baidu.com/it/u=2985836749,28287576&fm=253&fmt=auto&app=138&f=JPEG?w=369&h=341",
  // },
];
