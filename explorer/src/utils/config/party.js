import * as mdx from "./mdx";
import i18n from "@/lang";
export default () => [
  {
    path: "prometheus",
    name: "Prometheus",
    desc: i18n.t("docs.party.prometheus.desc"),

    docs: {
      zh: mdx.prometheusDoc,
      en: mdx.prometheusDoc,
    },
    steps: [
      { title: i18n.t("docs.party.prometheus.step1"), dom: "prerequisites" },
      { title: i18n.t("docs.party.prometheus.step2"), dom: "install-prometheus" },
      { title: i18n.t("docs.party.prometheus.step3"), dom: "configure-prometheus" },
      { title: i18n.t("docs.party.prometheus.step4"), dom: "start-prometheus" },
      { title: i18n.t("docs.party.prometheus.step5"), dom: "verify-remote-write" },
    ],
  },
  {
    path: "telegraf",
    name: "Telegraf",
    desc: i18n.t("docs.party.telegraf.desc"),

    docs: {
      zh: mdx.telegrafDoc,
      en: mdx.telegrafDoc,
    },
    steps: [
      { title: i18n.t("docs.party.telegraf.step1"), dom: "prerequisites" },
      { title: i18n.t("docs.party.telegraf.step2"), dom: "install-telegraf" },
      { title: i18n.t("docs.party.telegraf.step3"), dom: "configure" },
      { title: i18n.t("docs.party.telegraf.step4"), dom: "start-telegraf" },
      { title: i18n.t("docs.party.telegraf.step5"), dom: "verify" },
    ],
  },
  {
    path: 'influxdb',
    name: i18n.t("docs.party.influxdb.title"),
    icon: "influxDB",
    desc: i18n.t("docs.party.influxdb.desc", [i18n.t("docs.party.influxdb.title")]),
    docs:{
      zh:mdx.influxDBDoc,
      en:mdx.influxDBDoc
    },
    steps: [
      { title: i18n.t("docs.party.influxdb.step1"), dom: "config" },
      { title: i18n.t("docs.party.influxdb.step2"), dom: "insert" },
      { title: i18n.t("docs.party.influxdb.step3"), dom: "examples" },
    ],
  },
  {
    path: 'opentsdbjson',
    name: i18n.t("docs.party.opentsdbjson.title"),
    desc:i18n.t("docs.party.influxdb.desc", [i18n.t("docs.party.opentsdbjson.title")]),
    docs: {
      zh: mdx.opentsJSONDoc,
      en: mdx.opentsJSONDoc,
    },
    steps: [
      { title: i18n.t("docs.party.influxdb.step1"), dom: "config" },
      { title: i18n.t("docs.party.influxdb.step2"), dom: "insert" },
      { title: i18n.t("docs.party.influxdb.step3"), dom: "examples" },
    ],
  },
  {
    path: 'opentsdbtelnet',
    name: i18n.t("docs.party.opentsdbtelnet.title"),
    desc: i18n.t("docs.party.influxdb.desc", [i18n.t("docs.party.opentsdbtelnet.title")]),
    docs: {
      zh: mdx.opentsTelnetDoc,
      en: mdx.opentsTelnetDoc,
    },
    steps: [
      { title: i18n.t("docs.party.influxdb.step1"), dom: "config" },
      { title: i18n.t("docs.party.influxdb.step2"), dom: "insert" },
      { title: i18n.t("docs.party.influxdb.step3"), dom: "examples" },
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