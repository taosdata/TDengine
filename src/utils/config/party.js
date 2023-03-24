import * as mdx from "./mdx";
import i18n from "@/lang";

export default [
  {
    name: "Prometheus",
    desc: i18n.t("docs.party.prometheus.desc"),
    docs: mdx.prometheusDoc,
    steps: [
      { title: i18n.t("docs.party.prometheus.step1"), dom: "prerequisites" },
      { title: i18n.t("docs.party.prometheus.step2"), dom: "install-prometheus" },
      { title: i18n.t("docs.party.prometheus.step3"), dom: "configure-prometheus" },
      { title: i18n.t("docs.party.prometheus.step4"), dom: "start-prometheus" },
      { title: i18n.t("docs.party.prometheus.step5"), dom: "verify-remote-write" },
    ],
  },
  {
    name: "Telegraf",
    desc: i18n.t("docs.party.telegraf.desc"),
    docs: mdx.telegrafDoc,
    steps: [
      { title: i18n.t("docs.party.telegraf.step1"), dom: "prerequisites" },
      { title: i18n.t("docs.party.telegraf.step2"), dom: "install-telegraf" },
      { title: i18n.t("docs.party.telegraf.step3"), dom: "configure" },
      { title: i18n.t("docs.party.telegraf.step4"), dom: "start-telegraf" },
      { title: i18n.t("docs.party.telegraf.step5"), dom: "verify" },
    ],
  },
  {
    name: i18n.t("docs.party.influxdb.title"),
    icon: "influxDB.svg",
    desc: i18n.t("docs.party.influxdb.desc", [i18n.t("docs.party.influxdb.title")]),
    docs: mdx.influxDBDoc,
    steps: [
      { title: i18n.t("docs.party.influxdb.step1"), dom: "config" },
      { title: i18n.t("docs.party.influxdb.step2"), dom: "insert" },
      { title: i18n.t("docs.party.influxdb.step3"), dom: "examples" },
    ],
  },
  {
    name: i18n.t("docs.party.opentsdbjson.title"),
    desc: i18n.t("docs.party.influxdb.desc", [i18n.t("docs.party.opentsdbjson.title")]),
    docs: mdx.opentsJSONDoc,
    steps: [
      { title: i18n.t("docs.party.influxdb.step1"), dom: "config" },
      { title: i18n.t("docs.party.influxdb.step2"), dom: "insert" },
      { title: i18n.t("docs.party.influxdb.step3"), dom: "examples" },
    ],
  },
  {
    name: i18n.t("docs.party.opentsdbtelnet.title"),
    desc: i18n.t("docs.party.influxdb.desc", [i18n.t("docs.party.opentsdbtelnet.title")]),
    docs: mdx.opentsTelnetDoc,
    steps: [
      { title: i18n.t("docs.party.influxdb.step1"), dom: "config" },
      { title: i18n.t("docs.party.influxdb.step2"), dom: "insert" },
      { title: i18n.t("docs.party.influxdb.step3"), dom: "examples" },
    ],
  },
];
