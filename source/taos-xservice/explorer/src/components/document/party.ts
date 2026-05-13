import * as mdx from "./mdx";
import { t } from "@/lang/index";
export default () => [
  {
    path: "prometheus",
    name: "Prometheus",
    icon: "Prometheus",
    desc: t("docs.party.prometheus.desc"),
    docs: {
      zh: mdx.prometheusDoc,
      en: mdx.prometheusDoc,
    },
    steps: [
      { title: t("docs.party.prometheus.step1"), dom: "prerequisites" },
      { title: t("docs.party.prometheus.step2"), dom: "install-prometheus" },
      { title: t("docs.party.prometheus.step3"), dom: "configure-prometheus" },
      { title: t("docs.party.prometheus.step4"), dom: "start-prometheus" },
      { title: t("docs.party.prometheus.step5"), dom: "verify-remote-write" },
    ],
  },
  {
    path: "telegraf",
    name: "Telegraf",
    icon: "Telegraf",
    desc: t("docs.party.telegraf.desc"),
    docs: {
      zh: mdx.telegrafDoc,
      en: mdx.telegrafDoc,
    },
    steps: [
      { title: t("docs.party.telegraf.step1"), dom: "prerequisites" },
      { title: t("docs.party.telegraf.step2"), dom: "install-telegraf" },
      { title: t("docs.party.telegraf.step3"), dom: "configure" },
      { title: t("docs.party.telegraf.step4"), dom: "start-telegraf" },
      { title: t("docs.party.telegraf.step5"), dom: "verify" },
    ],
  },
  {
    path: 'influxdb',
    name: t("docs.party.influxdb.title"),
    icon: "influxDB",
    desc: t("docs.party.influxdb.desc", [t("docs.party.influxdb.title")]),
    docs:{
      zh:mdx.influxDBDoc,
      en:mdx.influxDBDoc
    },
    steps: [
      { title: t("docs.party.influxdb.step1"), dom: "config" },
      { title: t("docs.party.influxdb.step2"), dom: "insert" },
      { title: t("docs.party.influxdb.step3"), dom: "examples" },
    ],
  },
  {
    path: 'opentsdbjson',
    icon: 'logo',
    name: t("docs.party.opentsdbjson.title"),
    desc:t("docs.party.influxdb.desc", [t("docs.party.opentsdbjson.title")]),
    docs: {
      zh: mdx.opentsJSONDoc,
      en: mdx.opentsJSONDoc,
    },
    steps: [
      { title: t("docs.party.influxdb.step1"), dom: "config" },
      { title: t("docs.party.influxdb.step2"), dom: "insert" },
      { title: t("docs.party.influxdb.step3"), dom: "examples" },
    ],
  },
  {
    path: 'opentsdbtelnet',
    icon: 'logo',
    name: t("docs.party.opentsdbtelnet.title"),
    desc: t("docs.party.influxdb.desc", [t("docs.party.opentsdbtelnet.title")]),
    docs: {
      zh: mdx.opentsTelnetDoc,
      en: mdx.opentsTelnetDoc,
    },
    steps: [
      { title: t("docs.party.influxdb.step1"), dom: "config" },
      { title: t("docs.party.influxdb.step2"), dom: "insert" },
      { title: t("docs.party.influxdb.step3"), dom: "examples" },
    ],
  },
];