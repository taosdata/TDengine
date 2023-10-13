import * as mdx from "./mdx";
import i18n from "@/lang";
export default () => [
  {
    name: "Java",
    type: "client",
    desc: i18n.t("docs.connector.desc", ["taos-jdbc"]),
    docs: {
      zh: "",
      en: mdx.javaDoc,
    },
    steps: [
      { title: i18n.t("docs.connector.java.step1"), dom: "add-dependency" },
      { title: i18n.t("docs.connector.java.step2"), dom: "config" },
      { title: i18n.t("docs.connector.java.step3"), dom: "connect" },
    ],
  },
  {
    name: "Go",
    type: "client",
    desc: i18n.t("docs.connector.desc", ["driver-go"]),
    docs: {
      zh: "",
      en: mdx.goDoc,
    },
    steps: [
      { title: i18n.t("docs.connector.go.step1"), dom: "initialize-module" },
      { title: i18n.t("docs.connector.go.step2"), dom: "add-dependency" },
      { title: i18n.t("docs.connector.go.step3"), dom: "config" },
      { title: i18n.t("docs.connector.go.step4"), dom: "connect" },
    ],
  },
  {
    name: "Python",
    type: "client",
    desc: i18n.t("docs.connector.desc", ["taospy"]),
    docs: {
      zh: "",
      en: mdx.pythonDoc,
    },
    steps: [
      { title: i18n.t("docs.connector.python.step1"), dom: "install-connector" },
      { title: i18n.t("docs.connector.python.step2"), dom: "config" },
      { title: i18n.t("docs.connector.python.step3"), dom: "connect" },
      { title: "Jupyter", dom: "jupyter" },
    ],
  },
  {
    name: "Node.js",
    type: "client",
    desc: i18n.t("docs.connector.desc", ["@tdengine/rest"]),
    docs: {
      zh: "",
      en: mdx.nodeDoc,
    },
    steps: [
      { title: i18n.t("docs.connector.node.step1"), dom: "install-connector" },
      { title: i18n.t("docs.connector.node.step2"), dom: "config" },
      { title: i18n.t("docs.connector.node.step3"), dom: "connect" },
    ]
  },
  {
    name: "C#",
    icon: "csharp.svg",
    type: "client",
    desc: i18n.t("docs.connector.desc", ["TDengine.Connector"]),
    docs: {
      zh: "",
      en: mdx.csharpDoc,
    },
    steps: [
      { title: i18n.t("docs.connector.csharp.step1"), dom: "create-project" },
      { title: i18n.t("docs.connector.csharp.step2"), dom: "config" },
      { title: i18n.t("docs.connector.csharp.step3"), dom: "connect" },
    ],
  },
  {
    name: "Rust",
    type: "client",
    desc: i18n.t("docs.connector.rust.desc"),
    docs: {
      zh: "",
      en: mdx.rustDoc,
    },
    steps: [
      { title: i18n.t("docs.connector.rust.step1"), dom: "create-project" },
      { title: i18n.t("docs.connector.rust.step2"), dom: "add-dependency" },
      { title: i18n.t("docs.connector.rust.step3"), dom: "config" },
      { title: i18n.t("docs.connector.rust.step4"), dom: "connect" },
    ],
  },
  {
    name: "REST API",
    type: "client",
    desc: i18n.t("docs.connector.rest.desc"),
    docs: {
      zh: "",
      en: mdx.restDoc,
    },
    steps: [
      { title: i18n.t("docs.connector.rest.step1"), dom: "config" },
      { title: i18n.t("docs.connector.rest.step2"), dom: "insert" },
      { title: i18n.t("docs.connector.rest.step3"), dom: "query" },
    ],
  }
]

