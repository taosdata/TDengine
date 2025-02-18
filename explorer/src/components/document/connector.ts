import * as mdx from "./mdx";
import { t } from "@/lang/index";

export default () => [
  {
    name: "Java",
    icon: "Java",
    type: "client",
    desc: t("docs.connector.desc", ["taos-jdbc"]),
    docs: {
      zh: mdx.javaDoc,
      en: mdx.javaDoc,
    },
    steps: [
      { title: t("docs.connector.java.step1"), dom: "add-dependency" },
      { title: t("docs.connector.java.step2"), dom: "config" },
      { title: t("docs.connector.java.step3"), dom: "connect" },
    ],
  },
  {
    name: "Go",
    icon: "Go",
    type: "client",
    desc: t("docs.connector.desc", ["driver-go"]),
    docs: {
      zh: mdx.goDoc,
      en: mdx.goDoc,
    },
    steps: [
      { title: t("docs.connector.go.step1"), dom: "initialize-module" },
      { title: t("docs.connector.go.step2"), dom: "add-dependency" },
      { title: t("docs.connector.go.step3"), dom: "config" },
      { title: t("docs.connector.go.step4"), dom: "connect" },
    ],
  },
  {
    name: "Python",
    icon: "Python",
    type: "client",
    desc: t("docs.connector.desc", ["taospy"]),
    docs: {
      zh: mdx.pythonDoc,
      en: mdx.pythonDoc,
    },
    steps: [
      { title: t("docs.connector.python.step1"), dom: "install-connector" },
      { title: t("docs.connector.python.step2"), dom: "config" },
      { title: t("docs.connector.python.step3"), dom: "connect" },
      { title: "Jupyter", dom: "jupyter" },
    ],
  },
  {
    name: "Node.js",
    icon: "NodeJS",
    type: "client",
    desc: t("docs.connector.desc", ["@tdengine/rest"]),
    docs: {
      zh: mdx.nodeDoc,
      en: mdx.nodeDoc,
    },
    steps: [
      { title: t("docs.connector.node.step1"), dom: "install-connector" },
      { title: t("docs.connector.node.step2"), dom: "config" },
      { title: t("docs.connector.node.step3"), dom: "connect" },
    ]
  },
  {
    name: "C#",
    icon: "csharp",
    type: "client",
    desc: t("docs.connector.desc", ["TDengine.Connector"]),
    docs: {
      zh: mdx.csharpDoc,
      en: mdx.csharpDoc,
    },
    steps: [
      { title: t("docs.connector.csharp.step1"), dom: "create-project" },
      { title: t("docs.connector.csharp.step2"), dom: "config" },
      { title: t("docs.connector.csharp.step3"), dom: "connect" },
    ],
  },
  {
    name: "Rust",
    icon: "Rust",
    type: "client",
    desc: t("docs.connector.rust.desc"),
    docs: {
      zh: mdx.rustDoc,
      en: mdx.rustDoc,
    },
    steps: [
      { title: t("docs.connector.rust.step1"), dom: "create-project" },
      { title: t("docs.connector.rust.step2"), dom: "add-dependency" },
      { title: t("docs.connector.rust.step3"), dom: "config" },
      { title: t("docs.connector.rust.step4"), dom: "connect" },
    ],
  },
  {
    name: 'R',
    icon: "R",
    type: 'client',
    desc: t('docs.connector.desc', ['taos']),
    docs: {
      zh:mdx.rDoc,
      en:mdx.rDoc
    },
    steps: [
      { title: t('docs.connector.r.step1'), dom: 'create-project' },
      { title: t('docs.connector.r.step2'), dom: 'config' },
      { title: t('docs.connector.r.step3'), dom: 'connect' }
    ]
  },
  {
    name: "REST API",
    icon: "restapi",
    type: "client",
    desc: t("docs.connector.rest.desc"),
    docs: {
      zh: mdx.restDoc,
      en: mdx.restDoc,
    },
    steps: [
      { title: t("docs.connector.rest.step1"), dom: "config" },
      { title: t("docs.connector.rest.step2"), dom: "insert" },
      { title: t("docs.connector.rest.step3"), dom: "query" },
    ],
  }
]

