import * as mdx from "./mdx";

const common = [
  {
    name: "Java",
    type: "client",
    desc: "Connect using the taos-jdbcdriver to encapsulate SQL as a REST request.",
    docs: {
      zh: "",
      en: mdx.javaEN,
    },
    steps: [
      { title: "Add Dependency", dom: "add-dependency" },
      { title: "Config", dom: "config" },
      { title: "Connect", dom: "connect" },
    ],
  },
  {
    name: "Go",
    type: "client",
    desc: "Connect using the driver-go to encapsulate SQL as a REST request.",
    docs: {
      zh: "",
      en: mdx.goEN,
    },
    steps: [
      { title: "Initialize Module", dom: "initialize-module" },
      { title: "Add Dependency", dom: "add-dependency" },
      { title: "Config", dom: "config" },
      { title: "Connect", dom: "connect" },
    ],
  },
  {
    name: "Python",
    type: "client",
    desc: "Connect using the taospy package to encapsulate SQL as a REST request.",
    docs: {
      zh: "",
      en: mdx.pythonEN,
    },
    steps: [
      { title: "Install connector", dom: "install-connector" },
      { title: "Config", dom: "config" },
      { title: "Connect", dom: "connect" },
    ],
  },
  {
    name: "Node.js",
    type: "client",
    desc: "Connect using the @tdengine/rest connector to encapsulate SQL as a REST request.",
    docs: {
      zh: "",
      en: mdx.nodeEN,
    },
    steps: [
      { title: "Install connector", dom: "install-connector" },
      { title: "Config", dom: "config" },
      { title: "Connect", dom: "connect" },
    ],
  },
  {
    name: "C#",
    icon: "csharp.svg",
    type: "client",
    desc: "Connect using the TDengine.Connector to encapsulate SQL as a REST request.",
    docs: {
      zh: "",
      en: mdx.csharpEN,
    },
    steps: [
      { title: "Create Project", dom: "create-project" },
      { title: "Config", dom: "config" },
      { title: "Connect", dom: "connect" },
    ],
  },
  {
    name: "Rust",
    type: "client",
    desc: "Connect using the taos connector to encapsulate SQL in a websocket connection.",
    docs: {
      zh: "",
      en: mdx.rustEN,
    },
    steps: [
      { title: "Create Project", dom: "create-project" },
      { title: "Add Dependency", dom: "add-dependency" },
      { title: "Config", dom: "config" },
      { title: "Connect", dom: "connect" },
    ],
  },
];

export const dataIn = common.concat([
  {
    name: "REST API",
    type: "client",
    desc: "In this section we will explain how to write into TDengine cloud service using REST API",
    docs: {
      zh: "",
      en: mdx.restIN,
    },
    steps: [
      { title: "Config", dom: "config" },
      { title: "Insert", dom: "insert" },
    ],
  },
]);
export const dataOut = common.concat([
  {
    name: "REST API",
    type: "client",
    desc: "In this section we will explain how to query data from TDengine cloud service using REST API. ",
    docs: {
      zh: "",
      en: mdx.restOUT,
    },
    steps: [
      { title: "Config", dom: "config" },
      { title: "Query", dom: "query" },
    ],
  },
]);
