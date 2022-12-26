import * as mdx from "./mdx";
export default [
  {
    name: "Grafana",
    desc: "TDengine can be quickly integrated with the open-source data visualization system Grafana to build a data monitoring and alerting system. The whole process does not require any code development. And you can visualize the contents of the data tables in TDengine on a dashboard.",
    docs: {
      zh: "",
      en: mdx.grafanaEN,
    },
    steps: [
      { title: "Install Grafana", dom: "install-grafana" },
      { title: "Install TDengine plugin ", dom: "install-tdengine-plugin" },
      { title: "Verify Plugin", dom: "verify-plugin" },
      { title: "Use Grafana", dom: "use-grafana" },
    ],
  },
  {
    name: "Google Data Studio",
    desc: "Google Data Studio can quickly access TDengine and create interactive reports and dashboards using its web-based reporting features.The whole process does not require any code development. ",
    docs: {
      zh: "",
      en: mdx.gdsEN
    },
    steps: [
      {title: "Choose Data Source", dom: "choose-data-source"},
      {title: "Configuration", dom: "connector-configuration"},
      {title: "Create Report or Dashboard", dom: "create-report-or-dashboard"},
    ]
  }
];
