import * as mdx from "./mdx";
import i18n from "@/lang";
export default () => [
  {
    name: "Grafana",
    desc: i18n.t("docs.virtual.grafana.desc"),
    docs: {
      zh: mdx.grafanaDoc,
      en: mdx.grafanaDoc,
    },
    steps: [
      { title: i18n.t("docs.virtual.grafana.step1"), dom: "install-grafana" },
      { title: i18n.t("docs.virtual.grafana.step2"), dom: "install-tdengine-plugin" },
      { title: i18n.t("docs.virtual.grafana.step3"), dom: "verify-plugin" },
      { title: i18n.t("docs.virtual.grafana.step4"), dom: "use-grafana" },
    ],
  },
  {
    name: "Google Data Studio",
    desc: i18n.t("docs.virtual.gds.desc"),
    docs: {
      zh:mdx.gdsDoc,
      en:mdx.gdsDoc
    },
    steps: [
      {title: i18n.t("docs.virtual.gds.step1"), dom: "choose-data-source"},
      {title: i18n.t("docs.virtual.gds.step2"), dom: "connector-configuration"},
      {title: i18n.t("docs.virtual.gds.step3"), dom: "create-report-or-dashboard"},
    ]
  }
];
