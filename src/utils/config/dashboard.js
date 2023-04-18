import * as mdx from "./mdx";
import i18n from "@/lang";

export default [
  {
    name: "Dashboard",
    docs: mdx.dashboardDoc,
    steps: [
      { title: i18n.t("docs.dashboard.step1"), dom: "install-grafana" },
      { title: i18n.t("docs.dashboard.step2"), dom: "install-tdengine-plugin" },
      { title: i18n.t("docs.dashboard.step3"), dom: "start-grafana-server" },
      { title: i18n.t("docs.dashboard.step4"), dom: "login-in-grafana" },
      { title: i18n.t("docs.dashboard.step5"), dom: "add-grafana-dbsource" },
      { title: i18n.t("docs.dashboard.step6"), dom: "import-dashboard" },
    ],
  }
];
