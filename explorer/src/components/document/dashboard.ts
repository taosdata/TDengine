import * as mdx from "./mdx";
import { t } from "@/lang/index";

export default () => [
  {
    name: "Dashboard",
    icon: "logo",
    docs: {
      zh:mdx.dashboardDoc,
      en:mdx.dashboardDoc
    },
    steps: [
      { title: t("docs.dashboard.step1"), dom: "install-grafana" },
      { title: t("docs.dashboard.step2"), dom: "install-tdengine-plugin" },
      { title: t("docs.dashboard.step3"), dom: "start-grafana-server" },
      // { title: t("docs.dashboard.step4"), dom: "login-in-grafana" },
      { title: t("docs.dashboard.step5"), dom: "add-grafana-dbsource" }
      // { title: t("docs.dashboard.step6"), dom: "import-dashboard" },
    ],
  }
];
