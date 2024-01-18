import * as mdx from "./mdx";
import i18n from "@/lang";
export default () => [
  {
    name: "TDengine CLI",
    logo: "https://img1.baidu.com/it/u=3903506244,1663450695&fm=253&fmt=auto&app=138&f=GIF?w=500&h=310",
    desc: i18n.t("docs.tool.cli.desc"),
    docs: {
      zh: mdx.TDCLIDoc,
      en: mdx.TDCLIDoc,
    },
    steps: [
      { title: i18n.t("docs.tool.cli.step1"), dom: "installation" },
      { title: i18n.t("docs.tool.cli.step2"), dom: "config" },
      { title: i18n.t("docs.tool.cli.step3"), dom: "connect" },
      { title: i18n.t("docs.tool.cli.step4"), dom: "using-tdengine-cli" },
    ],
  },
  {
    name: "taosBenchmark",
    logo: "https://img1.baidu.com/it/u=3903506244,1663450695&fm=253&fmt=auto&app=138&f=GIF?w=500&h=310",
    desc: i18n.t("docs.tool.benchmark.desc"),
    docs: {
      zh: mdx.benchmarkDoc,
      en: mdx.benchmarkDoc,
    },
    steps: [
      { title: i18n.t("docs.tool.benchmark.step1"), dom: "introduction" },
      { title: i18n.t("docs.tool.benchmark.step2"), dom: "installation" },
      { title: i18n.t("docs.tool.benchmark.step3"), dom: "run" },
      { title: i18n.t("docs.tool.benchmark.step4"), dom: "configuration-file-parameters-in-detailed" },
    ]
  },
  {
    name: "taosDump",
    logo: "https://img1.baidu.com/it/u=3903506244,1663450695&fm=253&fmt=auto&app=138&f=GIF?w=500&h=310",
    desc: i18n.t('docs.dataout.dump.desc'),
    docs: {
      zh: mdx.taosDumpDoc,
      en: mdx.taosDumpDoc,
    },
    steps: [
      { title: i18n.t("docs.dataout.dump.step1"), dom: "introduction" },
      { title: i18n.t("docs.dataout.dump.step2"), dom: "installation" },
      { title: i18n.t("docs.dataout.dump.step3"), dom: "common-usage-scenarios" },
      { title: i18n.t("docs.dataout.dump.step4"), dom: "detailed-command-line-parameter-list" },
    ]
  },
  // {
  //   name: "taosX",
  //   logo: "https://img1.baidu.com/it/u=3903506244,1663450695&fm=253&fmt=auto&app=138&f=GIF?w=500&h=310",
  //   desc: "",
  //   docs: {
  //     zh: "",
  //     en: "https://spiderio.cn/docs-en/07-tools/04-taosx.md",
  //   },
  // },
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
    name: 'Seeq',
    desc: i18n.t('docs.tools.seeq.desc'),
    docs: {
      zh:mdx.seeqDoc,
      en:mdx.seeqDoc
    },
    steps: [
      { title: i18n.t('docs.tools.seeq.step1'), dom: 'seeq-repare' },
      { title: i18n.t('docs.tools.seeq.step2'), dom: 'seeq-install' },
      { title: i18n.t('docs.tools.seeq.step3'), dom: 'seeq-add-ds' },
      { title: i18n.t('docs.tools.seeq.step4'), dom: 'seeq-example' }
    ]
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
  },
  {
    name: 'PowerBI',
    icon: 'powerbilogo',
    desc: 'Power BI' + i18n.t('docs.tools.powerbi.desc'),
    docs: {
      zh: mdx.powerbiDoc,
      en: mdx.powerbiDoc
    },
    version: '>=3.2.2.0',
    steps: [
      { title: i18n.t('docs.tools.powerbi.step1'), dom: 'powerbi-repare' },
      { title: i18n.t('docs.tools.powerbi.step2'), dom: 'powerbi-install' },
      { title: i18n.t('docs.tools.powerbi.step3'), dom: 'powerbi-config' },
      { title: i18n.t('docs.tools.powerbi.step4'), dom: 'powerbi-import' },
      { title: i18n.t('docs.tools.powerbi.step5'), dom: 'powerbi-example' }
    ]
  },
  {
    name: 'YonghongBI',
    title: i18n.t('docs.tools.yonghongbi.name'),
    icon: 'yonghonglogo',
    desc: i18n.t('docs.tools.yonghongbi.desc') + i18n.t('docs.tools.yonghongbi.desc1'),
    docs: {
      zh: mdx.yonghongbiDoc,
      en: mdx.yonghongbiDoc
    },
    version: '>=3.2.2.0',
    isAli: true,
    steps: [
      { title: i18n.t('docs.tools.yonghongbi.step1'), dom: 'yonghongbi-repare' },
      { title: i18n.t('docs.tools.yonghongbi.step2'), dom: 'yonghongbi-install' },
      { title: i18n.t('docs.tools.yonghongbi.step3'), dom: 'yonghongbi-config' },
      { title: i18n.t('docs.tools.yonghongbi.step4'), dom: 'yonghongbi-createds' },
      { title: i18n.t('docs.tools.yonghongbi.step5'), dom: 'yonghongbi-report' }
    ]
  }
];
