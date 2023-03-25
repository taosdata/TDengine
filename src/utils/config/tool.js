import * as mdx from "./mdx";
import i18n from "@/lang";
export default [
  {
    name: "TDengine CLI",
    logo: "https://img1.baidu.com/it/u=3903506244,1663450695&fm=253&fmt=auto&app=138&f=GIF?w=500&h=310",
    desc: i18n.t("docs.tool.cli.desc"),
    docs: {
      zh: "",
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
      zh: "",
      en: mdx.benchmarkDoc,
    },
    steps: [
      { title: i18n.t("docs.tool.benchmark.step1"), dom: "introduction" },
      { title: i18n.t("docs.tool.benchmark.step2"), dom: "installation" },
      { title: i18n.t("docs.tool.benchmark.step3"), dom: "run" },
      { title: i18n.t("docs.tool.benchmark.step4"), dom: "configuration-file-parameters-in-detailed" },
    ]
  },
  // {
  //   name: "taosDump",
  //   logo: "https://img1.baidu.com/it/u=3903506244,1663450695&fm=253&fmt=auto&app=138&f=GIF?w=500&h=310",
  //   desc: "",
  //   docs: {
  //     zh: "",
  //     en: mdx.taosDump,
  //   },
  // },
  // {
  //   name: "taosX",
  //   logo: "https://img1.baidu.com/it/u=3903506244,1663450695&fm=253&fmt=auto&app=138&f=GIF?w=500&h=310",
  //   desc: "",
  //   docs: {
  //     zh: "",
  //     en: "https://spiderio.cn/docs-en/07-tools/04-taosx.md",
  //   },
  // },
];
