import * as mdx from "./mdx";
import i18n from "@/lang";
export default () => [
  {
    name: "taosDump",
    logo: "https://img1.baidu.com/it/u=3903506244,1663450695&fm=253&fmt=auto&app=138&f=GIF?w=500&h=310",
    desc: i18n.t("docs.dataout.dump.desc"),
    docs: {
      zh: mdx.dumpDoc,
      en: mdx.dumpDoc,
    },
    steps: [
      {title: i18n.t("docs.dataout.dump.step1"), dom: "introduction"},
      { title: i18n.t("docs.dataout.dump.step2"), dom: "installation" },
      { title: i18n.t("docs.dataout.dump.step3"), dom: "common-usage-scenarios" },
      { title: i18n.t("docs.dataout.dump.step4"), dom: "detailed-command-line-parameter-list" },
    ],
  }
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
