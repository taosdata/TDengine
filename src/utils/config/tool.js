import * as mdx from "./mdx";
export default [
  {
    name: "TDengine CLI",
    logo: "https://img1.baidu.com/it/u=3903506244,1663450695&fm=253&fmt=auto&app=138&f=GIF?w=500&h=310",
    desc: "The interactive shell for operating on TDengine",
    docs: {
      zh: "",
      en: mdx.TDCLIEN,
    },
    steps: [
      { title: "Installation", dom: "installation" },
      { title: "Config", dom: "config" },
      { title: "Connect", dom: "connect" },
      { title: "Using TDengine CLI ", dom: "using-tdengine-cli" },
    ],
  },
  {
    name: "taosBenchmark",
    logo: "https://img1.baidu.com/it/u=3903506244,1663450695&fm=253&fmt=auto&app=138&f=GIF?w=500&h=310",
    desc: "The tool for benchmark testing of inserting or querying data.",
    docs: {
      zh: "",
      en: mdx.benchmarkEN,
    },
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
