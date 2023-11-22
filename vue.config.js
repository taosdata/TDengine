const path = require("path");
const CompressionPlugin = require("compression-webpack-plugin");
let arg = {};
const argArr = process.argv.splice(2);
argArr.forEach((item, index) => {
  if (item.startsWith("--")) {
    arg[item.slice(2)] = argArr[index + 1];
  }
});
function resolve(dir) {
  return path.join(__dirname, dir);
}
const plugins = [];
if (arg.mode != "dev") {
  plugins.push(
    new CompressionPlugin({
      test: /\.js$|\.html$|\.css/,
      threshold: 10240,
      minRatio: 0.8,
      deleteOriginalAssets: false,
    })
  );
}

module.exports = {
  publicPath: "/",
  outputDir: "dist",
  assetsDir: "static",
  productionSourceMap: arg.mode=='dev'?true:false,
  configureWebpack: {
    resolve: {
      alias: {
        'element-ui':resolve('node_modules/element-ui'),
        "@": resolve("src"),
        assets: resolve("src/assets"),
        components: resolve("src/components"),
        views: resolve("src/views"),
        router: resolve("src/router"),
        store: resolve("src/store"),
        utils: resolve("src/utils"),
        api: resolve("src/api"),
        public: resolve("public"),
      },
    },
    module: {
      rules: [
        {
          test: /\.mjs$/,
          include: /node_modules/,
          type: "javascript/auto",
          use: ["babel-loader"],
        },
      ],
    },
    // externals: {
    //   vue: "Vue",
    //   "vue-router": "VueRouter",
    //   vuex: "Vuex",
    //   axios: "axios",
    //   "vue-i18n": "VueI18n",
    //   "element-ui": "ELEMENT",
    //   echarts: "echarts",
    //   "js-cookie": "Cookies",
    //   "vue-codemirror": "VueCodemirror",
    // },
    plugins,
  },
  css: {
    loaderOptions: {
      scss: {
        additionalData: `
					@import "@/styles/global.scss";
				`,
      },
      sass:{
        sassoptions:{
          javascriptEnabled:true
        }
      }
    },
  },
  chainWebpack(config) {
    // 配置svg-sprite-loader
    // 自动将assets/fonts/svg文件夹下的svg代码都放入symbol标签中,
    // 并且设置id为icon-加文件名
    // svg-sprite-loader会自动完成svg图标的注册
    config.module.rule("svg").exclude.add(resolve("src/assets/fonts/svg"));
    config.module
      .rule("icon")
      .test(/\.svg$/)
      .include.add(resolve("src/assets/fonts/svg"))
      .end()
      .use("svg-sprite-loader")
      .loader("svg-sprite-loader")
      .options({
        symbolId: "icon-[name]",
      });
  },
};
