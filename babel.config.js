let plugins = ["@babel/plugin-proposal-optional-chaining", "@babel/plugin-proposal-nullish-coalescing-operator",
  // "primjs",{
  //   "languages":["bash","shell","sh","java"],
  //   "plugins":["line-numbers"],
  //   "theme":"okaidia",
  //   "css":true
  // }
];
if (process.env.NODE_ENV === "prd") {
  plugins.push("transform-remove-console");
}
module.exports = {
  presets: ["@vue/cli-plugin-babel/preset"],
  plugins,
};
