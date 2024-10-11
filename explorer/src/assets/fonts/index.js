const load = require.context("./svg", true, /\.svg$/);
load.keys().map(load);