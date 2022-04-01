const taos = require("td2.0-connector");

const conn = taos.connect({
  host: "localhost",
});

// 未完成, 等待 TD-14448 解决
