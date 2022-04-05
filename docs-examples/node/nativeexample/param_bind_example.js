const taos = require("td2.0-connector");

const conn = taos.connect({
  host: "localhost",
});
