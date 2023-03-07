const fs = require("fs");
const path = require("path");
const process = require("process");

let cus_prompt = process.env.CUS_PROMPT || "taos";
let cus_name = process.env.CUS_NAME || "TDengine";
let cus_email = process.env.CUS_EMAIL || "support@taosdata.com";

let data_path = path.join("src", "views", "0_login", "data.json");

if (cus_name === "TDengine" && cus_prompt === "taos") {
	let oem_data = path.join("scripts", "tdengine-data.json");
	fs.copyFileSync(oem_data, data_path);
} else {
	let oem_data = path.join("scripts", "oem-data.json");
	let data = fs.readFileSync(oem_data, "utf8");
	let newData = data.replaceAll("CUS_NAME", cus_name).replaceAll("CUS_EMAIL", cus_email).replaceAll("CUS_PROMPT", cus_prompt);
	fs.writeFileSync(data_path, newData);
}
