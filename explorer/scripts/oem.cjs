const fs = require("fs");
const path = require("path");
const process = require("process");
const child_process = require("child_process");

let cmd = process.argv[2] || "build";
let mode = process.argv[3] || "prd";

let cus_prompt = process.env.CUS_PROMPT || "taos";
let cus_name = process.env.CUS_NAME || "TDengine TSDB";
let cus_email = process.env.CUS_EMAIL || "support@taosdata.com";

let data_path = path.join("src", "views", "0_login", "data.json");
let cus_config = process.env.CUS_CONFIG;
let cus_config_data;
if (cus_config) {
	if (fs.existsSync(cus_config)) {
		cus_config_data = fs.readFileSync(cus_config, "utf-8");
	} else {
		try {
			let data = JSON.parse(cus_config);
			cus_config_data = data;
		} catch {
			console.error("CUS_CONFIG is not a json file or json string");
		}
	}
}

if (cus_name.includes("TDengine") || cus_prompt === "taos") {
	let oem_data = path.join("scripts", "tdengine-data.json");
	fs.copyFileSync(oem_data, data_path);
	cus_config_data ||= fs.readFileSync(data_path, "utf-8");
} else {
	let oem_data = path.join("scripts", "oem-data.json");
	let data = fs.readFileSync(oem_data, "utf8");
	let newData = data.replace(/CUS_NAME/g, cus_name).replace(/CUS_EMAIL/g, cus_email).replaceAll(/CUS_PROMPT/g, cus_prompt);
	fs.writeFileSync(data_path, newData);
	cus_config_data ||= newData;
}

process.env.VITE_APP_CUS_CONFIG = cus_config_data;
console.log('output:', "vite " + cmd + " --mode " + mode);

child_process.execSync("vite " + cmd + " --mode " + mode, { stdio: "inherit", shell: true })
