let result = [];
let share_data = #{};

for (k, i) in data.keys() {
	if (k.len == 5 && (k.starts_with("U0") || k.starts_with("U1") || k.starts_with("U2"))) {
		let ymd = data["DATA_DATE"].split('-');
		if ymd[1].len == 1 { ymd[1] = "0" + ymd[1] } 
		if ymd[2].len == 1 { ymd[2] = "0" + ymd[2] } 
		let item = #{"_ts": `${ymd[0]}-${ymd[1]}-${ymd[2]} ${k.sub_string(1,2)}:${k.sub_string(3,2)}:00`, "_value": data[k]};
		result.push(item);
	} else if (k != "DATA_DATE") {
		share_data.set(k, data[k]);
	}
}

for (item, i) in result {
	result[i] += share_data;
}

result