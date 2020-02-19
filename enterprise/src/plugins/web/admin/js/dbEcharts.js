//var DbCurveColor = ['rgb(70,190,233)', 'rgb(255,160,34)', 'rgb(166,200,76)'];
//var DbCurveColor = ['rgb(109,202,238)', 'rgb(255,180,79)', 'rgb(183,210,108)'];
//var DbCurveColor = ['rgb(123,208,240)', 'rgb(255,201,128)', 'rgb(186,213,117)'];
//var DbCurveColor = ['rgb(143,215,241)', 'rgb(255,207,142)', 'rgb(199,220,141)'];

var DbCurveColor = ['rgb(109,202,238)', 'rgb(255,180,79)', 'rgb(183,210,108)'];
var DbAreaColor = ['rgb(143,215,241)', 'rgb(255,207,142)', 'rgb(199,220,141)'];

var dbIsUseTime = false;

var DbEchartOption = new function() {
	this.grid = function() {		
		return {
			top: 38,
			bottom: 46,
			left: 44,
			right: 15
		};
	};
	this.xAxis = function() {
		return {
			//type: 'time',
			type: 'category',
			splitLine: {
				show: true,
				lineStyle: {
					color: "#d4d4d4",
					type: "solid",
					width: 0.5
				},
				interval: function (index, value) {
					if (index == 0) {
						return false;
					}
					var date = new Date(value);
					var minute = date.getMinutes() * 1;
					var hour = date.getHours() * 1;
					var month = (date.getMonth()) * 1 + 1;
					var day = date.getDate() * 1;
					
					if (dbDnodeIntervalId == "last1hour") {
						if (minute % 15 == 0) return true;
					}
					else if (dbDnodeIntervalId == "last4hours") {
						if (minute == 0) return true;
					}
					else if (dbDnodeIntervalId == "last12hours") {
						if (hour % 3 == 0 && minute == 0) return true;
					}
					else if (dbDnodeIntervalId == "last1day") {
						if (hour % 6 == 0 && minute == 0) return true;
					}
					else if (dbDnodeIntervalId == "last7days") {
						if (hour == 0 && minute == 0) return true;
					}
					else if (dbDnodeIntervalId == "last30days") {
						if (day % 5 == 0 && hour == 0 && minute == 0) return true;
					}
					
					return "";				
				}
			},
			splitNumber : 4,
			axisLine : {
				show : false,
			},
			axisTick : {
				show : false,
			},
			axisLabel : {
				show : true,
				showMinLabel : false,
				showMaxLabel : false,
				interval: 0,
				formatter: function (value, index) {
					if (index == 0) {
						return "";
					}
					var date = new Date(value);
					var minute = date.getMinutes() * 1;
					var hour = date.getHours() * 1;
					var month = (date.getMonth()) * 1 + 1;
					var day = date.getDate() * 1;
					
					showHour = hour;
					showMonth = month;
					showDay = day;
					showMinute = minute;
					if (hour < 10) showHour = "0" + hour;
					if (minute < 10) showMinute = "0" + minute;
					if (day < 10) showDay = "0" + day;
					if (month < 10) showMonth = "0" + month;
					
					if (dbDnodeIntervalId == "last1hour") {
						if (minute % 15 == 0) return showHour + ":" + showMinute;
					}
					else if (dbDnodeIntervalId == "last4hours") {
						if (minute == 0) return showHour + ":" + showMinute;
					}
					else if (dbDnodeIntervalId == "last12hours") {
						if (hour % 3 == 0 && minute == 0) return showHour + ":" + showMinute;
					}
					else if (dbDnodeIntervalId == "last1day") {
						if (hour % 6 == 0 && minute == 0) return showHour + ":" + showMinute;
					}
					else if (dbDnodeIntervalId == "last7days") {
						if (hour == 0 && minute == 0) return showMonth + "-" + showDay;
					}
					else if (dbDnodeIntervalId == "last30days") {
						if (day % 5 == 0 && hour == 0 && minute == 0) return showMonth + "-" + showDay;
					}
					
					return "";
				}
			},
			
			data: [],
		};
	};
	
	this.title = function() {
		return {
			text: '',
			show: true,
			x: 'center',  
			y: 'top',
			padding : [12, 0, 0, 0],
			textStyle : {
				fontStyle: 'normal',
				fontSize: '12.5',
				fontWeight: '100'
			}
		};
	};	
};
var DbMonitorData = new function() {
	this.cache= {
		"status": "succ",
		"head": ["ts",
				"taosd_cpu", "system_cpu",
				"taosd_mem", "system_mem", "mem_total",
				"disk_used", "disk_total",
				"band_speed",
				"io_read", "io_write",
				"http", "select", "insert"
				],
		"data": [],
		"rows": 0
	};
	
	this.Clear = function() {
		this.cache.rows = 0;
		this.cache.data = [];
	};
	
	this.Update = function(d, type) {
		if (this.cache.rows < d.rows) {
			
			for (var row = this.cache.rows; row < d.rows; ++row) {
				this.cache.data[row] = [d.data[row][0].substring(0, 19), '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-'];
			}
			this.cache.rows = d.rows;
		}
		
		if (type == 'cpu') {
			for (var row = 0; row < d.rows; ++row) {
				this.cache.data[row][1] = d.data[row][1].toFixed(2) + "%";
				this.cache.data[row][2] = d.data[row][2].toFixed(2) + "%";
			}
		}
		else if (type == 'memory') {
			for (var row = 0; row < d.rows; ++row) {
				this.cache.data[row][3] = d.data[row][1].toFixed(1) + "MB";
				this.cache.data[row][4] = d.data[row][2].toFixed(1) + "MB";
				this.cache.data[row][5] = (d.data[row][3]/1024).toFixed(0) + "GB";
			}
		}
		else if (type == 'disk') {
			for (var row = 0; row < d.rows; ++row) {
				this.cache.data[row][6] = d.data[row][1].toFixed(1) + "GB";
				this.cache.data[row][7] = d.data[row][1].toFixed(0) + "GB";
			}
		}
		else if (type == 'bandwidth') {
			for (var row = 0; row < d.rows; ++row) {
				this.cache.data[row][8] = d.data[row][1].toFixed(2) + "Kb/s";
			}
		}
		else if (type == 'io') {
			for (var row = 0; row < d.rows; ++row) {
				this.cache.data[row][9] = d.data[row][1].toFixed(2) + "KB/s";;
				this.cache.data[row][10] = d.data[row][2].toFixed(2) + "KB/s";;
			}
		}
		else if (type == 'request') {
			for (var row = 0; row < d.rows; ++row) {
				this.cache.data[row][11] = d.data[row][1];
				this.cache.data[row][12] = d.data[row][2];
				this.cache.data[row][13] = d.data[row][3];
			}
		}
		
		DbCache.Clear();
		DbCache.OnData(this.cache);
	};
}

var DbCpuChart = new function() {
	this.maxVal = 0;
	this.graph = null;
	this.data = null;
	this.option = {
		backgroundColor: 'rgb(245,245,245)',
		animation: false,
		title: DbEchartOption.title(),
		grid: DbEchartOption.grid(),
		xAxis: DbEchartOption.xAxis(),
		tooltip:{
			trigger: 'axis',
			textStyle: {
				fontSize: 12,
			},
			formatter: function(value) 
			{ 
				var showdate = value[0].axisValueLabel.substring(5, 16);					
				if (dbIsUseTime) {
					return showdate + '<br>' 
						+ value[0].marker + value[0].seriesName + ' ' + value[0].data[1].toFixed(2) +'%<br>'
						+ value[1].marker + value[1].seriesName + ' ' + value[1].data[1].toFixed(2) +'%';
				}
				else {
					return showdate + '<br>' 
						+ value[0].marker + value[0].seriesName + ' ' + value[0].data.toFixed(2) +'%<br>'
						+ value[1].marker + value[1].seriesName + ' ' + value[1].data.toFixed(2) +'%';
				
				}
			} 
		},
		legend: {
			show: true,  
			x: 'left',  
			y: 'bottom',  
			data:['taosd', 'system'],
			padding : [0, 0, 9, 44],
		},
		yAxis: {
			name: '',
			nameTextStyle : {
				padding : [0, 0, 0, 58],
			},
			type: 'value',
			splitNumber: 5,
			min: 0,
			splitLine: {
				show: true,
				lineStyle: {
					color: "#d4d4d4",
					type: "solid",
					width: 0.5
				}
			},
			axisLine : {
				show : false,
			},
			axisTick : {
				show : false,
			},
			axisLabel : {
				show : true,
				showMinLabel : true,
				showMaxLabel : true,
				interval: 0,
				formatter: function (value, index) {	
					if (value == 0) {
						return "0%";
					}
					var showValue = value;
					if (DbCpuChart.maxVal <= 1) {
						showValue = value.toFixed(2);
					}
					else if (DbCpuChart.maxVal < 9) {
						showValue = value.toFixed(1);
					}
					
					showValue = showValue+"";
					if (showValue.length >= 6) {
						return showValue.substring(0, 5) + '%';
					}
					return showValue + '%';
				}
			},
		},
		series: [
			{
				name: 'taosd',
				type: 'line',
				data: [],		
				lineStyle: {
					normal: {
						type: 'solid',
						color:DbCurveColor[0],
						width: 1.5
					},
				},
				itemStyle:{
					normal:{
						color:DbCurveColor[0],
					}
				},
				areaStyle: {normal: {color: {
					type: 'linear',
					x: 0,
					y: 0,
					x2: 0,
					y2: 1,
					colorStops: [{
						offset: 0, color: DbAreaColor[0],
					}, {
						offset: 1, color: DbAreaColor[0],
					}],
					globalCoord: false
				}}},
				yAxisIndex: 0,
				smooth: true,
				smoothMonotone: 'x',
				hoverAnimation: false,
				showSymbol: false,
				symbolSize: 5
			},
			{
				name: 'system',
				type: 'line',
				data: [],		
				lineStyle: {
					normal: {
						type: 'solid',
						color:DbCurveColor[1],
						width: 1.5
					},
				},
				itemStyle:{
					normal:{
						color:DbCurveColor[1],
					}
				},
				yAxisIndex: 0,
				smooth: true,
				smoothMonotone: 'x',
				hoverAnimation: false,
				showSymbol: false,
				symbolSize: 5,
			}
		]
	};	
	
	this.Init = function(divid)
	{
		this.graph = echarts.init(document.getElementById(divid));
		this.graph.setOption(this.option);
		this.graph.resize();		
	};  

	this.OnData = function(d)
	{
		if (d == null) {
			return;
		}
		this.data = d;
		this.option.series[0].data = [];
		this.option.series[1].data = [];
		this.option.xAxis.data = [];
		this.maxVal = 0;
		
		var length = d.data.length;
		for (var row = 0; row < length; ++row) {
			var data = d.data[row]; 	
			this.option.xAxis.data[row] = data[0];
			if (dbIsUseTime) {
				this.option.series[0].data[row] = [data[0],data[1]];
				this.option.series[1].data[row] = [data[0],data[2]];				
			}
			else {
				this.option.series[0].data[row] = data[1];
				this.option.series[1].data[row] = data[2];
			}
			if (data[3]*1 != 0)
				this.option.title.text = "cpu usage(cores:" + data[3] + ")";
			this.maxVal = Math.max(this.maxVal, data[1]);
			this.maxVal = Math.max(this.maxVal, data[2]);
		}
		
		this.graph.setOption(this.option);
		this.graph.resize();		
	}
};

var DbMemoryChart = new function() {
	this.maxVal = 0;
	this.graph = null;
	this.data = null;
	this.option = {
		backgroundColor: 'rgb(245,245,245)',
		animation: false,
		title: DbEchartOption.title(),
		grid: DbEchartOption.grid(),
		xAxis: DbEchartOption.xAxis(),
		tooltip:{
			trigger: 'axis',
			textStyle: {
				fontSize: 12,
			},
			formatter: function(value) 
			{ 
				var showdate = value[0].axisValueLabel.substring(5, 16);
				if (dbIsUseTime) {
					return showdate + '<br>' 
						+ value[0].marker + value[0].seriesName + ' ' + value[0].data[1].toFixed(2) +' GB<br>'
						+ value[1].marker + value[1].seriesName + ' ' + value[1].data[1].toFixed(2) +' GB';
				}
				else {
					return showdate + '<br>' 
						+ value[0].marker + value[0].seriesName + ' ' + value[0].data.toFixed(2) +' GB<br>'
						+ value[1].marker + value[1].seriesName + ' ' + value[1].data.toFixed(2) +' GB';
				}
			} 
		},
		legend: {
			show: true,  
			x: 'left',  
			y: 'bottom',  
			data:['taosd', 'system'],
			padding : [0, 0, 9, 44],
		},
		yAxis: {
			name: '',
			nameTextStyle : {
				padding : [0, 0, 0, 47],
			},
			type: 'value',
			splitNumber: 5,
			min: 0,
			splitLine: {
				show: true,
				lineStyle: {
					color: "#d4d4d4",
					type: "solid",
					width: 0.5
				}
			},
			axisLine : {
				show : false,
			},
			axisTick : {
				show : false,
			},
			axisLabel : {
				show : true,
				showMinLabel : true,
				showMaxLabel : true,
				interval: 0,
				formatter: function (value, index) {
					return dbGetValue(value, DbMemoryChart.maxVal);
				}
			},
		},
		series: [
			{
				name: 'taosd',
				type: 'line',
				data: [],		
				lineStyle: {
					normal: {
						type: 'solid',
						color:DbCurveColor[0],
						width: 1.5
					},
				},
				itemStyle:{
					normal:{
						color:DbCurveColor[0],
					}
				},
				areaStyle: {normal: {color: {
					type: 'linear',
					x: 0,
					y: 0,
					x2: 0,
					y2: 1,
					colorStops: [{
						offset: 0, color: DbAreaColor[0],
					}, {
						offset: 1, color: DbAreaColor[0],
					}],
					globalCoord: false
				}}},
				yAxisIndex: 0,
				smooth: true,
				smoothMonotone: 'x',
				hoverAnimation: false,
				showSymbol: false,
				symbolSize: 5
			},
			{
				name: 'system',
				type: 'line',
				data: [],		
				lineStyle: {
					normal: {
						type: 'solid',
						color:DbCurveColor[1],
						width: 1.5
					},
				},
				itemStyle:{
					normal:{
						color:DbCurveColor[1],
					}
				},
				yAxisIndex: 0,
				smooth: true,
				smoothMonotone: 'x',
				hoverAnimation: false,
				showSymbol: false,
				symbolSize: 5,
			}
		]
	};	
	
	this.Init = function(divid)
	{
		this.graph = echarts.init(document.getElementById(divid));
		this.graph.setOption(this.option);
		this.graph.resize();		
	};  

	this.OnData = function(d)
	{
		if (d == null) {
			return;
		}
		this.data = d;
		this.option.series[0].data = [];
		this.option.series[1].data = [];
		this.option.xAxis.data = [];
		this.maxVal = 0;
		
		var length = d.data.length;
		for (var row = 0; row < length; ++row) {
			var data = d.data[row]; 	
			this.option.xAxis.data[row] = data[0];	
			if (dbIsUseTime) {
				this.option.series[0].data[row] = [data[0],data[1]/1000];
				this.option.series[1].data[row] = [data[0],data[2]/1000];				
			}
			else {
				this.option.series[0].data[row] = data[1]/1000;
				this.option.series[1].data[row] = data[2]/1000;
			}
			if (data[3]*1 != 0)
				this.option.title.text = "memory(total:" + (data[3]/1000).toFixed(1) +"GB)";
			this.maxVal = Math.max(this.maxVal, data[1]/1000);
			this.maxVal = Math.max(this.maxVal, data[2]/1000);
		}	
		
		this.graph.setOption(this.option);
		this.graph.resize();		
	}
};

var DbDiskChart = new function() {
	this.maxVal = 0;
	this.graph = null;
	this.data = null;
	this.option = {
		backgroundColor: 'rgb(245,245,245)',
		animation: false,
		title: DbEchartOption.title(),
		grid: DbEchartOption.grid(),
		xAxis: DbEchartOption.xAxis(),
		tooltip:{
			trigger: 'axis',
			textStyle: {
				fontSize: 12,
			},
			formatter: function(value) 
			{ 
				var showdate = value[0].axisValueLabel.substring(5, 16);
				if (dbIsUseTime) {
					return showdate + '<br>' 
						+ value[0].marker + value[0].seriesName + ' ' + value[0].data[1].toFixed(2) +' GB';
				}
				else {
					return showdate + '<br>' 
					+ value[0].marker + value[0].seriesName + ' ' + value[0].data.toFixed(2) +' GB';
				}
			} 
		},
		legend: {
			show: true,  
			x: 'left',  
			y: 'bottom',  
			data:['system'],
			padding : [0, 0, 9, 44],
		},		
		yAxis: {
			name: '',
			nameTextStyle : {
				padding : [0, 0, 0, 61],
			},
			type: 'value',
			splitNumber: 5,
			min: 0,
			splitLine: {
				show: true,
				lineStyle: {
					color: "#d4d4d4",
					type: "solid",
					width: 0.5
				}
			},
			axisLine : {
				show : false,
			},
			axisTick : {
				show : false,
			},
			axisLabel : {
				show : true,
				showMinLabel : true,
				showMaxLabel : true,
				interval: 0,
				formatter: function (value, index) {
					return dbGetValue(value, DbDiskChart.maxVal);
				}
			},
		},
		series: [
			{
				name: 'system',
				type: 'line',
				data: [],		
				lineStyle: {
					normal: {
						type: 'solid',
						color:DbCurveColor[0],
						width: 1.5
					},
				},
				itemStyle:{
					normal:{
						color:DbCurveColor[0],
					}
				},
				areaStyle: {normal: {color: {
					type: 'linear',
					x: 0,
					y: 0,
					x2: 0,
					y2: 1,
					colorStops: [{
						offset: 0, color: DbAreaColor[0],
					}, {
						offset: 1, color: DbAreaColor[0],
					}],
					globalCoord: false
				}}},
				yAxisIndex: 0,
				smooth: true,
				smoothMonotone: 'x',
				hoverAnimation: false,
				showSymbol: false,
				symbolSize: 5
			}
		]
	};	
	
	this.Init = function(divid)
	{
		this.graph = echarts.init(document.getElementById(divid));
		this.graph.setOption(this.option);
		this.graph.resize();		
	};  

	this.OnData = function(d)
	{
		if (d == null) {
			return;
		}
		this.data = d;
		this.option.series[0].data = [];
		this.option.xAxis.data = [];
		this.maxVal = 0;
		
		var length = d.data.length;
		for (var row = 0; row < length; ++row) {
			var data = d.data[row]; 	
			this.option.xAxis.data[row] = data[0];	
			if (dbIsUseTime) {
				this.option.series[0].data[row] = [data[0],data[1]];			
			}
			else {
				this.option.series[0].data[row] = data[1];
			}
			if (data[2]*1 != 0)
				this.option.title.text = "diskspace(total:" + (data[2]*1).toFixed(0) +"GB)";
			this.maxVal = Math.max(this.maxVal, data[1]);
		}
		
		this.graph.setOption(this.option);
		this.graph.resize();		
	}
};

var DbBandWidthChart = new function() {
	this.maxVal = 0;
	this.graph = null;
	this.data = null;
	this.option = {
		backgroundColor: 'rgb(245,245,245)',
		animation: false,
		title: DbEchartOption.title(),
		grid: DbEchartOption.grid(),
		xAxis: DbEchartOption.xAxis(),
		tooltip:{
			trigger: 'axis',
			textStyle: {
				fontSize: 12,
			},
			formatter: function(value) 
			{ 
				var showdate = value[0].axisValueLabel.substring(5, 16);
				if (dbIsUseTime) {
					return showdate + '<br>' 
						+ value[0].marker + value[0].seriesName + ' ' + value[0].data[1].toFixed(3) +' Kb/s';
				}
				else {
					return showdate + '<br>' 
						+ value[0].marker + value[0].seriesName + ' ' + value[0].data.toFixed(3) +' Kb/s';
				}
			} 
		},
		legend: {
			show: true,  
			x: 'left',  
			y: 'bottom',  
			data:['system'],
			padding : [0, 0, 9, 44],
		},
		yAxis: {
			name: '',
			nameTextStyle : {
				padding : [0, 0, 0, 60],
			},
			type: 'value',
			splitNumber: 5,
			min: 0,
			splitLine: {
				show: true,
				lineStyle: {
					color: "#d4d4d4",
					type: "solid",
					width: 0.5
				}
			},
			axisLine : {
				show : false,
			},
			axisTick : {
				show : false,
			},
			axisLabel : {
				show : true,
				showMinLabel : true,
				showMaxLabel : true,
				interval: 0,
				formatter: function (value, index) {
					return dbGetValue(value, DbBandWidthChart.maxVal);
				}
			},
		},
		series: [
			{
				name: 'system',
				type: 'line',
				data: [],		
				lineStyle: {
					normal: {
						type: 'solid',
						color:DbCurveColor[0],
						width: 1.5
					},
				},
				itemStyle:{
					normal:{
						color:DbCurveColor[0],
					}
				},
				areaStyle: {normal: {color: {
					type: 'linear',
					x: 0,
					y: 0,
					x2: 0,
					y2: 1,
					colorStops: [{
						offset: 0, color: DbAreaColor[0],
					}, {
						offset: 1, color: DbAreaColor[0],
					}],
					globalCoord: false
				}}},
				yAxisIndex: 0,
				smooth: true,
				smoothMonotone: 'x',
				hoverAnimation: false,
				showSymbol: false,
				symbolSize: 5
			}
		]
	};	
	
	this.Init = function(divid)
	{
		this.graph = echarts.init(document.getElementById(divid));
		this.graph.setOption(this.option);
		this.graph.resize();		
	};  

	this.OnData = function(d)
	{
		if (d == null) {
			return;
		}
		this.data = d;
		this.option.series[0].data = [];
		this.option.xAxis.data = [];
		this.maxVal = 0;
		
		var length = d.data.length;
		for (var row = 0; row < length; ++row) {
			var data = d.data[row]; 	
			this.option.xAxis.data[row] = data[0];	
			if (dbIsUseTime) {
				this.option.series[0].data[row] = [data[0],data[1]];			
			}
			else {
				this.option.series[0].data[row] = data[1];
			}
			this.maxVal = Math.max(this.maxVal, data[1]);
		}	
		this.option.title.text = "bandwidth";
		this.graph.setOption(this.option);
		this.graph.resize();		
	}
};

var DbIoChart = new function() {
	this.maxVal = 0;
	this.graph = null;
	this.data = null;
	this.option = {
		backgroundColor: 'rgb(245,245,245)',
		animation: false,
		title: DbEchartOption.title(),
		grid: DbEchartOption.grid(),
		xAxis: DbEchartOption.xAxis(),
		tooltip:{
			trigger: 'axis',
			textStyle: {
				fontSize: 12,
			},
			formatter: function(value) 
			{ 
				var showdate = value[0].axisValueLabel.substring(5, 16);
				if (dbIsUseTime) {
					return showdate + '<br>' 
						+ value[0].marker + value[0].seriesName + ' ' + value[0].data[1].toFixed(2) +' KB/s<br>'
						+ value[1].marker + value[1].seriesName + ' ' + value[1].data[1].toFixed(2) +' KB/s';
				}
				else {
					return showdate + '<br>' 
						+ value[0].marker + value[0].seriesName + ' ' + value[0].data.toFixed(2) +' KB/s<br>'
						+ value[1].marker + value[1].seriesName + ' ' + value[1].data.toFixed(2) +' KB/s';
				}
			} 
		},
		legend: {
			show: true,  
			x: 'left',  
			y: 'bottom',  
			data:['read', 'write'],
			padding : [0, 0, 9, 44],
		},
		yAxis: {
			name: '',
			nameTextStyle : {
				padding : [0, 0, 0, 37],
			},
			type: 'value',
			splitNumber: 5,
			min: 0,
			splitLine: {
				show: true,
				lineStyle: {
					color: "#d4d4d4",
					type: "solid",
					width: 0.5
				}
			},
			axisLine : {
				show : false,
			},
			axisTick : {
				show : false,
			},
			axisLabel : {
				show : true,
				showMinLabel : true,
				showMaxLabel : true,
				interval: 0,
				formatter: function (value, index) {
					return dbGetValue(value, DbIoChart.maxVal);
				}
			},
		},
		series: [
			{
				name: 'read',
				type: 'line',
				stack: 'a',
				data: [],		
				lineStyle: {
					normal: {
						type: 'solid',
						color:DbCurveColor[0],
						width: 1.5
					},
				},
				itemStyle:{
					normal:{
						color:DbCurveColor[0],
					}
				},
				yAxisIndex: 0,
				smooth: true,
				smoothMonotone: 'x',
				hoverAnimation: false,
				areaStyle: {normal: {color: {
					type: 'linear',
					x: 0,
					y: 0,
					x2: 0,
					y2: 1,
					colorStops: [{
						offset: 0, color: DbAreaColor[0],
					}, {
						offset: 1, color: DbAreaColor[0],
					}],
					globalCoord: false
				}}},
				showSymbol: false,
				symbolSize: 5
			},
			{
				name: 'write',
				type: 'line',
				stack: 'a',
				data: [],		
				lineStyle: {
					normal: {
						type: 'solid',
						color:DbCurveColor[1],
						width: 1.5
					},
				},
				itemStyle:{
					normal:{
						color:DbCurveColor[1],
					}
				},
				yAxisIndex: 0,
				smooth: true,
				smoothMonotone: 'x',
				hoverAnimation: false,
				areaStyle: {normal: {}},
				showSymbol: false,
				symbolSize: 5,
			}
		]
	};	
	
	this.Init = function(divid)
	{
		this.graph = echarts.init(document.getElementById(divid));
		this.graph.setOption(this.option);
		this.graph.resize();		
	};  

	this.OnData = function(d)
	{
		if (d == null) {
			return;
		}
		this.data = d;
		this.option.series[0].data = [];
		this.option.series[1].data = [];
		this.option.xAxis.data = [];
		this.maxVal = 0;
		
		var length = d.data.length;
		for (var row = 0; row < length; ++row) {
			var data = d.data[row]; 	
			this.option.xAxis.data[row] = data[0];	
			if (dbIsUseTime) {
				this.option.series[0].data[row] = [data[0],data[1]];
				this.option.series[1].data[row] = [data[0],data[2]];				
			}
			else {
				this.option.series[0].data[row] = data[1];
				this.option.series[1].data[row] = data[2];
			}
			this.maxVal = Math.max(this.maxVal, data[1] + data[1]);
		}	
		
		this.option.title.text = "disk io";
		this.graph.setOption(this.option);
		this.graph.resize();		
	}
};

var DbRequestChart = new function() {
	this.maxVal = 0;
	this.graph = null;
	this.data = null;
	this.option = {
		backgroundColor: 'rgb(245,245,245)',
		animation: false,
		title: DbEchartOption.title(),
		grid: DbEchartOption.grid(),
		xAxis: DbEchartOption.xAxis(),
		tooltip:{
			trigger: 'axis',
			textStyle: {
				fontSize: 12,
			},
			formatter: function(value) 
			{ 
				var showdate = value[0].axisValueLabel.substring(5, 16);
				if (dbIsUseTime) {
					return showdate + '<br>' 
						+ value[2].marker + value[2].seriesName + ' ' + value[2].data[1] +'<br>'
						+ value[1].marker + value[1].seriesName + ' ' + value[1].data[1] +'<br>'
						+ value[0].marker + value[0].seriesName + ' ' + value[0].data[1];
				}
				else {
					return showdate + '<br>' 
						+ value[2].marker + value[2].seriesName + ' ' + value[2].data +'<br>'
						+ value[1].marker + value[1].seriesName + ' ' + value[1].data +'<br>'
						+ value[0].marker + value[0].seriesName + ' ' + value[0].data;
				}
			} 
		},
		legend: {
			show: true,  
			x: 'left',  
			y: 'bottom',  
			data:['http', 'select', 'insert'],
			padding : [0, 0, 9, 44],
		},
		yAxis: {
			name: '',
			nameTextStyle : {
				padding : [0, 0, 0, 44],
			},
			type: 'value',
			splitNumber: 5,
			min: 0,
			splitLine: {
				show: true,
				lineStyle: {
					color: "#d4d4d4",
					type: "solid",
					width: 0.5
				}
			},
			axisLine : {
				show : false,
			},
			axisTick : {
				show : false,
			},
			axisLabel : {
				show : true,
				showMinLabel : true,
				showMaxLabel : true,
				interval: 0,
				formatter: function (value, index) {
					return dbGetValue(value, DbRequestChart.maxVal);
				}
			},
		},
		series: [
			{
				name: 'insert',
				type: 'line',
				data: [],	
				stack: 'a',	
				lineStyle: {
					normal: {
						type: 'solid',
						color:DbCurveColor[2],
						width: 1.5
					},
				},
				itemStyle:{
					normal:{
						color:DbCurveColor[2],
					}
				},
				areaStyle: {normal: {color: {
					type: 'linear',
					x: 0,
					y: 0,
					x2: 0,
					y2: 1,
					colorStops: [{
						offset: 0, color: DbAreaColor[2],
					}, {
						offset: 1, color: DbAreaColor[2],
					}],
					globalCoord: false
				}}},
				yAxisIndex: 0,
				smooth: true,
				smoothMonotone: 'x',
				hoverAnimation: false,
				showSymbol: false,
				symbolSize: 5,
			},
			{
				name: 'select',
				type: 'line',
				data: [],	
				stack: 'a',	
				lineStyle: {
					normal: {
						type: 'solid',
						color:DbCurveColor[1],
						width: 1.5
					},
				},
				itemStyle:{
					normal:{
						color:DbCurveColor[1],
					}
				},
				areaStyle: {normal: {color: {
					type: 'linear',
					x: 0,
					y: 0,
					x2: 0,
					y2: 1,
					colorStops: [{
						offset: 0, color: DbAreaColor[1],
					}, {
						offset: 1, color: DbAreaColor[1],
					}],
					globalCoord: false
				}}},
				yAxisIndex: 0,
				smooth: true,
				smoothMonotone: 'x',
				hoverAnimation: false,
				showSymbol: false,
				symbolSize: 5,
			},
			{
				name: 'http',
				type: 'line',
				data: [],	
				stack: 'a',				
				lineStyle: {
					normal: {
						type: 'solid',
						color:DbCurveColor[0],
						width: 1.5
					},
				},
				itemStyle:{
					normal:{
						color:DbCurveColor[0],
					}
				},
				areaStyle: {normal: {color: {
					type: 'linear',
					x: 0,
					y: 0,
					x2: 0,
					y2: 1,
					colorStops: [{
						offset: 0, color: DbAreaColor[0],
					}, {
						offset: 1, color: DbAreaColor[0],
					}],
					globalCoord: false
				}}},
				yAxisIndex: 0,
				smooth: true,
				smoothMonotone: 'x',
				hoverAnimation: false,
				showSymbol: false,
				symbolSize: 5
			}
		]
	};	
	
	this.Init = function(divid)
	{
		this.graph = echarts.init(document.getElementById(divid));
		this.graph.setOption(this.option);
		this.graph.resize();		
	};  

	this.OnData = function(d)
	{
		if (d == null) {
			return;
		}
		this.data = d;
		this.option.series[0].data = [];
		this.option.series[1].data = [];
		this.option.series[2].data = [];
		this.option.xAxis.data = [];
		this.maxVal = 0;
		
		var length = d.data.length;
		for (var row = 0; row < length; ++row) {
			var data = d.data[row]; 	
			this.option.xAxis.data[row] = data[0]; 	
			if (dbIsUseTime) {
				this.option.series[0].data[row] = [data[0],data[3]];
				this.option.series[1].data[row] = [data[0],data[2]];	
				this.option.series[2].data[row] = [data[0],data[1]];					
			}
			else {
				this.option.series[0].data[row] = data[3];
				this.option.series[1].data[row] = data[2];
				this.option.series[2].data[row] = data[1];
			}
			this.maxVal = Math.max(this.maxVal, data[1]+data[2]+data[3]);
		}	
		
		this.option.title.text = "request";
		this.graph.setOption(this.option);
		this.graph.resize();		
	}
};

function dbGetValue(value, maxVal)
{
	if (value == 0) {
		return "0";
	}
	if (maxVal < 1) {
		return value.toFixed(2);
	}
	else if (maxVal < 5) {
		return value.toFixed(1);
	}
	else if (maxVal < 6000) {
		return value.toFixed(0);
	}
	else if (maxVal < 6000000) {
		return (value / 1000).toFixed(0) + 'K';
	}
	else if (maxVal < 6000000000) {
		return (value / 1000000).toFixed(0) + 'M';
	}
	else {
		return (value / 1000000000).toFixed(0) + 'G';
	}
	
	return value;
}

