// const QPSGrid = {
//   left: 80,
//   bottom: 50,
// };
// 默认的y轴:当没有数据的时候显示
// import { parseTime } from "@/utils";
import i18n from "@/lang";
const yDefault = {
  max: null,
  min: null,
};
const axisLabel = {
  color: "rgb(81, 100, 189)",
  fontSize: 14,
};
const nameTextStyle = {
  color: "rgb(81, 100, 189)",
  fontSize: 16,
  fontFamily: "Amazon Ember, Helvetica Neue, Roboto, Arial, sans-serif",
  fontWeight: "bold",
};
const axisLine = {
  show: true,
  lineStyle: {
    color: "rgb(81, 100, 189)",
    width: 2,
  },
};
const legend = {
  bottom: 0,

  tooltip: {
    show: true,
    formatter: function ({ name }) {
      return i18n.t("dashboard.latencyDesc").replace("{per}", name.replace(/\D*/, ""));
    },
  },
  // left: 20,
};

// const resGrid = { left: 80, bottom: 50 };
const cpuGrid = {
  top: "30px",
  bottom: "45px",
  left: "80px",
  right: "0",
};
const axisPointer = {
  lineStyle: {
    width: 2,
  },
};
const yAxis = {
  axisLabel,
  type: "value",

  nameTextStyle,
  axisLine,
  splitNumber: 5,
};
// const offsetUTCTime = new Date().getTimezoneOffset() * 60 * 1000;
// 处理utc时间为当前时区的时间
export function handleTime(time) {
  return time;
  // if (!time) return "";
  // let date = new Date(time);
  // if (isNaN(date.getTime())) return time;
  // return parseTime(date.getTime() - offsetUTCTime, "YYYY-MM-DD kk:ss");
}
function handleValue(value) {
  if (!value) return 0;
  value = Number(value);
  if (isNaN(value)) return 0;
  if (String(value).indexOf(".") > -1) {
    return value.toFixed(value > 1 ? 2 : 4);
  } else {
    return value;
  }
}
// 获取cpu图
export function cpuUsage(data) {
  // let series = Object.keys(data)
  //   .filter(item => data[item])
  //   .map(item => {
  //     return {
  //       type: "line",
  //       name: item,
  //       data: data[item].map(ite => [handleTime(ite.ts), ite.gauge?.toFixed(2) || 0]),
  //       symbol: "none",
  //     };
  //   });
  let result = data.map(ite => [handleTime(ite.ts), handleValue(ite.gauge)]);
  let option = {
    title: {
      show: false,
    },
    tooltip: { trigger: "axis" },
    xAxis: {
      type: "time",
      axisLabel,
      axisLine,
      axisPointer,
    },
    grid: cpuGrid,
    yAxis: {
      ...yAxis,
      ...(data.length > 0 ? yDefault : { min: 0, max: 1, type: "value" }),
    },
    series: [
      {
        type: "line",
        name: "",
        data: result,
        smooth: true,
        showSymbol: false,
        selectedMode: "single",
      },
    ],
  };
  return [option, result.slice(-1)[0]];
}
export function memUsage(data) {
  let result = data.map(item => [handleTime(item.ts), handleValue(item.used)]);
  let option = {
    xAxis: {
      type: "time",
      axisLabel,
      axisLine,
      axisPointer,
    },
    tooltip: { trigger: "axis", showContent: false },
    grid: cpuGrid,
    yAxis: {
      ...yAxis,
      ...(data.length > 0 ? yDefault : { min: 0, max: 1, type: "value" }),
    },
    series: [
      {
        type: "line",
        name: "Memory",
        data: result,
        smooth: true,
        showSymbol: false,
        selectedMode: "single",
      },
    ],
  };
  return [option, result.slice(-1)[0]];
}
export function storageUsage(data = []) {
  let result = data?.map(item => [handleTime(item.ts), handleValue(item.gauge)]);
  let option = {
    tooltip: {
      trigger: "axis",
      // formatter(params) {
      //   let current_data = params[0].data;
      //   return current_data[2] + "-->" + current_data[1];
      // },
    },
    xAxis: {
      type: "time",
      axisLabel,
      axisLine,
      axisPointer,
    },
    grid: cpuGrid,
    yAxis: {
      ...yAxis,
      ...(data.length > 0 ? yDefault : { min: 0, max: 1, type: "value" }),
    },
    // legend,
    series: [
      {
        type: "line",
        name: "Storage",
        data: result,
        smooth: true,
        showSymbol: false,
        selectedMode: "single",
      },
    ],
  };
  return [option, result.slice(-1)[0]];
}
export function insertResTime(data) {
  data = Object.keys(data).map(item => {
    return {
      name: item,
      type: "line",
      showSymbol: false,
      sampling: "lttb",
      selectedMode: "single",
      smooth: true,
      // areaStyle: {},
      data: data[item].map(ite => [handleTime(ite.ts), handleValue(ite.value)]),
    };
  });
  let option = {
    tooltip: {
      trigger: "axis",
      // formatter(params) {
      //   let current_data = params[0].data;
      //   return current_data[2] + "-->" + current_data[1];
      // },
    },
    legend,
    grid: cpuGrid,
    xAxis: {
      type: "time",
      boundaryGap: false,
      axisLabel,
      nameTextStyle,
      axisLine,
      axisPointer,
    },
    yAxis: {
      ...yAxis,
      ...(data.length > 0 ? yDefault : { min: 0, max: 1, type: "value" }),
    },
    series: data,
  };
  return [option, data[0]?.data?.slice(-1)?.[0]];
}

export function queryResTime(data) {
  data = Object.keys(data).map(item => {
    return {
      name: item,
      type: "line",
      showSymbol: false,
      sampling: "lttb",
      selectedMode: "single",
      smooth: true,
      // areaStyle: {},
      data: data[item].map(ite => [handleTime(ite.ts), handleValue(ite.value)]),
    };
  });
  let option = {
    tooltip: {
      trigger: "axis",
      // formatter(params) {
      //   let current_data = params[0].data;
      //   return current_data[2] + "-->" + current_data[1];
      // },
    },
    legend,
    grid: cpuGrid,
    xAxis: {
      type: "time",
      boundaryGap: false,
      axisLabel,
      nameTextStyle,
      axisLine,
      axisPointer,
    },
    yAxis: {
      ...yAxis,
      ...(data.length > 0 ? yDefault : { min: 0, max: 1, type: "value" }),
    },
    series: data,
  };
  return [option, data[0]?.data?.slice(-1)[0]];
}

export function insertQPS(data) {
  data = data.map(item => [handleTime(item.ts), handleValue(item.gauge)]);
  let option = {
    tooltip: {
      trigger: "axis",
      // formatter(params) {
      //   let current_data = params[0].data;
      //   return current_data[2] + "-->" + current_data[1];
      // },
    },

    grid: cpuGrid,
    xAxis: {
      type: "time",
      boundaryGap: false,
      axisLabel,
      axisLine,
      axisPointer,
    },
    yAxis: {
      ...yAxis,
      ...(data.length > 0 ? yDefault : { min: 0, max: 1, type: "value" }),
    },
    series: [
      {
        name: "Inserts",
        type: "line",
        showSymbol: false,
        selectedMode: "single",
        sampling: "lttb",
        smooth: true,
        data,
      },
    ],
  };
  return [option, data.slice(-1)[0]];
}

export function queryQPS(data) {
  data = data.map(item => [handleTime(item.ts), handleValue(item.gauge)]);
  let option = {
    tooltip: {
      trigger: "axis",
    },
    grid: cpuGrid,
    xAxis: {
      type: "time",
      boundaryGap: false,
      axisLabel,
      axisLine,
      axisPointer,
    },
    yAxis: {
      ...yAxis,
      ...(data.length > 0 ? yDefault : { min: 0, max: 1, type: "value" }),
    },
    series: [
      {
        name: "Queries",
        type: "line",
        showSymbol: false,
        selectedMode: "single",
        sampling: "lttb",
        smooth: true,
        data,
      },
    ],
  };
  return [option, data.slice(-1)[0]];
}

// export function handleIO(data, $t) {
//   let read = {};
//   data.forEach(item => {
//     if (!read[item]) {
//       read[item.host] = [];
//     }
//     read[item.host].push([handleTime(item.ts), item.gauge]);
//   });
//   return Object.keys(read).map(item => {
//     return {
//       type: "line",
//       name: item + "-" + $t("dashboard.ioRead"),
//       data: read[item],
//       symbol: "none",
//     };
//   });
// }
