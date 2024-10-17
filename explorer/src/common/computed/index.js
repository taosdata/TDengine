import { OFFSETUTCTIME } from "@/const";
// 设置UTC为当前时间的时区
function getDate() {
  return new Date(Date.now() + OFFSETUTCTIME);
}
// 公共计算属性
export default {
  pickerOptions() {
    return {
      shortcuts: [
        {
          text: this.$t("agoHour"),
          onClick: picker => {
            const end = getDate();
            const start = getDate();
            const selectTime = start.getTime() - 3600 * 1000 * 1;
            start.setTime(selectTime);
            picker.$emit("pick", [start, end]);
          },
        },
        {
          text: this.$t("yesterday"),
          onClick: picker => {
            const end = getDate();
            const start = getDate();
            const selectTime = start.getTime() - 3600 * 1000 * 24;
            start.setTime(selectTime);
            picker.$emit("pick", [start, end]);
          },
        },
        {
          text: this.$t("agoWeek"),
          onClick: picker => {
            const end = getDate();
            const start = getDate();
            const selectTime = start.getTime() - 3600 * 1000 * 24 * 7;
            start.setTime(selectTime);
            picker.$emit("pick", [start, end]);
          },
        },
        {
          text: this.$t("agoMonth"),
          onClick: picker => {
            const end = getDate();
            const start = getDate();
            const selectTime = start.getTime() - 3600 * 1000 * 24 * 30;
            start.setTime(selectTime);
            picker.$emit("pick", [start, end]);
          },
        },
      ],
      disabledDate: date => {
        const currentTime = Date.now() + OFFSETUTCTIME;
        const dateTime = +new Date(date);
        return dateTime > currentTime;
      },
    };
  },
  timeList() {
    return [
      {
        label: this.$t("agoHour"),
        value: 60 * 60 * 1000,
      },
      {
        label: this.$t("agoDay"),
        value: 24 * 60 * 60 * 1000,
      },
      {
        label: this.$t("agoWeek"),
        value: 7 * 24 * 60 * 60 * 1000,
      },
      {
        label: this.$t("agoMonth"),
        value: 30 * 24 * 60 * 60 * 1000,
      },
      {
        label: this.$t("agoQuarter"),
        value: 90 * 24 * 60 * 60 * 1000,
      },
    ];
  },
};
