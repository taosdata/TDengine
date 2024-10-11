export default {
  data() {
    return {
      version_gt_3300: false,
      version_gt_3100: false,
      version_gt_3110: false,
    };
  },
  created() {
    this.handlerVersion();
  },
  methods: {
    handlerVersion() {
      let version = localStorage.getItem("agent_version");
      let [a, b, c, d] = version.split(".");
      if (a > 3 || (a == 3 && b >= 3)) {
        this.version_gt_3300 = true;
      }

      if (a > 3 || (a == 3 && b > 1) || (a == 3 && b >= 1 && c > 0)) {
        // 3.1.1.0 增加的 VARBINARY 数据类型
        this.version_gt_3110 = true;
      } else {
        // 小于这个版本，不支持 VARBINARY
        this.filterDataType(["VARBINARY"]);
      }

      if (a > 3 || (a == 3 && b >= 1 && c >= 0)) {
        // 3.1.0.0 增加的 Geometry 数据类型
        this.version_gt_3100 = true;
      } else {
        // 小于这个版本 VARBINARY/GEOMETRY 两个类型都不支持
        this.filterDataType(["VARBINARY", "GEOMETRY"]);
      }
    },
    filterDataType(types) {
      this.dataType = this.dataType.filter((item) => !types.includes(item.value));
      this.tagType = this.tagType.filter((item) => !types.includes(item.value));
    },
  },
};
