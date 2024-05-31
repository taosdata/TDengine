export default {
  data() {
    return {
      version_gt_3300: false,
      version_gt_3100: false,
      version_gt_3110: false,
    }
  },
  created() {
    this.handlecVersion();
  },
  methods: {
    handlecVersion() {
      let version = localStorage.getItem("agent_version");
      let [a, b, c, d] = version.split(".");
      if (a > 3 || (a == 3 && b >= 3) ){
        this.version_gt_3300 = true;
      }

      if (a > 3 || (a == 3 && b >= 1) || (a == 3 && b >= 1 && c >0 )){
        this.version_gt_3110 = true;
      }

      if (a > 3 || (a == 3 && b >= 1)){
        this.version_gt_3100 = true;
      }
     
    },
  }
}