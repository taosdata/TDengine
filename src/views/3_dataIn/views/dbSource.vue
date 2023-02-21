<template>
  <div class="dbsource">
    <component
      :is="currentName"
      :sourceList="sourceList"
      :dbsource="uidata"
    ></component>
  </div>
</template>
<script>
import DataSource from "./dataSource.vue";
import DbSourceUI from "./dbSourceUI.vue";
export default {
  name: "DbSource",
  components: {
    dbsource: DataSource,
    ui: DbSourceUI,
  },
  data() {
    return {
      currentName: "dbsource",
      sourceList: [],
      uidata: null,
    };
  },
  created() {
    this.getData();
  },
  methods: {
    async getData() {
      try {
        await fetch("http://192.168.0.201:6050/ds/in", {
          method: "get",
        })
          .then((res) => res.json())
          .then((result) => {
            this.sourceList = result;
            console.log(result, "jieguo-----");
          });
      } catch (error) {
        console.log(error);
      }
    },
    toggleComponent(name, id) {
      this.currentName = name;
      if (id) {
        let data = this.sourceList.filter((item) => item.id === id);
        this.uidata = this.deepClone(data);
        console.log(this.uidata, "神拷贝");
      }else{
        this.getData()
      }
    },
    hasProp(obj, key) {
      return Object.hasOwnProperty.call(obj, key);
    },
    //给需要输入的项目加value字段
    deepClone(source) {
      if (!source && typeof source !== "object") {
        throw new Error("error arguments", "deepClone");
      }
      const targetObj = source.constructor === Array ? [] : {};
      Object.keys(source).forEach((keys) => {
        if (source[keys] && typeof source[keys] === "object") {
          targetObj[keys] = this.deepClone(source[keys]);
        } else {
          targetObj[keys] = source[keys];
          console.log(this.hasProp(targetObj, "required"), "判断有误required");
          targetObj["value"] = "";
        }
      });
      return targetObj;
    },
  },
};
</script>