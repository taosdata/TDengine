<template>
  <div class="dbsource">
    <component
      :is="currentName"
      :sourceList="sourceList"
      :dbsource="uidata"
      :editId='editId'
      :dbName='dbName'
      :isEditable="isEditable"
    ></component>
  </div>
</template>
<script>
import DataSource from "./dataSource.vue";
import DbSourceUI from "./dbSourceUI.vue";
import { getUIData } from "@/api/explorer/datain";
import { Message } from "element-ui";
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
      editId:0,
      dbName:'',
      isEditable: false,
    };
  },
  created() {
    this.getData();
  },
  methods: {
    async getData() {
      try {
        await getUIData().then((result) => {
          console.log(result,'数据源====');
          this.sourceList = result.filter(val=>val.id==='tmq');
        });
        this.$parent.$parent.$parent.sourceDisabled=false
      } catch (error) {
        if(error.response.status==404){
          this.$parent.$parent.$parent.sourceDisabled=true
        }
        if(error.response.status===500){
          this.$parent.$parent.$parent.sourceDisabled=true
        }
      }
    },
    toggleComponent(name, type, id,dbname) {
      this.currentName = name;

      if (type) {
        //新增
        let data = this.sourceList.filter((item) => item.id === type);
        this.uidata = this.deepClone(data);
        this.isEditable = false;
      } else {
        this.isEditable = true;
        this.editId=id
        this.dbName=dbname
        this.getData();
        if (!this.uidata[0].protocol.value) {
          this.uidata[0].protocol.value =
            this.uidata[0].protocol.choices.filter((item) => {
             return  item.display === "Native";
            })[0].name;
        }
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
          if (keys === "alternatives") {
            targetObj["value"] = targetObj.alternatives[0].name;
          }
          if (keys === "protocol") {
            targetObj.protocol["value"] = targetObj.protocol.choices.filter(
              (o) => o.name == "--"
            )[0].name;
          }
        } else {
          targetObj[keys] = source[keys];
          if (!targetObj.hasOwnProperty.call("value")) {
            targetObj["value"] = null;
          }
        }
      });
      return targetObj;
    },
  },
};
</script>
