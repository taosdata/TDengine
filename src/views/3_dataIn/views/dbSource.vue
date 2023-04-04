<template>
  <div class="dbsource">
    <component
      :is="currentName"
      :sourceList="sourceList"
      :dbsource="uidata"
      :editId="editId"
      :dbName="dbName"
      :tagName="tagName"
      :isEditable="isEditable"
    ></component>
  </div>
</template>
<script>
import DataSource from "./dataSource.vue";
import DbSourceUI from "./dbSourceUI.vue";
import OpcUI from "./opcUI.vue";
import { getUIData } from "@/api/explorer/datain";
import { Message } from "element-ui";

export default {
  name: "DbSource",
  components: {
    dbsource: DataSource,
    ui: DbSourceUI,
    opcui: OpcUI,
  },
  data() {
    return {
      tagName: "datasource",
      currentName: "dbsource",
      sourceList: [],
      uidata: null,
      editId: 0,
      dbName: "",
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
          this.sourceList =  result;
        });
        console.log(this.sourceList,'this.sourceList');
        this.$parent.$parent.$parent.sourceDisabled = false;
      } catch (error) {
        if (error.response.status == 404) {
          this.$parent.$parent.$parent.sourceDisabled = true;
        }
        if (error.response.status === 500) {
          this.$parent.$parent.$parent.sourceDisabled = true;
        }
      }
    },
    toggleComponent(type, id,editid, dbname) {
      // this.currentName = name;

      if (type) {
        //新增
        
        let data = this.sourceList.filter((item) => item.id === type);
        this.uidata = type=='opc'?data: this.deepClone(data);
        this.isEditable = false;
        switch (type) {
          case "tmq":
            this.currentName = "ui";
            this.tagName = "datasource";
            break;
          case "opc":
            this.currentName = "opcui";
            break;
          case "pi":
            this.currentName = "ui";
            this.tagName = "pi";
            break;
          case 'pitable':
            this.currentName='dbsource'
            this.tagName = "pi";
            break;
          case 'tmqtable':
            this.currentName='dbsource'
            this.tagName = "datasource";
            break;
          case 'opctable':
            this.currentName='dbsource'
            this.tagName = "opc";
        }
      } else {
        switch (id) {
          case "tmq":
            this.currentName = "ui";
            this.tagName = "datasource";
            break;
          case "opc":
            this.currentName = "opcui";
            this.tagName='opc'
            break;
          case "pi":
            this.currentName = "ui";
            this.tagName = "pi";
            break;
        }
        this.isEditable = true;
        this.editId = editid;
        this.dbName = dbname;
        this.getData();
        if (id === "tmq") {
          if (!this.uidata[0].protocol.value) {
            this.uidata[0].protocol.value =
              this.uidata[0].protocol.choices.filter((item) => {
                return item.display === "Native";
              })[0].name;
          }
        }
      }
      console.log(this.sourceList,type,id,this.currentName, "数据源呢");
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
            targetObj["value"] =
              targetObj.alternatives && targetObj.alternatives.length > 0
                ? targetObj.alternatives[0].name
                : "";
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
