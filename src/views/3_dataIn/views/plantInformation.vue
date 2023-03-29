<template>
  <div class="plant-info">
    <component
      :is="currentComp"
      :dbsource="dbsource"
      :dbName="dbName"
      :tagName="'pi'"
      :isEditable="isEditable"
    ></component>
    <!-- <DbSourceUI :dbsource='dbsource' :tagName='"pi"'></DbSourceUI> -->
  </div>
</template>
<script>
// import dbsource from "./datasource.json";
import DbSourceUI from "./dbSourceUI.vue";
import PITable from "./piTable.vue";
import { getUIData } from "@/api/explorer/datain";
import { Message } from "element-ui";
export default {
  name: "PlantInformation",
  components: {
    ui: DbSourceUI,
    pitable: PITable,
  },
  data() {
    return {
      currentComp: "pitable",
      dbsource: [],
      dbName: "",
      isEditable: false,
    };
  },
  created() {
    this.getUIDatas();
  },
  methods: {
    toggleComponent(name, db) {
      if (db) {
        this.isEditable = true;
        this.dbName = db;
      }
      this.currentComp = name;
    },
    async getUIDatas() {
      try {
        await getUIData().then((result) => {
          this.dbsource = result.filter((item) => item.id === "pi");
        });
        console.log(this.dbsource, "获取pi的配置文件---");
      } catch (error) {
        Message.error(error);
      }
    },
  },
};
</script>