<template>
  <div class="opc-info">
    <component
      :is="currentComp"
      :dbsource="dbsource"
      :tagName="'pi'"
      :isEditable="isEditable"
    ></component>
    <!-- <DbSourceUI :dbsource='dbsource' :tagName='"pi"'></DbSourceUI> -->
  </div>
</template>
<script>
// import dbsource from "./opc.json";
import DbSourceUI from "./opcUI.vue";
import OpcTable from "./opcTable.vue";
import { getUIData } from "@/api/explorer/datain";
import { Message } from "element-ui";
export default {
  name: "OpcView",
  components: {
    ui: DbSourceUI,
    opctable: OpcTable,
  },
  data() {
    return {
      currentComp: "opctable",
      dbsource: [],
      isEditable:false
    };
  },
  created(){
    this.getUIDatas()
  },
  methods: {
    toggleComponent(name) {
      this.currentComp = name;
    },
    async getUIDatas() {
      try {
        await getUIData().then((result) => {
          this.dbsource = result.filter(item=>item.id==='opc');
        });
        console.log(this.dbsource,'获取的配置文件---');
      } catch (error) {
        Message.error(error);
      }
    },
  },
};
</script>