<template>
  <div class="dbsource">
    <component
      :is="currentName"
      :uidata="uidata"
      :editId="editId"
      :dbName="dbName"
      :tagName="tagName"
      :protocol="protocol"
      :isEditable="isEditable"
      :sourceName="sourceName"
      ref="table"
    ></component>
  </div>
</template>
<script>
import DataSource from "./dataSourceTable.vue";
import DbSourceUI from "./detailUI.vue";

export default {
  name: "DbSource",
  components: {
    dbsource: DataSource,
    ui: DbSourceUI,
  },
  data() {
    return {
      sourceName: "",
      protocol: "ua", //只针对opc的ua/da
      tagName: "datasource",
      currentName: "",
      sourceList: [],
      uidata: null,
      editId: 0,
      dbName: "",
      isEditable: false,
      agentID: "",
      mqttParser: null,
      staticParser: null,
      staticOpc: null,
      currentTaskStatus: "",
    };
  },
  created() {
    // this.getData();
  },
  methods: {
    toggleComponent(type, id, editid, dbname) {
      if (type) {
        //新增
        this.isEditable = false;
        switch (type) {
          case "tmq":
            this.currentName = "ui";
            this.tagName = "datasource";
            break;
        }
      } else {
        switch (id) {
          case "tmq":
            this.currentName = "ui";
            this.tagName = "datasource";
            break;
        }
        this.isEditable = true;
        this.editId = editid;
        this.dbName = dbname;
        // this.getData();
      }
    },
    hasProp(obj, key) {
      return Object.hasOwnProperty.call(obj, key);
    },
    reloadTable() {
      if (this.currentName == "dbsource") {
        this.$nextTick(() => {
          this.$refs.table.refresh();
        });
      }
    },
  }
};
</script>
