<template>
  <div class="dbsource">
    <component
      :is="currentName"
      :sourceList="sourceList"
      :dbsourceList="uidata"
      :dbsource="uidata"
      :editId="editId"
      :dbName="dbName"
      :tagName="tagName"
      :protocol="protocol"
      :isEditable="isEditable"
      ref="table"
    ></component>
    <MqttConnector></MqttConnector>
  </div>
</template>
<script>
import DataSource from "./dataSource.vue";
import DbSourceUI from "./dbSourceUI.vue";
import OpcUI from "./opcUI.vue";
import MqttConnector from '../components/mqttConnector.vue'
import { getUIData } from "@/api/explorer/datain";

export default {
  name: "DbSource",
  components: {
    dbsource: DataSource,
    ui: DbSourceUI,
    opcui: OpcUI,
    MqttConnector
  },
  data() {
    return {
      protocol: "ua", //只针对opc的ua/da
      tagName: "datasource",
      currentName: "",
      sourceList: [],
      uidata: null,
      editId: 0,
      dbName: "",
      isEditable: false,
      agentID: "",
    };
  },
  created() {
    this.getData();
  },
  methods: {
    async getData() {
      try {
        await getUIData().then((result) => {
          this.sourceList = result;
        });
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
    toggleComponent(type, id, editid, dbname) {
      // this.currentName = name;
      if (type) {
        //新增

        let data = this.sourceList.filter((item) => item.id === type);
        this.uidata = type == "opc" ? data : this.deepClone(data);
        this.isEditable = false;
        switch (type) {
          case "tmq":
            this.currentName = "ui";
            this.tagName = "datasource";
            break;
          case "opcua":
            this.currentName = "opcui";
            this.protocol = "ua";
            break;
          case "opcda":
            this.currentName = "opcui";
            this.protocol = "da";
            break;
          case "pi":
            this.currentName = "ui";
            this.tagName = "pi";
            break;
          case "influxdb":
            this.currentName = "ui";
            this.tagName = "influxdb";
            break;
          case "pitable":
            this.currentName = "dbsource";
            this.tagName = "pi";
            break;
          case "tmqtable":
            this.currentName = "dbsource";
            this.tagName = "datasource";
            break;
          case "opctable":
            this.currentName = "dbsource";
            this.tagName = "opc";
            break;
          case "mqtt":
            console.log('新增mqtt----');
            this.currentName = "opcui";
            this.tagName = "mqtt";
            break;
          case "pibackfill":
            this.currentName = "ui";
            this.tagName = "pibackfill";
          break;
        }
      } else {
        switch (id) {
          case "tmq":
            this.currentName = "ui";
            this.tagName = "datasource";
            break;
          case "opcua":
            this.currentName = "opcui";
            this.tagName = "opc";
            this.protocol = "ua";
            break;
          case "opcda":
            this.currentName = "opcui";
            this.tagName = "opc";
            this.protocol = "da";
            break;
          case "pi":
            this.currentName = "ui";
            this.tagName = "pi";
            break;
          case "influxdb":
            this.currentName = "ui";
            this.tagName = "influxdb";
            break;
          case "mqtt":
            this.currentName = "opcui";
            this.tagName = "mqtt";
            break;
          case "pibackfill":
            this.currentName = "ui";
            this.tagName = "pibackfill";
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
                : undefined;
          }
          if (keys === "protocol") {
            targetObj.protocol["value"] = targetObj.protocol.choices.filter(
              (o) => o.name == "--"
            )[0]?.name;
          }
        } else {
          targetObj[keys] = source[keys];
          if (!Object.hasOwnProperty.call(targetObj,"value")) {
            targetObj["value"] = undefined;
          }
        }
      });
      return targetObj;
    },
    reloadTable() {
      if (this.currentName == "dbsource") {
        this.$nextTick(() => {
          this.$refs.table.refresh();
        });
      }
    },
  },
};
</script>
