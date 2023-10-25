<template>
  <div class="dbsource">
    <component
      :is="currentName"
      :sourceList="sourceList"
      :dbsource="uidata"
      :editId="editId"
      :dbName="dbName"
      :tagName="tagName"
      :protocol="protocol"
      :mqttParser="mqttParser"
      :constMqttparser="parserobj"
      :opcConfig="opcConfig"
      :isEditable="isEditable"
      :echoData="echoData"
      :sourceName="sourceName"
      @setEditData="setEditData"
      :isCopyable="isCopyable"
      ref="table"
    ></component>
  </div>
</template>
<script>
import DataSource from "./dataSource.vue";
import DbSourceUI from "./dbSourceUI.vue";
import OpcUI from "./opcUI.vue";

import { getUIData, getTask } from "@/api/explorer/datain";
import constparser from "./mqttparser.json";
import constOpc from "./opcconfig.json";
import { deepClone } from "@/utils";
const opcDefaultChecked = ["value", "original_ts"];
export default {
  name: "DbSource",
  components: {
    dbsource: DataSource,
    ui: DbSourceUI,
    opcui: OpcUI,
  },
  data() {
    return {
      sourceName: "",
      opcConfig: constOpc,
      parserobj: constparser,
      protocol: "ua", //只针对opc的ua/da
      tagName: "datasource",
      currentName: "",
      sourceList: [],
      uidata: [],
      editId: 0,
      dbName: "",
      isEditable: false,
      isCopyable: false,
      agentID: "",
      mqttParser: null,
      staticParser: null,
      staticOpc: null,
      currentTaskStatus: "",
      echoData: deepClone(opcDefaultChecked),
    };
  },
  created() {
    this.staticParser = deepClone(constparser);
    this.staticOpc = deepClone(constOpc);
    this.getData();
  },
  methods: {
    //设置编辑时候的数据
    setEditData(data) {
      this.uidata = deepClone(data);
    },
    //回显opc的数据
    echoOpcData() {
      let opcconfigData = this.uidata[0].datasets.categories.filter(
        (item) => item.name == this.$t("datasource.opcconfig")
      )[0].category[0];

      if (!opcconfigData.value) {
        opcconfigData.value = JSON.stringify(constOpc);
      }
      this.echoData = deepClone(
        JSON.parse(opcconfigData.value).column_configs.map(
          (item) => item.column_name
        )
      );
      let others = deepClone(this.staticOpc).column_configs.filter(
        (item) => !this.echoData.includes(item.column_name)
      );
      let newEcho = {
        column_configs: deepClone(
          JSON.parse(opcconfigData.value).column_configs.concat(others)
        ),
        stable_prefix: JSON.parse(opcconfigData.value).stable_prefix,
      };
      let result = ["received_ts", "original_ts", "value", "quality"].map(
        (item) => {
          let res = deepClone(
            JSON.parse(opcconfigData.value).column_configs.concat(others)
          ).filter((val) => {
            if (val.column_name == item) {
              return val;
            }
          })[0];
          return res;
        }
      );

      JSON.parse(opcconfigData.value).column_configs = deepClone(result);
      this.$store.commit("app/SET_OPC_CONFIG", {
        column_configs: result,
        stable_prefix: JSON.parse(opcconfigData.value).stable_prefix,
      });

      opcconfigData.value = JSON.stringify(newEcho);

      this.opcConfig = deepClone(JSON.parse(opcconfigData.value));
      this.opcConfig.column_configs = deepClone(result);
    },
    async getData() {
      try {
        let result = await getUIData();
        this.$set(this, "sourceList", result);
      } catch (error) {
        console.log(error);
      }
    },
    changeEditable(val) {
      this.isEditable = val;
    },
    setEditID(val) {
      this.editId = val;
    },
    toggleComponent(type, id, editid, dbname) {
      if (type && !this.isEditable) {
        //新增
        let data = this.sourceList.filter((item) => item.id === type);
        if (type == "mqtt" || type == "kafka") {
          // this.uidata = this.deepClone(data);
          this.$set(this.uidata, 0, this.deepClone(data)[0]);
          this.parserobj = deepClone(this.staticParser);
          this.parserobj.model.columns.push("ts"); //默认新增时候选中ts列
          this.$store.commit("app/SET_MQTT_PARSER", this.parserobj);
        } else {
          // this.uidata = type == "opc" ? data : this.deepClone(data);
          if (type == "opc") {
            this.$set(this.uidata, 0, data[0]);
          } else {
            this.$set(this.uidata, 0, this.deepClone(data)[0]);
          }
          this.opcConfig = deepClone(this.staticOpc);
          this.echoData = deepClone(opcDefaultChecked);
          this.$store.commit("app/SET_OPC_CONFIG", this.opcConfig);
        }
        this.isEditable = false;
        switch (type) {
          case "tmq":
            this.currentName = "ui";
            this.tagName = "datasource";
            break;
          case "opcua":
            this.currentName = "opcui";
            this.tagName = "opcua";
            this.protocol = "ua";
            break;
          case "opcda":
            this.currentName = "opcui";
            this.tagName = "opcda";
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
          case "opentsdb":
            this.currentName = "ui";
            this.tagName = "opentsdb";
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
            this.currentName = "opcui";
            this.tagName = "mqtt";
            break;
          case "pibackfill":
            this.currentName = "ui";
            this.tagName = "pibackfill";
            break;
          case "csv":
            this.currentName = "opcui";
            this.tagName = "csv";
            break;
          case "taos":
            this.currentName = "ui";
            this.tagName = "taos";
            break;
          case "kafka":
            this.currentName = "opcui";
            this.tagName = "kafka";
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
            // if (this.$store.state.app.opcnodesfiles.length == 0) {
            // this.echoOpcData();
            // }

            break;
          case "opcda":
            this.currentName = "opcui";
            this.tagName = "opc";
            this.protocol = "da";
            // this.echoOpcData();
            break;
          case "pi":
            this.currentName = "ui";
            this.tagName = "pi";
            break;
          case "influxdb":
            this.currentName = "ui";
            this.tagName = "influxdb";
            break;
          case "opentsdb":
            this.currentName = "ui";
            this.tagName = "opentsdb";
            break;
          case "mqtt":
            this.currentName = "opcui";
            this.tagName = "mqtt";

            this.uidata[0].parser.fields = this.uidata[0].parser.fields.map(
              (item) => {
                if (item.name == "payload") {
                  item["value"] = "json";
                }
                return item;
              }
            );

            break;
          case "pibackfill":
            this.currentName = "ui";
            this.tagName = "pibackfill";
            break;
          case "csv":
            (this.currentName = "opcui"), (this.tagName = "csv");
            break;
          case "taos":
            this.currentName = "ui";
            this.tagName = "taos";
            break;
          case "kafka":
            this.currentName = "opcui";
            this.tagName = "kafka";

            this.uidata[0].parser.fields = this.uidata[0].parser.fields.map(
              (item) => {
                if (item.name == "value") {
                  item["value"] = "json";
                  item["name"] = "payload";
                }
                return item;
              }
            );

            break;
        }

        this.isEditable = true;
        this.dbName = dbname;
        this.getData();
        if (id === "taos") {
          if (!this.uidata[0].protocol.value) {
            this.uidata[0].protocol.value =
              this.uidata[0].protocol.choices.filter((item) => {
                return item.display === this.$t("datasource.tmqprotocol");
              })[0]?.name;
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
          if (!Object.hasOwnProperty.call(targetObj, "value")) {
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
  watch: {
    "$i18n.locale": {
      deep: true,
      async handler(val) {
        if (this.isEditable) {
          let result = await getTask(
            localStorage.getItem("local_clusterID"),
            "datain"
          );
          let data = result.filter((item) => item.id == this.editId)[0]?.from_detail
;
          this.$set(this, "uidata", [].concat(data));
        } else {
          await this.getData();
          let data = this.sourceList.filter(
            (item) => item.id === this.$store.state.app.currentDBType
          );
          this.$set(this, "uidata", deepClone(data));
        }
      },
    },
    "$store.state.app.mqttParser": {
      deep: true,
      handler(val) {
        this.parserobj = val;
      },
    },
    "$store.state.app.opcConfig": {
      deep: true,
      handler(val) {
        this.opcConfig = val;
      },
    },
    "$store.state.app.currentDBType": {
      deep: true,
      handler(val) {
        this.toggleComponent(val);
      },
    },
  },
};
</script>
<style lang="scss" scoped>
.dbsource {
  margin-top: 10px;
}
</style>
