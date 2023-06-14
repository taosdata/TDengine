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
      :mqttParser="mqttParser"
      :constMqttparser="parserobj"
      :opcConfig="opcConfig"
      :isEditable="isEditable"
      :echoData="echoData"
      ref="table"
    ></component>
  </div>
</template>
<script>
import DataSource from "./dataSource.vue";
import DbSourceUI from "./dbSourceUI.vue";
import OpcUI from "./opcUI.vue";
import { getUIData } from "@/api/explorer/datain";
import constparser from "./mqttparser.json";
import constOpc from "./opcconfig.json";
import { deepClone } from "@/utils";
const opcDefaultChecked=["value", "received_time"]
export default {
  name: "DbSource",
  components: {
    dbsource: DataSource,
    ui: DbSourceUI,
    opcui: OpcUI,
  },
  data() {
    return {
      opcConfig: constOpc,
      parserobj: constparser,
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
      echoData: opcDefaultChecked,
    };
  },
  created() {
    this.staticParser = deepClone(constparser);
    this.staticOpc = deepClone(constOpc);
    this.getData();
  },
  methods: {
    //回显opc的数据
    echoOpcData() {
      let opcconfigData=this.uidata[0].groups.filter(item=>item.name==this.$t('datasource.opcconfig'))[0].params[0]
      this.echoData = deepClone(
        JSON.parse(opcconfigData.value).column_configs.map(
          (item) => item.column_name
        )
      );
      let others = this.staticOpc.column_configs.filter(
        (item) => !this.echoData.includes(item.column_name)
      );
      let newEcho = {
        column_configs: deepClone(
          JSON.parse(opcconfigData.value
          ).column_configs.concat(others)
        ),
        stable_prefix: JSON.parse(opcconfigData.value)
          .stable_prefix,
      };
      this.$store.commit("app/SET_OPC_CONFIG", {
        column_configs: deepClone(
          JSON.parse(
            opcconfigData.value
          ).column_configs.concat(others)
        ),
        stable_prefix: JSON.parse(opcconfigData.value)
          .stable_prefix,
      });

      opcconfigData.value = JSON.stringify(newEcho);
      this.opcConfig = deepClone(
        JSON.parse(opcconfigData.value)
      );
    },
    async getData() {
      try {
        await getUIData().then((result) => {
          this.sourceList = result;
        });
        this.$parent.$parent.$parent.sourceDisabled = false;
      } catch (error) {
        if (error.response && error.response.status == 404) {
          this.$parent.$parent.$parent.sourceDisabled = true;
        }
        if (error.response && error.response.status === 500) {
          this.$parent.$parent.$parent.sourceDisabled = true;
        }
      }
    },
    toggleComponent(type, id, editid, dbname) {
      // this.currentName = name;
      if (type) {
        //新增

        let data = this.sourceList.filter((item) => item.id === type);
        if (type == "mqtt") {
          this.uidata = this.deepClone(data);
          this.parserobj = deepClone(this.staticParser);
          this.$store.commit("app/SET_MQTT_PARSER", this.parserobj);
        } else {
          this.uidata = type == "opc" ? data : this.deepClone(data);
          this.opcConfig = deepClone(this.staticOpc);
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
            this.echoData=deepClone(opcDefaultChecked)
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
            this.echoOpcData();
            break;
          case "opcda":
            this.currentName = "opcui";
            this.tagName = "opc";
            this.protocol = "da";
            this.echoOpcData();
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
        }
        this.isEditable = true;
        this.editId = editid;
        this.dbName = dbname;
        this.getData();
        if (id === "tmq") {
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
  },
};
</script>
