<template>
  <div class="connector">
    <div class="json-zone">
      <ul class="header">
        <li>
          <span>
            {{ $t("datasource.ascolumn") }}
          </span>
          <el-tooltip
            effect="light"
            :content="$t('datasource.selectfieldtip')"
            placement="right-start"
          >
            <i class="el-icon-info" style="color: #4259ce"></i>
          </el-tooltip>
        </li>
        <li>
          <span>
            {{ $t("datasource.astag") }}
          </span>
          <el-tooltip
            effect="light"
            :content="$t('datasource.selectfieldtip')"
            placement="right-start"
          >
            <i class="el-icon-info" style="color: #4259ce"></i>
          </el-tooltip>
        </li>
        <li>{{ $t("datasource.colname") }}</li>
        <li>{{ $t("datasource.rename") }}</li>
        <li>{{ $t("datasource.coltype") }}</li>
        <li></li>
      </ul>
      <div class="col-content">
        <Mqttcolumn
          v-for="item in fields.filter((item) => item.name != 'payload')"
          :key="item.name"
          :colData="item"
          @deleteRow="deleteRow"
          @changeAddStatus="changeAddStatus"
        >
        </Mqttcolumn>
        <Mqttcolumn
          v-for="(item, index) in connectorData.parse.payload.json"
          :key="index"
          :index="index"
          :colData="item"
          @deleteRow="deleteRow"
          @changeAddStatus="changeAddStatus"
          ref="mqtt"
        >
        </Mqttcolumn>
      </div>
      <el-button
        icon="el-icon-plus"
        size="small"
        type="primary"
        :disabled="disable"
        plain
        @click="addRow"
      ></el-button>
    </div>

    <el-form
      label-width="200px"
      :model="connectorData.model"
      :rules="rules"
      ref="ruleForm"
    >
      <el-form-item
        v-for="(item, index) in Object.keys(connectorData.model).filter(
          (item) => visiblecols.includes(item)
        )"
        :key="index"
        :prop="item"
      >
        <span slot="label">
          <el-tooltip
            effect="light"
            :content="$t('datasource.createsubtbtip')"
            placement="right-start"
            v-if="item != 'using'"
          >
            <i class="el-icon-info"></i>
          </el-tooltip>

          {{ $t(`datasource.${item}`) }}
        </span>

        <el-input v-model="connectorData.model[item]"></el-input>
      </el-form-item>
    </el-form>
  </div>
</template>
<script>
import Mqttcolumn from "./mqttColumn.vue";
export default {
  name: "MqttConnector",
  components: { Mqttcolumn },
  props: {
    connectorData: {
      type: Object,
      default: () => {
        return null;
      },
    },
    fields: {
      type: Array,
      default: () => {
        return [];
      },
    },
  },
  data() {
    return {
      showSuperTip: false,
      visiblecols: ["using", "name"],
      visible: true,
      disable: false,
      nameisnull: true,
      rules: {
        name: [
          {
            required: true,
            message: this.$t("datasource.subtip"),
            trigger: "blur",
          },
        ],
      },
    };
  },
  methods: {
    getTagOrColumn(event, val) {
      this.connectorData.model[val] = event.split(",");
    },
    changeAddStatus() {
      this.$nextTick(() => {
        this.disable = Array.from(this.$refs.mqtt).some(
          (item) => item.addStatus
        );
      });
    },
    deleteRow(ind) {
      this.$confirm(this.$t("datasource.delcol"), {
        confirmButtonText: this.$t("datasource.ok"),
        cancelButtonText: this.$t("datasource.cancel"),
        type: "warning",
      }).then(() => {
        let oldData = this.$store.state.app.mqttParser;
        oldData.parse.payload.json.splice(ind, 1);
        this.$store.commit("app/SET_MQTT_PARSER", oldData);
        this.disable = false;
      });
    },
    addRow() {
      let oldData = this.$store.state.app.mqttParser;
      oldData.parse.payload.json.push({
        name: "",
        alias: "",
        cast: "",
      });
      this.$store.commit("app/SET_MQTT_PARSER", oldData);
      this.changeAddStatus();
    },
    closeMqttDialog() {
      this.$emit("closeMqttDialog");
    },
    submit() {
      let tags = this.$store.state.app.mqttParser.model.tags;
      let supername = this.connectorData.model["using"];
      if ((tags.length > 0 && !supername) || (tags.length == 0 && supername)) {
        this.showSuperTip = true;
      } else {
        this.showSuperTip = false;
      }
      this.$refs["ruleForm"].validate((valid) => {
        if (valid) {
          this.nameisnull = false;
        } else {
          this.nameisnull = true;
          return false;
        }
      });
    },
  },
  mounted() {
    this.changeAddStatus();
  },
};
</script>
<style lang="scss" scoped>
.json-zone {
  display: flex;
  flex-direction: column;
  overflow: hidden;
  max-height: 250px;
  //   padding-left: 200px;
  margin-bottom: 15px;
  position: relative;
  .header {
    display: grid;
    grid-template-columns: 1fr 1fr 1fr 1fr 1fr 0.5fr;
    column-gap: 10px;
    background-color: #f5f7fa;
    border: 1px solid #ebeef5;
    border-bottom: none;
    padding-top: 5px;
    padding-bottom: 5px;
    li {
      color: #909399;
      font-size: 16px;
      text-align: center;
    }
  }
  .col-content {
    overflow: auto;
    border: none;
    margin-bottom: 15px;

    border: 1px solid #ebeef5;
  }
  .info {
    position: absolute;
    top: 45px;
    left: 0px;
    cursor: pointer;
    i {
      font-size: 25px;
      color: #4259ce;
    }
  }
}
::v-deep {
  .el-form-item__label {
    text-align: left;
    color: #4259ce;
    font-size: 14px;
  }
}
.header-cont {
  margin-bottom: 10px;
}
.block-title {
  font-size: 16px;
  color: #4259ce;
  font-weight: 600;
}
</style>