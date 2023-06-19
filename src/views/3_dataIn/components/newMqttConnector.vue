<template>
  <div class="connector">
    <div class="json-zone">
      <ul class="header">
        <li>
          <span>
            {{ $t("datasource.primarykey") }}
          </span>
          <el-tooltip
            effect="light"
            :content="$t('datasource.primarytip')"
            placement="right-start"
          >
            <i class="el-icon-info"></i>
          </el-tooltip>
        </li>
        <li>
          <span>
            {{ $t("datasource.ascolumn") }}
          </span>
          <el-tooltip
            effect="light"
            :content="$t('datasource.selectfieldtip')"
            placement="right-start"
          >
            <i class="el-icon-info"></i>
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
            <i class="el-icon-info"></i>
          </el-tooltip>
        </li>
        <li>{{ $t("datasource.colname") }}</li>
        <li>{{ $t("datasource.rename") }}</li>
        <li>{{ $t("datasource.coltype") }}</li>
        <li></li>
      </ul>
      <div class="col-content">
        <span style="color:red;font-size:24px;">
          {{
            fields.filter((item) => item.name != 'payload').length
          }}
          {{connectorData.parse.payload.json}}
        </span>
        <Mqttcolumn
          v-for="item in fields.filter((item) => item.name != 'payload')"
          :key="item.name"
          :colData="item"
          @changePrimary="changePrimary"
          @changeAddStatus="changeAddStatus"
          :isEditable="isEditable"
          ref="staticmqtt"
        >
        <!-- <template #localindex> 
          <span style="color:orange;font-size:20px;">{{item.name}}</span>
        </template> -->
        </Mqttcolumn>
        <Mqttcolumn
          v-for="(item, index) in connectorData.parse.payload.json"
          :key="index"
          :index="index"
          :colData="item"
          @deleteRow="deleteRow"
          @changePrimary="changePrimary"
          :isEditable="isEditable"
          @changeAddStatus="changeAddStatus"
          ref="mqtt"
        >
        <!-- <template #localindex> 
          <span style="color:orange;font-size:20px;">{{index}}</span>
        </template> -->
        </Mqttcolumn>
      </div>
      <div class="footer">
        <el-button
          icon="el-icon-plus"
          size="small"
          type="primary"
          :disabled="disable"
          plain
          @click="addRow"
        ></el-button>
        <el-tooltip
          effect="light"
          :content="$t('datasource.addmqtttip')"
          placement="right-start"
          style="position: absolute; right: 5px"
        >
          <i class="el-icon-info"></i>
        </el-tooltip>
      </div>
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
import { deepClone } from "@/utils";
import Mqttcolumn from "./newMqttColumn.vue";
export default {
  name: "NewMqttConnector",
  components: { Mqttcolumn },
  provide() {
    return {
      currentKey: this.currentKey,
    };
  },
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
    isEditable: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      currentKey: {
        primary: "ts",
      },
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
        using: [
          {
            required: true,
            message: this.$t("datasource.usingtip"),
            trigger: "blur",
          },
        ],
      },
    };
  },
  methods: {
    changePrimary(data) {
      this.currentKey.primary = data;
    },
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
    deleteRow(ind, name) {
      this.$confirm(this.$t("datasource.delcol"), {
        confirmButtonText: this.$t("datasource.ok"),
        cancelButtonText: this.$t("datasource.cancel"),
        type: "warning",
      }).then(() => {
        let oldData = deepClone(this.$store.state.app.mqttParser);
        oldData.parse.payload.json.splice(ind, 1);

        if (name) {
          let columns = oldData.model.columns;
          let tags = oldData.model.tags;
          if (columns.includes(name)) {
            columns.splice(columns.indexOf(name), 1);
          }
          if (tags.includes(name)) {
            tags.splice(tags.indexOf(name), 1);
          }
          if (this.currentKey.primary == name) {
            //删的是主键，只有在新增时候才可以删除主键
            if (!columns.includes("ts")) {
              columns.unshift("ts");
              this.currentKey.primary = "ts";
              this.$refs.staticmqtt[0].changePrimary("ts");
            }
          }
          this.disable = false;
        }
        this.$store.commit("app/SET_MQTT_PARSER", oldData);
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
    if(this.connectorData.model.columns.length > 0){
      this.currentKey.primary = this.connectorData.model.columns[0]
    }
    // this.currentKey.primary =
    //   this.connectorData.model.columns.length > 0
    //     ? this.connectorData.model.columns[0]
    //     : "ts";
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
  .el-icon-info {
    color: #4259ce;
    margin-top: 0px;
    margin-left: 4px;
  }

  .header {
    display: grid;
    grid-template-columns: 2fr 2fr 2fr 3fr 3fr 3fr 0.5fr;
    column-gap: 10px;
    background-color: #f5f7fa;
    border: 1px solid #ebeef5;
    border-bottom: none;
    padding-top: 5px;
    padding-bottom: 5px;
    li {
      display: flex;
      justify-content: center;
      white-space: nowrap;
      color: #909399;
      font-size: 16px;
      align-items: center;
      text-align: center;
    }
  }
  .footer {
    .el-button {
      width: 96%;
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
