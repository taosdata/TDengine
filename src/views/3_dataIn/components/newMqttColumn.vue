<template>
  <ul
    :class="[
      'mqtt-column',
      isEditable &&
      (this.nonEditableCols.includes(colData['name']) ||
        colData['name'] == currentKey.primary)
        ? 'edit'
        : '',
    ]"
  >
    <li class="primary">
      <el-checkbox
        :value="colData['name'] == currentKey.primary"
        @change="changePrimary(colData['name'])"
        :disabled="
          ['topic', 'qos'].includes(colData['name']) ||
          !colData['name'] ||
          (!colData['cast'] && colData['name'] != 'ts') ||
          (colData['cast'] &&
            !colData['cast'].toLowerCase().includes('timestamp')) ||
          isEditable
        "
        >&nbsp;</el-checkbox
      >
    </li>
    <li class="ascolumn">
      <el-checkbox
        v-model="columnChecked"
        @change="setColumnChecked"
        :disabled="!colData.name || colData.name == currentKey.primary"
      >
        &nbsp;
      </el-checkbox>
    </li>
    <li class="astag">
      <el-checkbox
        v-model="tagChecked"
        @change="setTagChecked"
        :disabled="
          tagDisable || !colData.name || colData.name == currentKey.primary
        "
      >
        &nbsp;
      </el-checkbox>
    </li>
    <li>
      <template v-if="constcols.includes(colData['name'])">
        <span class="forbidden">{{ colData["name"] }}</span>
      </template>
      <el-input
        :value="colData['name']"
        size="mini"
        v-else
        @input="watchFieldVal"
      ></el-input>
    </li>
    <li>
      <template v-if="constcols.includes(colData['name'])">
        <span class="forbidden">
          <i class="el-icon-close"></i>
        </span>
      </template>
      <el-input v-model="colData['alias']" size="mini" v-else></el-input>
    </li>
    <li
      style="
        position: relative;
        display: flex;
        justify-content: center;
        max-width: 135px;
      "
    >
      <template v-if="constcols.includes(colData['name'])">
        <span class="forbidden">
          <i class="el-icon-close"></i>
        </span>
      </template>
      <template v-else>
        <el-select
          :value="colData['cast']"
          size="mini"
          @change="changeType"
          placeholder=""
        >
          <el-option
            v-for="item in mqttTypes"
            :key="item.value"
            :label="item.label"
            :value="item.value"
          ></el-option>
        </el-select>
        <el-input
          type="number"
          :min="1"
          v-if="['NCHAR', 'VARCHAR'].includes(colData['cast'])"
          size="mini"
          @input="handleChange"
          :value="
            /\d/.test(colData['cast'])
              ? Number(colData['cast'].replace(/[^\d]/g, ''))
              : num
          "
        ></el-input>
        <!-- <el-input-number
          :value="
            /\d/.test(colData['cast'])
              ? Number(colData['cast'].replace(/[^\d]/g, ''))
              : num
          "
          controls-position="right"
          @change="handleChange"
          :min="1"
          v-if="['NCHAR', 'VARCHAR'].includes(colData['cast'])"
          size="mini"
        ></el-input-number> -->
      </template>
    </li>
    <li class="icon-col" v-if="!constcols.includes(colData['name'])">
      <span class="icon-container" @click="deleteRow">
        <i class="el-icon-minus"></i>
      </span>
    </li>
  </ul>
</template>
<script>
import { Message } from "element-ui";
import { dataType } from "../../2_explorer/views/components/utils/index";
const timestamps = [
  {
    label: "TIMESTAMP(us)",
    value: "TIMESTAMP(us)",
  },
  {
    label: "TIMESTAMP(ns)",
    value: "TIMESTAMP(ns)",
  },
];
export default {
  name: "NewMqttColumn",
  inject: ["mqttParserObj", "currentKey"],
  props: {
    index: {
      type: Number,
      default: 0,
    },
    colData: {
      type: Object,
      default: () => {
        return null;
      },
    },
    isEditable: {
      type: Boolean,
      default: false,
    },
  },

  data() {
    return {
      columnChecked: false,
      tagChecked: false,
      tagDisable: false,
      nonEditableCols: ["ts", "qos", "topic"],
      num: 1,
      mqttTypes: [...dataType, ...timestamps].filter(
        (item) => item.value !== "NCHAR" && item.value != "VARCHAR"
      ),
      constcols: ["ts", "topic", "qos"],

      params: {
        name: "",
        alias: "",
        cast: "",
      },
    };
  },
  computed: {
    getParserObj() {
      return this.mqttParserObj();
    },
    addStatus() {
      if (!this.colData.name) {
        return true;
      }
      if (!this.colData.alias) {
        return true;
      }
      if (!this.colData.cast) {
        return true;
      }
      return false;
    },
  },
  methods: {
    //获取上次store中parser的值,并重新生成新的parser
    getPreveiousParser(val, type) {
      let oldparser = this.$store.state.app.mqttParser;
      let columns = oldparser.model.columns;
      let tags = oldparser.model.tags;
      if (type == "tag") {
        if (columns.includes(val)) {
          columns.splice(columns.indexOf(val), 1);
        }
        if (this.tagChecked) {
          if (!tags.includes(val)) {
            tags.push(val);
          }
        } else {
          tags.splice(tags.indexOf(val), 1);
        }
      }
      if (type == "column") {
        if (tags.includes(val)) {
          tags.splice(tags.indexOf(val), 1);
        }
        if (this.columnChecked) {
          if (!columns.includes(val)) {
            if (this.colData.name == this.currentKey.primary) {
              columns.unshift(val);
            } else {
              columns.push(val);
            }
          }
        } else {
          columns.splice(columns.indexOf(val), 1);
        }
      }
      this.$store.commit("app/SET_MQTT_PARSER", oldparser);
    },
    setColumnChecked() {
      this.tagChecked = false;
      this.getPreveiousParser(this.colData["name"], "column");
    },
    setTagChecked() {
      this.columnChecked = false;
      this.getPreveiousParser(this.colData["name"], "tag");
    },
    changeType(val) {
      this.colData["cast"] = val;
      if (
        !this.colData["cast"].toLowerCase().includes("timestamp") &&
        this.colData["name"] == this.currentKey.primary
      ) {
        this.changePrimary("ts");
      }
    },
    handleChange(val) {
      this.colData["cast"] = this.colData["cast"] + "(" + val + ")";
    },
    changePrimary(val) {
      this.columnChecked = true;
      this.tagChecked = false;
      this.$emit("changePrimary", val);
      this.getPreveiousParser(val, "column");
    },
    watchFieldVal(val) {
      if (this.constcols.includes(val)) {
        Message.error(this.$t("datasource.repeattip"));
        return;
      } else {
        this.colData["name"] = val;
      }
    },

    deleteRow() {
      this.$emit("deleteRow", this.index, this.colData["name"]);
    },
    addRow() {
      if (this.addStatus) return;
      this.$emit("addRow");
      this.addStatus = true;
    },

    //回显tag或者column选中
    echoColOrTag() {
      let oldparser = this.$store.state.app.mqttParser;
      let columns = oldparser.model.columns;
      let tags = oldparser.model.tags;
      if (columns.includes(this.colData.name)) {
        this.columnChecked = true;
      }
      if (tags.includes(this.colData.name)) {
        this.tagChecked = true;
      }
    },
  },
  mounted() {
    if (
      this.currentKey.primary &&
      this.currentKey.primary == this.colData.name
    ) {
      this.columnChecked = true;
    }
    if (!this.nonEditableCols.includes(this.currentKey.primary)) {
      this.nonEditableCols.concat(this.currentKey.primary);
    }

    this.echoColOrTag();
  },
  watch: {
    addStatus: {
      deep: true,
      handler(val) {
        this.$emit("changeAddStatus");
      },
    },
    "currentKey.primary": {
      immediate: true,
      handler(val, oldVal) {
        let oldparser = this.$store.state.app.mqttParser;
        let columns = oldparser.model.columns;
        if (oldVal) {
          if (this.colData.name == oldVal) {
            this.columnChecked = false;
          }
          if (columns.includes(oldVal)) {
            columns.splice(columns.indexOf(oldVal), 1);
          }
        }
        if (val == this.colData.name) {
          this.columnChecked = true;
        }
        console.log(val, oldVal, "主键---00");
      },
    },
    colData: {
      deep: true,
      handler(val, oldVal) {
        console.log(val, oldVal, "coldata");
      },
    },
    "$store.state.app.mqttParser": {
      deep: true,
      handler(val) {
        if (val.model.columns.includes(this.colData.name)) {
          this.columnChecked = true;
        }else{
          this.columnChecked = false;
        }
        if (val.model.tags.includes(this.colData.name)) {
          this.tagChecked = true;
        }else{
          this.tagChecked = false;
        }
      },
    },
  },
};
</script>
<style lang="scss" scoped>
::v-deep {
  .el-tooltip__popper.is-light {
    border: 1px solid #4259ce;
  }
  .el-input-number__decrease {
    height: 14px !important;
  }
  .el-input-number__increase {
    height: 14px !important;
  }
  .el-checkbox {
    display: flex;
    align-items: center;
    justify-content: center;
  }
}
.mqtt-column {
  display: grid;
  grid-template-columns: 2fr 2fr 2fr 3fr 3fr 3fr 0.5fr;
  column-gap: 10px;
  border-top: 1px solid #ebeef5;
  padding-top: 8px;
  padding-bottom: 8px;
  &.edit {
    position: relative;
    &::before {
      content: "";
      position: absolute;
      left: 0;
      right: 0;
      bottom: 0;
      top: 0;
      background: #f2f6fc66;
      z-index: 99;
    }
  }
  .primary {
    display: flex;
    justify-content: center;
  }
  .forbidden {
    display: flex;
    justify-content: center;
    cursor: initial;
    .el-icon-close {
      cursor: initial;
    }
  }
  .icon-col {
    display: flex;
    align-items: center;
    justify-content: center;
    padding-right: 10px;
    i {
      color: #999;
    }
  }
  .ascolumn,
  .astag {
    display: flex;
    align-items: center;
    justify-content: center;
  }
  .icon-container {
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 3px;
    border: 1px solid #999;
    width: 22px;
    height: 22px;
    border-radius: 50%;
    cursor: pointer;
    &.disabled {
      cursor: not-allowed;
    }
  }
}
</style>
