<template>
  <ul
    :class="[
      'csv-column',
      isEditable &&
      (this.nonEditableCols.includes(colData['name']) ||
        colData['name'] == colData.parser.model.columns[0])
        ? 'edit'
        : '',
    ]"
  >
    <li style="max-width: 150px; margin-right: 10px">
      <el-select
        :value="colData.parser.parse[csvColName].alias"
        filterable
        size="mini"
        :filter-method="handleFilter"
        placeholder=""
        clearable
        @clear='handleClearItem'
        @visible-change="
          (visible) =>
            handleVisble(visible, colData.parser.parse[csvColName].alias)
        "
        @change="handledbChange($event, 0)"
      >
        <el-option
          v-for="(item, index) in dbOptions"
          :key="item.field"
          :label="item.field"
          :value="item.field"
          :disabled="item.disabled"
        >
          <span style="float: left">{{ item.field }}</span>
          <span
            v-if="item.newByInpt"
            class="el-icon-close"
            style="
              float: right;
              color: #8492a6;
              font-size: 13px;
              line-height: 36px;
              cursor: pointer;
            "
            @click.stop="handleClear(index)"
          ></span>
        </el-option>
      </el-select>
    </li>
    <li
      style="
        position: relative;
        display: flex;
        justify-content: center;
        max-width: 150px;
      "
    >
      <template>
        <el-select
          :value="colData.parser.parse[csvColName].as.toUpperCase()"
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
    <li class="primary">
      <el-checkbox
        :disabled="
          !colData.parser.parse[csvColName].as
            .toUpperCase()
            .includes('TIMESTAMP')
        "
        :value="
          colData.parser.parse[csvColName].alias == colData.parser.model.columns[0]
        "
        @change="changePrimary(colData.parser.parse[csvColName].alias)"
        >&nbsp;</el-checkbox
      >
    </li>
    <li class="ascolumn">
      <el-checkbox
        v-model="columnChecked"
        @change="setColumnChecked"
        :disabled="
          colData.parser.parse[csvColName].alias == colData.parser.model.columns[0]
        "
      >
        &nbsp;
      </el-checkbox>
    </li>
    <li class="astag">
      <el-checkbox
        v-model="tagChecked"
        @change="setTagChecked"
        :disabled="
          colData.parser.parse[csvColName].alias == colData.parser.model.columns[0]
        "
      >
        &nbsp;
      </el-checkbox>
    </li>
  </ul>
</template>
<script>
import { Message } from "element-ui";
import { dataType } from "../../../2_explorer/views/components/utils/index";
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
  inject: ["currentKey"],
  props: {
    csvColName: {
      type: String,
      default: "",
    },
    dbOptions: {
      type: Array,
      default: () => {
        return [];
      },
    },
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
      value: ["", "", ""],
      oldValue: ["", "", ""],
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
    handleClearItem(val){
      console.log(val,'清楚初始化');
    },
    handleVisble(visible, value) {
      console.log(visible, value, "visible");
      this.$emit("handleVisble", visible, value);
    },
    handleFilter(value) {
      console.log(
        value,
        "filter",
        this.colData.parser.parse[this.csvColName],
        this.csvColName
      );
      this.$emit("handleFilter", value);
    },
    handleClear(index) {
      console.log(index, "调用父组件clear方法");
    },
    //获取上次store中parser的值,并重新生成新的parser
    getPreveiousParser(val, type, key) {
      let oldparser = this.$store.state.app.csvParser;
      let columns = oldparser.parser.model.columns;
      let tags = oldparser.parser.model.tags;
      if (key == "primary") {
        if (tags.includes(val)) {
          columns.splice(columns.indexOf(val), 1);
        }
        if (columns.includes(val)) {
          this.columnChecked = false;
          columns.splice(columns.indexOf(val), 1,undefined);
        } else {
          columns.push(val);
        }

        console.log(tags, type,key,"主键999");
      }
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
      if (type == "column" && key != "primary") {
        if (tags.includes(val)) {
          tags.splice(tags.indexOf(val), 1);
        }
        if (this.columnChecked) {
          if (!columns.includes(val)) {
            if (
              this.colData.parser.parse[this.csvColName].alias ==
              this.colData.parser.model.columns[0]
            ) {
              columns.unshift(val);
            } else {
              columns.push(val);
            }
          }
        } else {
          columns.splice(columns.indexOf(val), 1);
        }
      }
      this.$store.commit("app/SET_CSV_PARSER", oldparser);
      console.log(
        this.$store.state.app.csvParser,
        " this.$store.state.app.csvParser"
      );
    },
    setColumnChecked() {
      this.tagChecked = false;
      this.getPreveiousParser(this.colData.parser.parse[this.csvColName].alias, "column");
    },
    setTagChecked() {
      this.columnChecked = false;
      this.getPreveiousParser(this.colData.parser.parse[this.csvColName].alias, "tag");
    },
    changeType(val) {
      this.colData.parser.parse[this.csvColName].as = val;
    },
    handledbChange(val, index) {
      let oldItem=this.colData.parser.parse[this.csvColName].alias
      this.colData.parser.parse[this.csvColName].alias = val;
      let result = this.dbOptions.find((item) => item.field == val);
      
      console.log(result, val,index, "======");
      if (result) {
        if (Object.hasOwnProperty.call(result, "newByInpt")) {
          this.colData.parser.parse[this.csvColName].as = "";
        } else {
          this.colData.parser.parse[this.csvColName].as = result.type;
        }
      }

      this.$emit("handledbChange", oldItem, index);
    },
    handleChange(val) {
      this.colData.parser.parse[this.csvColName].as = val;
    },
    //选择主键
    changePrimary(val) {
      console.log(val, "主键");
      this.columnChecked = true;
      this.tagChecked = false;
      this.getPreveiousParser(val, "column", "primary");
    },
    watchFieldVal(val) {
      if (this.constcols.includes(val)) {
        Message.error(this.$t("datasource.repeattip"));
        return;
      } else {
        this.colData["name"] = val;
      }
    },

    //回显tag或者column选中
    echoColOrTag() {
      // let oldparser = this.$store.state.app.mqttParser;
      // let columns = oldparser.model.columns;
      // let tags = oldparser.model.tags;
      // if (columns.includes(this.colData.name)) {
      //   this.columnChecked = true;
      // }
      // if (tags.includes(this.colData.name)) {
      //   this.tagChecked = true;
      // }
    },
  },
  mounted() {
    console.log(this.colData, "csv----column");
  },
  watch: {
    addStatus: {
      deep: true,
      handler(val) {
        this.$emit("changeAddStatus");
      },
    },
    "$store.state.app.mqttParser": {
      deep: true,
      handler(val) {
        // if (val.model.columns.includes(this.colData.name)) {
        //   this.columnChecked = true;
        // } else {
        //   this.columnChecked = false;
        // }
        // if (val.model.tags.includes(this.colData.name)) {
        //   this.tagChecked = true;
        // } else {
        //   this.tagChecked = false;
        // }
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
.csv-column {
  display: grid;
  grid-template-columns: 1.5fr 1.5fr 1fr 1fr 1fr;
  column-gap: 10px;
  border-top: 1px solid #ebeef5;
  padding-top: 8px;
  padding-bottom: 8px;
  position: relative;
  li {
    display: flex;
    align-items: center;
    justify-content: center;
  }
  &:not(:last-child) {
    &::before {
      content: "";
      height: 1px;
      bottom: -1px;
      right: 815px;
      left: -130px;
      background: #ebeef5;
      position: absolute;
    }
  }

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
