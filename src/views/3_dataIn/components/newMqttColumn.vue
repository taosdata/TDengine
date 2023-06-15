<template>
  <ul
    :class="[
      'mqtt-column',
      isEditable &&
      (this.nonEditableCols.includes(colData['name']) ||
        colData['name'] == currentPrimary)
        ? 'edit'
        : '',
    ]"
  >
    <li class="primary">
      <el-checkbox
        :value="colData['name'] == currentPrimary"
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
      <span style="color: red; font-size: 24px">{{ columnChecked }}</span>
      <el-checkbox v-model="columnChecked" @change="setColumnChecked">
        &nbsp;
      </el-checkbox>
      <!-- <el-radio
        :value="radio === '1' || colData.name == currentPrimary ? '1' : '2'"
        label="1"
        @click.native="checkColumn"
        :disabled="colData['name'] == ''"
        :style="
          !constcols.includes(colData['name'])
            ? { display: 'flex', paddingLeft: '6px' }
            : ''
        "
        >&nbsp;</el-radio -->
      <!-- > -->
    </li>
    <li class="astag">
      <el-checkbox
        v-model="tagChecked"
        @change="setTagChecked"
        :disabled="tagDisable"
      >
        &nbsp;
      </el-checkbox>
      <!-- <el-radio
        :value="radio"
        label="2"
        :style="
          !constcols.includes(colData['name'])
            ? { display: 'flex', paddingLeft: '6px' }
            : ''
        "
        @click.native="
          () =>
            !(colData['name'] == '' || colData['name'] == currentPrimary) &&
            checkTag()
        "
        :disabled="colData['name'] == '' || colData['name'] == currentPrimary"
        >&nbsp;</el-radio
      > -->
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
          v-model="colData['cast']"
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
    currentPrimary: {
      type: String,
      default: "",
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
      primaryradio: "ts",
      radio: "",
      primaryval: false,
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
    primary() {
      return this.currentPrimary.primary;
    },
    // getCurrentKey(){
    //   return this.currentKey
    // },
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
    //获取上次store中parser的值
    getPreveiousParser(val, type) {
      let oldparser = this.$store.state.app.mqttParser;
      let columns = oldparser.model.columns;
      let tags = oldparser.model.tags;
      if (type == "tag") {
        if (this.tagChecked) {
          tags.push(val);
        } else {
          tags.splice(tags.indexOf(val), 1);
        }
      }
      if (type == "column") {
        if (this.columnChecked) {
          if (!columns.includes(val)) {
            if (this.colData.name == this.$parent.defaultSelect) {
              columns.unshift(val);
            } else {
              columns.push(val);
            }
          }
        } else {
          columns.splice(columns.indexOf(val), 1);
        }
      }
      console.log(oldparser, "处理过需要保存到store的parser");
      this.$store.commit("app/SET_MQTT_PARSER", oldparser);
    },
    setColumnChecked() {
      // if(this.colData['name']==this.currentPrimary){//主键状态必然选中
      //   this.columnChecked=true
      // }
      this.getPreveiousParser(this.colData["name"], "column");
      this.tagChecked = false;
    },
    setTagChecked() {
      this.columnChecked = false;
      this.getPreveiousParser(this.colData["name"], "tag");
    },
    changeType() {
      if (
        !this.colData["cast"].toLowerCase().includes("timestamp") &&
        this.colData["name"] == this.currentPrimary
      ) {
        this.changePrimary("ts");
      }
    },
    handleChange(val) {
      this.colData["cast"] = this.colData["cast"] + "(" + val + ")";
    },
    changePrimary(val) {
      this.columnChecked = true;
      this.$emit("changePrimary", val);
      this.getPreveiousParser(val, "column");
      // let oldparser = this.$store.state.app.mqttParser;
      // let columns = oldparser.model.columns;
      // let tags = oldparser.model.tags;
      // console.log(
      //   columns,
      //   tags,
      //   val,
      //   "切换主键",
      //   this.$parent,
      //   this.currentPrimary
      // );
      // if (tags.includes(val)) {
      //   tags.splice(tags.indexOf(val), 1);
      // }
      // if (!columns.includes(val)) {
      //   columns.unshift(val);
      //   this.$store.commit("app/SET_MQTT_PARSER", oldparser);
      // }
    },
    watchFieldVal(val) {
      if (this.constcols.includes(val)) {
        Message.error(this.$t("datasource.repeattip"));
        return;
      } else {
        this.colData["name"] = val;
      }
    },
    checkColumn() {
      this.radio = "1";
      if (this.colData.name) {
        let oldparser = this.$store.state.app.mqttParser;
        let columns = oldparser.model.columns;
        let tags = oldparser.model.tags;
        let index = tags.findIndex((item) => item == this.colData.name);
        if (!columns.includes(this.colData.name)) {
          columns.push(this.colData.name);
        }

        if (index !== -1) {
          tags.splice(index, 1);
        }
      }
    },
    checkTag() {
      if (this.colData.name == this.currentPrimary) {
        Message.warning(this.$t("datasource.primaryColTagtip"));
        return;
      }
      this.radio = "2";
      if (this.colData.name) {
        let oldparser = this.$store.state.app.mqttParser;
        let columns = oldparser.model.columns;
        let tags = oldparser.model.tags;
        let index = columns.findIndex((item) => item == this.colData.name);
        if (!tags.includes(this.colData.name)) {
          tags.push(this.colData.name);
        }

        if (index !== -1) {
          columns.splice(index, 1);
        }
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
        this.radio = "1";
      }
      if (tags.includes(this.colData.name)) {
        this.radio = "2";
      }
    },
  },
  mounted() {
    if(this.$parent.defaultSelect==this.colData.name){
      this.columnChecked=true
    } 
    if (!this.nonEditableCols.includes(this.currentPrimary)) {
      this.nonEditableCols.concat(this.currentPrimary);
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
    currentPrimary: {
      deep: true,
      immediate: true,
      handler(val, oldVal) {
        if (this.colData.name == val) {
          this.columnChecked = true;
          this.tagDisable = true;
        } else {
          this.columnChecked = false;
          this.tagDisable = false;
        }
        let oldparser = this.$store.state.app.mqttParser;
        let columns = oldparser.model.columns;
        columns.map((item, index) => {
          if (item == val) {
            columns.unshift(columns.splice(index, 1)[0]);
          }
        });
        if (columns.includes(oldVal)) {
          columns.splice(columns.indexOf(oldVal), 1);
        }
        this.$store.commit("app/SET_MQTT_PARSER", oldparser);

        console.log(val, oldVal, "新老主键");
        // // this.radio = '1'
        // if (
        //   this.colData.name !== this.currentPrimary &&
        //   this.colData.name === oldVal
        // ) {
        //   this.radio = "";
        // }
        // if (this.colData.name === this.currentPrimary) {
        //   const timer = setTimeout(() => {
        //     if (this.radio !== "1") {
        //       this.radio = "1";
        //     }
        //     clearTimeout(timer);
        //   }, 150);
        // }
      },
    },
    'currentKey.primary': {
      immediate:true,
      handler(val) {
        console.error(this.currentKey, this.currentKey.primary, "监听父组件主键");
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
