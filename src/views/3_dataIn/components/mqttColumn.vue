<template>
  <ul class="mqtt-column">
    <li class="primary">
      <el-checkbox
        :value="colData['name'] == currentPrimary"
        @change="changePrimary(colData['name'])"
        :disabled="
          ['topic', 'qos'].includes(colData['name']) ||
          !colData['name'] ||
          (!colData['cast'] && colData['name'] != 'ts') ||
          (colData['cast'] &&
            !colData['cast'].toLowerCase().includes('timestamp'))
        "
        >&nbsp;</el-checkbox
      >
    </li>
    <li class="ascolumn">
      <el-radio
        v-model="radio"
        label="1"
        @input="checkColumn"
        :disabled="colData['name'] == ''"
        >&nbsp;</el-radio
      >
    </li>
    <li class="astag">
      <el-radio
        v-model="radio"
        label="2"
        @input="checkTag"
        :disabled="colData['name'] == ''"
        >&nbsp;</el-radio
      >
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
  name: "MqttColumn",
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
  },
  data() {
    return {
      num: 1,
      mqttTypes: [...dataType, ...timestamps].filter(item=>item.value!=='NCHAR'&&item.value!='VARCHAR'),
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
    changeType() {
      this.num = 1;
    },
    handleChange(val) {
      this.colData["cast"] = this.colData["cast"] + "(" + val + ")";
    },
    changePrimary(val) {
      this.$emit("changePrimary", val);
    },
    watchFieldVal(val) {
      if (this.constcols.includes(val)) {
        Message.error(this.$t('datasource.repeattip'));
        return;
      } else {
        this.colData["name"] = val;
      }
    },
    checkColumn() {
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
    this.echoColOrTag();
  },
  watch: {
    addStatus: {
      deep: true,
      handler(val) {
        this.$emit("changeAddStatus");
      },
    },
    currentPrimary:{
        deep:true,
        handler(val){
            console.log(val,'最新的主键');
        }
    }
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