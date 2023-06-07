<template>
  <ul class="mqtt-column">
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
      <el-input v-model="colData['name']" size="mini" v-else ></el-input>
    </li>
    <li>
      <template v-if="constcols.includes(colData['name'])">
        <span class="forbidden">
          <i class="el-icon-close"></i>
        </span>
      </template>
      <el-input v-model="colData['alias']" size="mini" v-else></el-input>
    </li>
    <li style="position: relative">
      <template v-if="constcols.includes(colData['name'])">
        <span class="forbidden">
          <i class="el-icon-close"></i>
        </span>
      </template>
      <template v-else>
        <el-input v-model="colData['cast']" size="mini"></el-input>
        <el-tooltip
          effect="light"
          :content="$t('datasource.addmqtttip')"
          placement="right-start"
          style="position: absolute"
        >
          <i
            class="el-icon-info"
            style="color: #4259ce; margin-top: 5px; margin-left: 4px"
          ></i>
        </el-tooltip>
      </template>
    </li>
    <li class="icon-col" v-if="!constcols.includes(colData['name'])">
      <span class="icon-container" @click="deleteRow" v-if="index != 0">
        <i class="el-icon-minus"></i>
      </span>
    </li>
  </ul>
</template>
<script>
import { Message } from 'element-ui';
export default {
  name: "MqttColumn",
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
  },
  data() {
    return {
      constcols: ["ts", "topic", "qos"],
      radio: "",
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
    watchFieldVal(val){
        console.log(val,'输入');
        if(this.constcols.includes(val)){
            Message.error('不能输入ts,topic,qos作为新字段')
            return
        }else{
            console.log('tiaoshi');
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
      this.$emit("deleteRow", this.index);
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
      if(columns.includes(this.colData.name)){
        this.radio='1'
      }
      if(tags.includes(this.colData.name)){
        this.radio='2'
      }
    },
  },
  mounted() {
    this.echoColOrTag()
  },
  watch: {
    addStatus: {
      deep: true,
      handler(val) {
        this.$emit("changeAddStatus");
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
}
.mqtt-column {
  display: grid;
  grid-template-columns: 1fr 1fr 1fr 1fr 1fr 0.5fr;
  column-gap: 10px;
  border-top: 1px solid #ebeef5;
  padding-top: 8px;
  padding-bottom: 8px;
  .forbidden {
    display: flex;
    justify-content: center;
    cursor: initial;
  }
  .icon-col {
    display: flex;
    align-items: center;
    justify-content: center;
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