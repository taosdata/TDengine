<template>
  <ul class="mqtt-column">
    <li>
      <el-input v-model="params.name" size="mini" @change="sendLatestCont"></el-input>
    </li>
    <li>
      <el-input v-model="params.alias" size="mini" @change="sendLatestCont"></el-input>
    </li>
    <li>
      <el-input v-model="params.cast" size="mini" @change="sendLatestCont"></el-input>
    </li>
    <li>
      <span class="icon-container" @click="deleteRow" v-if="index!=0">
        <i class="el-icon-minus"></i>
      </span>
      <!-- <span
        :class="['icon-container', addStatus ? 'disabled' : 'able']"
        @click="addRow"
      >
        <i :class="['el-icon-plus', addStatus ? 'disabled' : 'able']"></i
      ></span> -->
      <!-- <span class="icon-container"> <i class="el-icon-check"></i></span> -->
    </li>
  </ul>
</template>
<script>
export default {
  name: "MqttColumn",
  props: {
    index: {
      type: Number,
      default: 0,
    },
  },
  data() {
    return {
      addDisable: true,
      params: {
        name: "",
        alias: "",
        cast: "",
      },
    };
  },
  computed: {
    addStatus() {
      if (!this.params.name) {
        return true;
      }
      if (!this.params.alias) {
        return true;
      }
      if (!this.params.cast) {
        return true;
      }
      return false;
    },
  },
  methods: {
    deleteRow() {
      this.$emit("deleteRow", this.index);
    },
    addRow() {
      if (this.addStatus) return
      this.$emit("addRow");
      this.addStatus=true
    },
    sendLatestCont(){
        this.$emit('sendLatestCont',this.params,this.index)
    }
  },
  watch:{
    addStatus:{
        deep:true,
        handler(val){
            this.$emit('changeAddStatus')
        }
    }
  }
};
</script>
<style lang="scss" scoped>
.mqtt-column {
  display: grid;
  grid-template-columns: 2fr 2fr 2fr 0.5fr;
  grid-gap: 10px;
  margin-bottom: 10px;
  .icon-container {
    display: inline-block;
    padding: 3px;
    border: 1px solid #DCDFE6;
    width: 28px;
    border-radius: 50%;
    cursor: pointer;
    &.disabled {
      cursor: not-allowed;
    }
  }
}
</style>