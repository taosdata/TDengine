<template>
  <ul class="mqtt-column">
    <li>
      <el-input v-model="colData['name']"></el-input>
    </li>
    <li>
      <el-input v-model="colData['alias']"></el-input>
    </li>
    <li>
      <el-input v-model="colData['cast']"></el-input>
    </li>
    <li class="icon-col">
      <span class="icon-container" @click="deleteRow" v-if="index != 0">
        <i class="el-icon-minus"></i>
      </span>
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
    colData: {
      type: Object,
      default: () => {
        return null;
      },
    },
  },
  data() {
    return {
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
      if (!this.colData.cast) {
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
      if (this.addStatus) return;
      this.$emit("addRow");
      this.addStatus = true;
    },
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
.mqtt-column {
  display: grid;
  grid-template-columns: 2fr 2fr 2fr 0.5fr;
  column-gap: 10px;
  margin-bottom: 10px;
  .icon-col {
    display: flex;
    align-items: center;
    justify-content: center;
  }
  .icon-container {
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 3px;
    border: 1px solid #dcdfe6;
    width: 28px;
    height: 28px;
    border-radius: 50%;
    cursor: pointer;
    &.disabled {
      cursor: not-allowed;
    }
  }
}
</style>