<template>
  <div class="tag-column">
    <span class="label">{{ tagColumnData.field }}</span>
    <el-select
      :value="tagColumnData.condition"
      placeholder=""
      size="small"
      @change="setCondition"
      style="width: 200px"
    >
      <el-option
        v-for="item in tagColumnData.conditionList"
        :key="item"
        :label="item"
        :value="item"
      >
      </el-option>
    </el-select>
    <div class="second-condition">
      <el-input
        :value="tagColumnData.value"
        size="small"
        @input="setTagValue"
        v-if="showInput"
      ></el-input>
      <div v-if="showBetween" class="between">
        <span>AND</span>
        <el-input :value="tagColumnData.betweenVal" size="small" @input="setBetweenVal"></el-input>
      </div>
    </div>
  </div>
</template>
<script>
export default {
  name: "TagColumn",
  props: {
    tagColumnData: {
      type: Object,
      default: () => {
        return null;
      },
    },
  },
  data() {
    return {
      showInput: true,
      showBetween: false,
    };
  },
  methods: {
    setCondition(val) {
      if (["IS NULL", "IS NOT NULL"].includes(val)) {
        this.showInput = false;
      } else {
        this.showInput = true;
      }
      if (val.includes("BETWEEN")) {
        this.showBetween = true;
      } else {
        this.showBetween = false;
      }
      this.tagColumnData.condition = val;
      this.$forceUpdate();
    },
    setTagValue(val) {
      this.tagColumnData.value = val;
      this.$forceUpdate();
    },
    setBetweenVal(val){
      this.tagColumnData.betweenVal=val
      this.$forceUpdate();
    }
  },
};
</script>
<style lang="scss" scoped>
.tag-column {
  margin-bottom: 15px;
  display: grid;
  grid-template-columns: 1fr 2fr 3fr;
  .label {
    text-align: center;
    line-height: 30px;
    margin-right: 0px;
  }
}
</style>
