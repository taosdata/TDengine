<template>
  <div>
    <el-row class="row-style">
      <el-col :span="12" class="col-style">
        <span class="label">LIMIT</span>
        <el-input-number 
          v-model="otherRule.limit" 
          placeholder="" 
          style="margin-right: 8px;" 
          :size="size"
          controls-position="right"
          >
        </el-input-number>
      </el-col>
      <el-col :span="12" class="col-style">
        <span class="label">OFFSET</span>
        <el-input-number 
          v-model="otherRule.offset" 
          placeholder="" 
          style="margin-right: 8px;" 
          :size="size"
          controls-position="right"
          >
        </el-input-number>
      </el-col>
    </el-row>
    <!-- <el-row class="row-style">
      <el-col :span="12" class="col-style">
        <span class="label">ORDER BY</span>
        <el-input 
          v-model="otherRule.orderby" 
          placeholder="" 
          style="margin-right: 8px;" 
          :size="size"
          controls-position="right"
          >
        </el-input>
      </el-col>
    </el-row> -->
  </div>
</template>

<script>
export default {
  name: 'OtherRule',
  props: {
    otherRule: {
      type: Object,
      default: () => {}
    },
    size: {
      type: String,
      default: 'mini'
    }
  },
}
</script>

<style lang="scss" scoped>
.col-style {
  display: flex;
  align-items: center;
}
.row-style {
  margin-top: 10px;
}
</style>