<template>
  <el-dialog 
     :visible.sync="resultVisible" 
     width="50%"
     :show-close="false"
     title="数据源连通性及版本检测"
     >
     <!-- <el-row :class="['flexStart','flexColumn']">
      <div :class="['text','text-awating',result.available ? 'text-success': 'text-error']">数据源连通性及版本检测</div>
     </el-row> -->
     <!-- <el-row v-if="loading" :class="['flexCenter','flexColumn']">
      <el-progress type="circle" :percentage="percentage" :color="colors"></el-progress>
      <div :class="['text','text-awating']">正在检测，请稍等。。。</div>
     </el-row> -->
     <el-row :class="['flexCenter','flexColumn']">
        <!-- <el-progress v-if="result.available" type="circle" :percentage="100" status="success"></el-progress> -->
        <!-- <el-progress v-else type="circle" :percentage="100" status="exception"></el-progress> -->
        <div class="text">
          <div :class="[result.available ? 'text-success': 'text-error']"> <i class="el-icon-circle-check"></i> {{ result.available ? '数据源可用' : '数据源不可用'}}</div>
          <div class="text-content">版本号：{{ result.version }}</div>
          <div class="text-content">{{ result.since }}</div>
        </div>
     </el-row>
     <span slot="footer" class="dialog-footer" v-if="JSON.stringify(result) !== '{}'">
        <el-button type="primary" size="small" plain @click="handleClose">确 定</el-button>
      </span>
   </el-dialog>
</template>

<script>
export default {
 name: 'ResultDialog',
 props: {
  result: {
    type: Object,
  },
  resultVisible: {
    type: Boolean,
    default: () => false
  },
  loading: {
    type: Boolean,
    default: () => true
  },
  percentage: {
    type: Number,
    default: () => 10,
  },
 },
 data() {
  return {
    colors: [
      {color: '#f56c6c', percentage: 20},
      {color: '#e6a23c', percentage: 40},
      {color: '#5cb87a', percentage: 60},
      {color: '#1989fa', percentage: 80},
      {color: '#6f7ad3', percentage: 100}
    ],
  }
 },
 computed: {
  visible: {
    get() {
      return this.resultVisible
    },
    set(val) {
      // this.resultVisible = val
    }
  }
 },
 methods: {
  handleClose(){
    this.$emit('cancelModal')
  }
 },
 components: {
   
 },
}
</script>

<style scoped>
  ::v-deep .el-progress.is-success .el-progress__text {
    color: #33b169 !important;
    font-size: 28px !important;
  }
  ::v-deep .el-progress.is-exception .el-progress__text {
    color: #ff2e4d !important;
    font-size: 28px !important;
  }
  ::v-deep .el-icon-check {
    font-weight: 600;
  }

  ::v-deep .el-icon-close {
    font-weight: 600;
  }

  ::v-deep .el-progress-circle {
    width: 80px !important;
    height: 80px !important;
  }
  ::v-deep .el-dialog__title {
    /* color: #33b169; */
  }
  .flexColumn {
    /* flex-direction: column; */
  }
  .text {
    text-align: left;
    font-size: 16px;
    font-weight: 500;
    padding: 10px;
  }

  .text-success {
    color: #33b169;
    padding-bottom: 10px;
  }
  .text-error {
    color: #ff2e4d;
    padding-bottom: 10px;
  }
  .text-content {
    padding-left: 20px;
  }
</style>
