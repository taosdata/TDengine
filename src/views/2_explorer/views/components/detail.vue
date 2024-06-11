<template>
  <div class="detail">
    <component :is="currentComponent"></component>
  </div>
</template>

<script>
  import DatabaseCreate from "./databases/db_create";
  // import CreateStableOrTable from "./createStableOrtable.vue";
  import StableCreate from "./stables/stb_create";
  import Info from "./info.vue";
  import TableCreate from "./tables/tb_create";
   import DatabasePrivileges from "./privilege";
  // import Matrixs from "./matrixs";
  Array.prototype.insert = function (index, item) {
    this.splice(index, 0, item);
  };
  Array.prototype.remove = function (index) {
    if (index > -1 && this.length > 1) {
      this.splice(index, 1);
    }
  };
  export default {
    components: {
      DatabaseCreate,
      // CreateStableOrTable,
      DatabasePrivileges,
      Info,
      TableCreate,
      StableCreate,
      // Matrixs
    },
    computed: {
      currentComponent() {
        return this.$store.state.console.currentComponent;
      },
    },
  };
</script>

<style lang="scss" scoped>
  .detail {
    height:100%;
    // max-height: 100vh;
    overflow: auto;
    position: relative;
    padding: 0 15px;
  }
  //去掉输入框禁止使用和前置的背景色
  .detail ::v-deep .el-input.is-disabled .el-input__inner,
  .detail ::v-deep .el-input-group__append,
  .detail ::v-deep .el-input-group__prepend {
    background-color: unset;
    color: #606266;
    .el-button.is-disabled,
    .el-button.is-disabled:hover,
    .el-button.is-disabled:focus {
      background-color: transparent;
      border-color: transparent;
    }
  }
  .detail ::v-deep .el-input-group__prepend {
    width: 150px;
    padding-left: 15px;
  }
  .detail ::v-deep .el-input.is-disabled .el-input__inner {
    color: #606266;
  }
  .detail ::v-deep .el-form-item {
    margin-bottom: 25px;
  }
  .detail ::v-deep .el-form-item__label {
    font-weight: 500;
  }
  .detail ::v-deep .flexCenter .el-select .el-input__inner {
    border-color: #dcdfe6;
    border-left: none;
    border-top-right-radius: 0;
    border-bottom-right-radius: 0;
  }
  .detail ::v-deep .flexCenter .el-select:first-of-type .el-input__inner  {
    border-left: 1px solid #dcdfe6;
    border-right: none;
  }
  .detail ::v-deep .flexCenter .el-input .el-input__inner {
    border-color: #dcdfe6;
    border-top-left-radius: 0;
    border-bottom-left-radius: 0;
  }
  .detail ::v-deep .flexCenter .action-btn {
    display: flex;
    margin-left: 10px;
    .el-button + .el-button {
      margin-left: 0px;
      border-left-style: none;
    }
  }

  .detail .action-btn ::v-deep .el-button--small {
    padding: 9px 10px;
  }
  .detail ::v-deep .el-tag {
    border-left: none;
  }
</style>
