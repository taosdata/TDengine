
<template>
  <span class="stateCheckBox">
    <el-checkbox
      v-model="check"
      :indeterminate="mid"
      :disabled="data.disabled"
      @change="change"
    >
      <span
        class="label"
        :class="[check ? 'active' : '', data.disabled ? 'disabled' : '']"
        >{{ data.label }}</span
      >
    </el-checkbox>
  </span>
</template>

<script>
/**
 * @description: props 说明
 * data {
 *  label: 显示的值，
 *  disabled：是否禁用
 * }
 */
export default {
  components: {},
  model: {
    prop: 'value',
    event: 'input',
  },
  props: {
    value: {
      type: [Number, String],
      default: 0,
    },
    data: {
      type: Object,
      default: () => {
        return {};
      },
    },
  },
  data() {
    return {
      check: false,
      mid: false,
      num: 0,
    };
  },
  mounted() {
    this.loadState(this.value);
  },
  watch: {
    num() {
      this.$emit('input', this.num);
    },
  },
  methods: {
    // 初始化状态
    loadState(num) {    
      this.num = (num == null || num == 0) ? 0 : (num == 'false' ? 2 : 1);
      this.chooseState(this.num);
    },
    // 改变状态
    chooseState(num) {
      this.check = num == 1 ? true : false;
      this.mid = num == 2 ? true : false;
    },
    // 点击多选框
    change() {
      this.num++;
      this.num > 2 ? (this.num = 0) : '';
      this.chooseState(this.num);
    },
  },
};
</script>

<style lang="scss" scoped>
.stateCheckBox {
  margin-right: 30px;
  .label {
    color: #606266;
  }
  .active {
    color: #409eff;
  }
  .disabled {
    color: #c0c4cc;
  }
  ::v-deep .el-checkbox__input.is-indeterminate .el-checkbox__inner::before {
    content: "X" !important;
    font-size: 24px;
    z-index: 100;
    color: #fff;
    position: absolute;
    top: -3px;
  }
}
</style>
