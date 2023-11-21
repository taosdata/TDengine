<template>
  <section
    class="flexCenter"
    :class="{ len: VariableTableColumnType.includes(currentValue) }"
  >
    <el-select
      v-model="currentValue"
      size="mini"
      :placeholder="$t('dataIn.dataType')"
      @change="handleType"
    >
      <el-option
        v-for="item in TDengineDataType"
        :key="item"
        :value="item"
      ></el-option>
    </el-select>
    <el-input
      class="input-len"
      v-if="VariableTableColumnType.includes(currentValue)"
      size="mini"
      type="number"
      :placeholder="$t('length')"
      :min="8"
      @change="handleTypeLength"
      v-model="typeLength"
    ></el-input>
  </section>
</template>

<script>
import { VariableTableColumnType, TDengineDataType, VariableTableColumnTypeMaxLenthMap } from '@/const';
export default {
  props: {
    value: {
      type: String,
      default: ''
    }
  },
  components: {},
  data() {
    this.maxLengthMap = VariableTableColumnTypeMaxLenthMap;
    return {
      VariableTableColumnType,
      TDengineDataType,
      minTypeLength: 8
    };
  },
  computed: {
    typeLength: {
      get() {
        return this.value.match(/\((\d+)\)/)?.[1] ?? this.minTypeLength;
      },
      set(val) {
        this.changeValue(this.joinValue(val));
      }
    },
    currentValue: {
      get() {
        return this.value.replace(/\(\d+\)/, '');
      },
      set(val) {
        this.$emit('input', val);
      }
    }
  },
  watch: {},
  created() {},
  mounted() {},
  methods: {
    handleType(type) {
      if (VariableTableColumnType.includes(type)) {
        this.changeValue(type + '(' + this.typeLength + ')');
      }
    },
    handleTypeLength(val) {
      if (!val) return this.changeValue(this.joinValue(this.minTypeLength));
      let result = val.match(/\d+/g)?.[0] || this.minTypeLength;
      if (result < this.minTypeLength) {
        result = this.minTypeLength;
      } else if (result > this.maxLengthMap[this.currentValue]) {
        result = this.maxLengthMap[this.currentValue];
      }
      this.changeValue(this.joinValue(result));
    },
    changeValue(val) {
      this.$emit('input', val);
      this.$emit('change', val);
    },
    joinValue(len) {
      if (!len) return this.currentValue;
      return this.currentValue + '(' + len + ')';
    }
  }
};
</script>

<style scoped lang="scss">
.input-len :deep(.el-input__inner) {
  border-left: none;
  border-top-left-radius: 0;
  border-bottom-left-radius: 0;
}
:deep(.el-select .el-input .el-input__inner) {
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
}
</style>
