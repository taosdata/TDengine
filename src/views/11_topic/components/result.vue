<template>
  <el-form style="text-align: left" size="mini" label-width="140px" label-position="left">
    <el-form-item :label="$t('topic.function')">
      <el-select class="w100" v-model="result.fn" clearable placeholder="" size="mini">
        <el-option v-for="item in fnList" :key="item.lable" :value="item.label"></el-option>
      </el-select>
    </el-form-item>
    <template v-if="currentFn && currentFn.filters">
      <el-form-item v-for="item in currentFn.filters" :label="item.label" :key="item.field">
        <el-select class="w100" clearable v-if="item.type == 'select'" v-bind="item" v-model="result.params[item.field]">
          <el-option v-for="ite in getOptions(item)" :key="ite.value" v-bind="ite"></el-option>
        </el-select>
        <el-input v-else-if="item.type == 'input'" clearable v-model="result.params[item.field]" v-bind="item"></el-input>
        <el-input-number v-else-if="item.type == 'number'" clearable v-model="result.params[item.field]" v-bind="item"></el-input-number>
      </el-form-item>
    </template>
  </el-form>
</template>

<script>
  import { isArray } from "@/utils/validate";

  export default {
    props: {
      result: {
        type: Object,
        default: () => ({}),
      },
      fnList: {
        type: Array,
        default: () => [],
      },
      fieldList: {
        type: Array,
        default: () => [],
      },
      field: {
        type: String,
        default: "",
      },
    },
    components: {},
    data() {
      return {};
    },
    computed: {
      currentFn() {
        if (this.fnList[0]?.label) {
          return this.fnList.find(item => item.label == this.result.fn);
        }
        return null;
      },
      options() {
        return this.fieldList.filter(item => item.filed != this.field);
      },
    },
    watch: {},
    created() {},
    mounted() {
      console.log(this.fnList,'函数列表',this.filed);
    },
    methods: {
      getOptions(item) {
        let options = item.options;
        if (!options) return [];
        if (isArray(item.options)) return item.options;
        if (typeof item.options == "function") return item.options.call(this) || [];
      },
    },
  };
</script>

<style scoped lang="scss"></style>
