<template>
  <el-date-picker v-bind="$attrs" v-model="currentValue" value-format="timestamp"> </el-date-picker>
</template>

<script>
  import { OFFSETUTCTIME } from "@/const";
  export default {
    props: {
      valueFormat: {
        type: String,
        default: "timestamp",
      },
      value: {
        type: [String, Number, Date],
        default: "",
      },
    },
    components: {},
    data() {
      return {};
    },
    computed: {
      currentValue: {
        get() {
          if (this.valueFormat === "timestamp") {
            return this.value ? this.value + OFFSETUTCTIME : "";
          }
          return this.value;
        },
        set(val) {
          if (this.valueFormat === "timestamp") {
            val = val ? val - OFFSETUTCTIME : "";
          }
          this.$emit("input", val);
        },
      },
    },
    watch: {},
    created() {},
    mounted() {},
    methods: {},
  };
</script>

<style scoped lang="scss"></style>
