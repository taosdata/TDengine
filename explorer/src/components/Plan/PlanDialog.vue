<template>
  <el-dialog :visible.sync="visible" center :title="title" :width="width + 'px'" :close-on-click-modal="false">
    <Plan ref="plan" :step.sync="step" v-bind="$attrs" v-on="$listeners" />
  </el-dialog>
</template>

<script>
  import Plan from "./index.vue";
  export default {
    props: {
      onConfirm: {
        type: Function,
        default: () => {},
      },
      onCancel: {
        type: Function,
        default: () => {},
      },
      value: {
        type: Boolean,
        default: false,
      },
    },
    components: {
      Plan,
    },
    data() {
      return {
        step: 1,
        visible: false,
      };
    },
    watch: {
      value: {
        handler(val) {
          this.visible = val;
        },
        immediate: true,
      },
      visible() {
        this.$emit("input", this.visible);
      },
    },
    computed: {
      width() {
        return this.step == 1 ? 1200 : 600;
      },
      title() {
        return this.step == 1 ? this.$t("plan.planTitle") : this.$t("billing.creditCardInfo");
      },
    },
    methods: {},
  };
</script>

<style></style>
