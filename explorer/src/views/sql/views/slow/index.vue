<template>
  <div class="slow">
    <div class="slow_content">
      <SlowOp></SlowOp>
      <SlowTable></SlowTable>
    </div>
  </div>
</template>

<script>
import { SlowOp, SlowTable } from "./components";

export default {
  components: { SlowOp, SlowTable },
  data() {
    return {};
  },
  created() {
    this.$store.dispatch("slow/getSlowSqlList");
  }
};
</script>

<style scoped>
.slow_content {
  display: flex;
  flex-direction: column;
  overflow-x: auto;
}
</style>
