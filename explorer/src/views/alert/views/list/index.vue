<template>
  <div>
    <AlertOp></AlertOp>
    <AlertsTable></AlertsTable>
  </div>
</template>

<script>
import AlertOp from "./components/AlertOp";
import AlertsTable from "./components/AlertsTable";

export default {
  components: { AlertOp, AlertsTable },
  data() {
    return {};
  },
  created() {
    this.$store.dispatch("alert/getAlertList");
  }
};
</script>

<style lang="scss" scoped></style>
