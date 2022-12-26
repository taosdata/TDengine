<template>
  <div class="page-wrapper">
    <MainContentHeader :title="$t('route.activity')"></MainContentHeader>
    <div class="content">
      <ActivityOp></ActivityOp>
      <ActivityTable></ActivityTable>
    </div>
  </div>
</template>

<script>
import ActivityOp from "./components/ActivityOp";
import ActivityTable from "./components/ActivityTable";

export default {
  components: { ActivityOp, ActivityTable },
  data() {
    return {};
  },
  computed: {},
  mounted() {
    this.$store.dispatch("activity/getActivityList", { current_page: 1 });
  },
  methods: {},
};
</script>

<style lang="scss" scoped></style>
