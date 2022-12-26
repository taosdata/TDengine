<template>
  <div class="page-wrapper">
    <MainContentHeader :title="$t('replication.title')"></MainContentHeader>
    <div class="content">
      <section class="flexEnd">
        <el-button plain class="big-button" icon="el-icon-refresh" :disabled="requestIng" @click="refresh">{{ $t("refresh") }}</el-button>
        <el-button :disabled="disabled" class="big-button" plain @click="dialog = true" icon="el-icon-plus">{{
          $t("replication.addNewReplication")
        }}</el-button>
      </section>
      <ReplicationList />
    </div>
    <el-dialog :title="$t('replication.addNewReplication')" align="center" :visible.sync="dialog" width="500px">
      <CreateReplication @close="dialog = false" />
    </el-dialog>
  </div>
</template>

<script>
  import ReplicationList from "./components/replication.vue";
  import CreateReplication from "./components/createReplication.vue";
  export default {
    components: { ReplicationList, CreateReplication },
    data() {
      return {
        dialog: false,
        requestIng: false,
      };
    },
    computed: {
      dbList() {
        return this.$store.state.replication.dbList;
      },
      disabled() {
        return false;
      },
    },
    created() {
      this.$store.dispatch("replication/getTaskList");
    },
    watch: {},
    methods: {
      async refresh() {
        if (this.requestIng) return;
        this.requestIng = true;
        await this.$store.dispatch("replication/getTaskList");
        this.requestIng = false;
      },
    },
  };
</script>

<style lang="scss" scoped></style>
