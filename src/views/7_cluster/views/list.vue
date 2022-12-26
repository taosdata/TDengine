<template>
  <div class="cluster-list">
    <section class="header-option">
      <el-button plain size="mini" class="big-button" icon="el-icon-refresh" :disabled="requestIng" @click="refresh">{{ $t("refresh") }}</el-button>
      <el-button class="big-button" v-permission plain @click="addCluster" size="small" icon="el-icon-plus">{{
        $t("cluster.createCluster")
      }}</el-button>
    </section>
    <section class="cluster-content">
      <el-empty v-if="!clusterList.length" :image-size="200"></el-empty>
      <cluster-card
        v-for="item in clusterList"
        @upgrade="upgrade"
        @edit="edit"
        :key="item.id"
        :cluster="item"
        :tokenShow="item.id == tokenId"
      ></cluster-card>
    </section>
    <Plan
      v-model="dialog"
      @close="dialog = false"
      :cloudAndRegion="cloudAndRegion"
      :currentPlan="currentPlan"
      @stepChange="stepChange"
      @change="planChange"
    />
    <el-dialog align="center" :title="title" :visible.sync="editDialog" width="500px">
      <EditCluster @close="editDialog = false" :info="clusterInfo" :editType="editType" />
    </el-dialog>
  </div>
</template>

<script>
  import ClusterCard from "../components/ClusterCard";
  import { createOrder } from "api/gateway/app";
  import EditCluster from "../components/EditCluster";
  export default {
    components: { ClusterCard, EditCluster, Plan: () => import("components/Plan/PlanDialog") },
    data() {
      return {
        dialog: false,
        tokenId: "",
        requestIng: false,
        editType: 0,
        editDialog: false,
        dialogWidth: 1200,
        clusterInfo: {},
        currentPlan: null,
        cloudAndRegion: {
          cloudId: "",
          regionId: "",
        },
      };
    },
    computed: {
      clusterList() {
        return this.$store.state.app.clusters;
      },
      btnDisabled() {
        return this.requestIng || (this.editType == 0 && !this.clusterInfo.alias);
      },
      title() {
        return {
          0: this.$t("cluster.changeCluster"),
          1: this.$t("register.CR"),
        }[this.editType];
      },
      canCreate() {
        return this.$store.getters.currentPricePlan?.clusterNum > this.$store.state.app.clusters.length;
      },
    },
    created() {},
    methods: {
      addCluster() {
        this.$router.push("instances/create");
      },
      edit(cluster) {
        this.editType = 0;
        cluster.alias = cluster.alias || cluster.name;
        this.clusterInfo = cluster;
        this.editDialog = true;
      },
      async close(update) {
        if (update == 1) {
          await this.$store.dispatch("app/getClusterList");
          this.$router.push("/instanceStatus");
        }
        this.dialog = false;
      },
      async refresh() {
        if (this.requestIng) return;
        this.requestIng = true;
        await this.$store.dispatch("app/getClusterList").catch(() => false);
        this.requestIng = false;
      },
      upgrade(cluster) {
        this.editDialog = false;
        this.clusterInfo = cluster;
        this.cloudAndRegion = {
          cloudId: cluster.cloud_id,
          regionId: cluster.region_id,
        };
        this.currentPlan = cluster.service_level;
        this.dialog = true;
      },
      planChange(plan) {
        if (this.requestIng) return;
        this.$confirm(this.$t("cluster.upgradeTip") + plan.planName, this.$t("upgradeTip"), {
          confirmButtonText: this.$t("confirm"),
          cancelButtonText: this.$t("cancel"),
          type: "warning",
        })
          .then(async () => {
            this.requestIng = true;
            createOrder({
              app_id: this.clusterInfo.id,
              cluster_id: this.clusterInfo.name,
              price_plan_id: plan.id,
              cloud_id: plan.cloudId,
              region_id: plan.regionId,
              account_id: this.clusterInfo.account_id,
            })
              .then(() => {
                this.$message.success(this.$t("cluster.upgradeSucc"));
                this.dialog = false;
              })
              .finally(() => {
                this.$store.dispatch("app/getClusterList");
                this.requestIng = false;
              });
          })
          .catch(() => {
            this.requestIng = false;
          });
      },
      stepChange(step) {
        this.dialogWidth = step == 1 ? 1200 : 500;
      },
    },
  };
</script>

<style lang="scss" scoped>
  .cluster-list {
    width: 100%;
    position: relative;
  }
  .header-option {
    display: flex;
    justify-content: flex-end;
    margin-bottom: 10px;
    position: sticky;
    top: 0;
    z-index: 5;
  }
  .cluster-content {
    position: relative;
    // padding-bottom: 50px;
  }
</style>
