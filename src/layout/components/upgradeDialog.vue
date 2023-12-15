<template>
  <el-dialog :visible.sync="visible" center :title="title" :width="width + 'px'" :close-on-click-modal="false">
    <!-- <Plan :step.sync="step" :currentPlan="currentPricePlan" :cloudAndRegion="cloudAndRegion" @change="planChange" /> -->
  </el-dialog>
</template>

<script>
  import Plan from "@/components/Plan/index.vue";
  import { createOrder } from "api/gateway/app";
  export default {
    components: {
      Plan,
    },
    data() {
      return {
        step: 1,
        requestIng: false,
      };
    },
    watch: {},
    computed: {
      width() {
        return this.step == 1 ? 1200 : 600;
      },
      title() {
        return this.step == 1 ? this.$t("plan.planTitle") : this.$t("billing.creditCardInfo");
      },
      visible: {
        get() {
          return this.$store.state.upgradeDialogVisible;
        },
        set(val) {
          this.$store.commit("SET_UPGRADE_DIALOG_VISIBLE", val);
        },
      },

      currentPricePlan() {
        return this.$store.getters.currentPricePlan?.priceLevel || "FREE";
      },
      clusterInfo() {
        return this.$store.state.app.current_cluster.id ? this.$store.state.app.current_cluster : this.$store.state.app.clusters[0];
      },
      cloudAndRegion() {
        return {
          cloudId: this.clusterInfo.cloud_id,
          regionId: this.clusterInfo.region_id,
        };
      },
    },
    methods: {
      planChange(plan) {
        if (this.requestIng) return;
        this.$confirm(this.$t("cluster.upgradeTip") + plan.planName, this.$t("upgradeTip"), {
          confirmButtonText: this.$t("confirm"),
          cancelButtonText: this.$t("cancel"),
          type: "success",
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
                this.visible = false;
                this.$message.success(this.$t("cluster.upgradeSucc"));
                this.$store.dispatch("app/getClusterList");
              })
              .finally(() => {
                this.requestIng = false;
              });
          })
          .catch(() => {
            this.requestIng = false;
          });
      },
    },
  };
</script>

<style></style>
