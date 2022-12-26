<template>
  <div class="create">
    <el-form
      size="small"
      :disabled="cannotCreate"
      :hide-required-asterisk="true"
      :rules="rules"
      ref="form"
      :model="info"
      label-position="left"
      label-width="160px"
    >
      <el-form-item :label="$t('register.CR')" prop="regionId" required>
        <el-row>
          <el-col :span="11">
            <el-select class="w100" v-model="info.cloudId" @change="cloudChange" :placeholder="$t('dashboard.cloud')">
              <el-option v-for="item in cloudList" :key="item.value" v-bind="item" /> </el-select
          ></el-col>
          <el-col :span="11" :offset="2">
            <el-select class="w100" v-model="info.regionId" @change="regionChange" :placeholder="$t('dashboard.region')">
              <el-option v-for="item in regionList" :key="item.value" v-bind="item" /> </el-select
          ></el-col>
        </el-row>
      </el-form-item>
      <el-form-item :label="$t('clusterName')" prop="alias" required>
        <el-input v-model.trim="info.alias" maxlength="64"></el-input>
      </el-form-item>
      <el-form-item :label="$t('register.demoData')">
        <el-checkbox v-model="info.hasDemoData">{{ $t("register.demoDataText") }} </el-checkbox>
        <el-tooltip effect="light" placement="top">
          <p style="width: 400px" slot="content">{{ $t("cluster.demoDataTip") }}</p>
          <el-icon class="el-icon-question icon-question"></el-icon>
        </el-tooltip>
      </el-form-item>
      <el-form-item :label="$t('plan.pricePlan')" prop="price_plan_id">
        <el-button @click="dialog = true" plain>{{ planBtnText }}</el-button>
      </el-form-item>
      <el-form-item>
        <el-button
          @click="create"
          :disabled="requestIng || cannotCreate"
          :loading="requestIng"
          style="width: 100%; margin-top: 10px"
          type="primary"
          >{{ $t("create") }}</el-button
        >
      </el-form-item>
      <p class="errorText" v-show="errorText">{{ errorText }}</p>
    </el-form>
    <p class="default-tip" @click="upgrade" v-show="cannotCreate" v-html="upgradePlanTip"></p>
    <Plan v-model="dialog" :cloudAndRegion="planCloudAndRegion" @close="dialog = false" @stepChange="stepChange" @change="planChange" />
  </div>
</template>

<script>
  import Plan from "components/Plan/PlanDialog";
  import { createCluster } from "api/gateway/app";
  export default {
    components: { Plan },
    data() {
      return {
        info: {
          alias: "",
          with_monitor: 0,
          hasDemoData: false,
          price_plan_id: "",
          price_level: "",
          cloudId: this.$store.getters.currentCloudAndRegion.cloudId,
          regionId: this.$store.getters.currentCloudAndRegion.regionId,
        },
        errorText: "",
        dialog: false,
        currentPlan: null,
        requestIng: false,
        dialogWidth: 1200,
        rules: {
          price_plan_id: [{ required: true, message: this.$t("plan.pleaseSelect") }],
        },
      };
    },
    computed: {
      currentAccountPlan() {
        return this.$store.getters.currentPricePlan || {};
      },
      cloudList() {
        return this.$store.state.app.cloudList;
      },
      regionList() {
        return this.cloudList.find(item => item.value == this.info.cloudId)?.regions || [];
      },
      planBtnText() {
        return this.currentPlan ? this.currentPlan.planName : this.$t("plan.selectPlan");
      },
      planCloudAndRegion() {
        return {
          cloudId: this.info.cloudId,
          regionId: this.info.regionId,
        };
      },
      upgradePlanTip() {
        let { clusterNum = 1, priceLevel = "free" } = this.currentAccountPlan;
        return this.$t("cluster.createInstanceUpgradeTip").replace("{size}", clusterNum).replace("{priceLevel}", priceLevel.toLowerCase());
      },
      cannotCreate() {
        let { clusterNum = 1 } = this.currentAccountPlan;
        let instanceLength = this.$store.state.app.clusters.length;
        if (clusterNum == -1) return false;
        return clusterNum <= instanceLength;
      },
    },
    mounted() {},
    methods: {
      create() {
        if (this.requestIng) return;
        this.$refs.form.validate(valid => {
          if (valid) {
            this.requestIng = true;
            createCluster(this.info)
              .then(async res => {
                await this.$store.dispatch("app/getClusterList", false);
                this.$message.success(this.$t("createSucc"));
                this.$store.commit(
                  "app/SET_CURRENT_CLUSTER",
                  this.$store.state.app.clusters.find(item => item.id == res.app_id)
                );
              })
              .catch(err => {
                this.errorText =
                  err?.data?.reduce((pre, cur) => {
                    return pre + cur?.error + ";";
                  }, "") || err.msg;
              })
              .finally(() => {
                this.requestIng = false;
              });
          }
        });
      },
      cloudChange() {
        this.info.regionId = this.regionList[0]?.value || "";
        this.regionChange();
      },
      regionChange() {
        this.currentPlan = null;
        this.info.price_plan_id = "";
        this.info.price_level = "";
      },
      planChange(plan) {
        this.currentPlan = plan;
        this.info.price_plan_id = plan.id;
        this.info.price_level = plan.priceLevel;
      },
      stepChange(step) {
        this.dialogWidth = step == 1 ? 1200 : 500;
      },
      upgrade(e) {
        if (e.target.tagName == "A") {
          this.$store.commit("SET_UPGRADE_DIALOG_VISIBLE", true);
        }
      },
    },
  };
</script>

<style lang="scss" scoped>
  .create {
    max-width: 800px;
  }
  .icon-question {
    // color: #464c4f;
    font-size: 16px;
    margin-left: 10px;
  }
</style>
