<template>
  <div class="create">
    <section class="create-page-content">
      <h2 class="create-page-title">{{ $t("cluster.createFirstInstanceTitle") }}</h2>
      <el-form
        :hide-required-asterisk="true"
        :rules="rules"
        ref="form"
        :model="info"
        label-position="top"
        label-width="160px"
        style="width: 700px; margin: 0 auto"
      >
        <el-form-item :label="$t('dashboard.cloud')" prop="cloudId" required>
          <el-select class="w100" v-model="info.cloudId" @change="cloudChange" :placeholder="$t('dashboard.cloud')">
            <el-option v-for="item in cloudList" :key="item.value" v-bind="item" />
          </el-select>
        </el-form-item>
        <el-form-item :label="$t('dashboard.region')" prop="regionId" required>
          <el-select class="w100" v-model="info.regionId" @change="regionChange" :placeholder="$t('dashboard.region')">
            <el-option v-for="item in regionList" :key="item.value" v-bind="item" />
          </el-select>
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
          <el-button @click="create" :disabled="requestIng" :loading="requestIng" style="width: 100%; margin-top: 10px" type="primary">{{
            $t("create")
          }}</el-button>
        </el-form-item>
        <p class="errorText" v-show="errorText">{{ errorText }}</p>
      </el-form>
    </section>
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
        return this.$store.getters.currentPricePlan?.priceLevel || "";
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
    },
    methods: {
      create() {
        debugger
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
      planChange(plan) {
        this.currentPlan = plan;
        this.info.price_plan_id = plan.id;
        this.info.price_level = plan.priceLevel;
      },
      regionChange() {
        this.currentPlan = null;
        this.info.price_plan_id = "";
        this.info.price_level = "";
      },
      stepChange(step) {
        this.dialogWidth = step == 1 ? 1200 : 500;
      },
    },
  };
</script>

<style lang="scss" scoped>
  .create {
    width: 100%;
    min-height: 100vh;
    display: flex;
    justify-content: center;
    padding-top: 5vh;
    .create-page-content {
      width: 800px;
    }
    .create-page-title {
      font-size: 24px;
      text-align: center;
      font-weight: normal;
    }
  }
  .icon-question {
    // color: #464c4f;
    font-size: 16px;
    margin-left: 10px;
  }
</style>
