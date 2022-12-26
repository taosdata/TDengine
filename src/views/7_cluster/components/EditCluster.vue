<template>
  <el-form
    ref="form"
    style="text-align: left; margin-bottom: 10px"
    size="small"
    :hide-required-asterisk="true"
    :model="info"
    label-position="left"
    label-width="120px"
  >
    <el-form-item v-if="editType == 0" :label="$t('clusterName')" prop="alias" required>
      <el-input v-model.trim="info.alias" maxlength="64"></el-input>
    </el-form-item>
    <template v-if="editType == 1">
      <el-form-item :label="$t('dashboard.cloud')">
        <el-select class="w100" v-model="cloud" :placeholder="$t('dashboard.cloud')">
          <el-option v-for="item in cloudList" :key="item.value" v-bind="item" />
        </el-select>
      </el-form-item>
      <el-form-item :label="$t('dashboard.region')">
        <el-select class="w100" v-model="region" :placeholder="$t('dashboard.region')">
          <el-option v-for="item in regionList" :key="item.value" v-bind="item" />
        </el-select>
      </el-form-item>
    </template>
    <el-row>
      <el-col :span="11">
        <el-button size="small" class="w100" :disabled="requestIng" @click="$emit('close')">{{ $t("cancel") }}</el-button>
      </el-col>
      <el-col :span="11" :offset="2">
        <el-button size="small" :disabled="btnDisabled" :loading="requestIng" class="w100" type="primary" @click="handle">{{
          $t("confirm")
        }}</el-button>
      </el-col>
    </el-row>
    <p class="errorText" v-show="errorText">{{ errorText }}</p>
  </el-form>
</template>

<script>
  import { getCloudRegion } from "@/api/register";
  import { changeCluster } from "@/api/gateway/app";
  export default {
    props: {
      info: {
        type: Object,
        default() {
          return {};
        },
      },
      editType: {
        type: Number,
        default: 0,
      },
    },
    data() {
      return {
        errorText: "",
        requestIng: false,
        cloud: this.$store.getters.currentCloudAndRegion.cloudId,
        region: this.$store.getters.currentCloudAndRegion.regionId,
      };
    },
    computed: {
      regionList() {
        return this.cloudList.find(item => item.value == this.cloud)?.regions || [];
      },
      cloudList() {
        return this.$store.state.app.cloudList;
      },
      btnDisabled() {
        if (this.requestIng) return true;
        return this.editType == 1 && (!this.cloud || !this.region);
      },
    },
    watch: {
      cloud() {
        this.region = this.regionList[0]?.value || "";
      },
    },
    methods: {
      handle() {
        if (this.editType == 1) return this.next();
        if (this.requestIng) return;
        this.errorText = "";
        this.$refs.form.validate(async valid => {
          if (valid) {
            this.requestIng = true;
            if (this.editType == 0) {
              const parmas = { alias: this.info.alias, app_id: this.info.id, cloudId: this.info.cloud_id, regionId: this.info.region_id };
              await changeCluster(parmas)
                .then(() => {
                  this.$emit("close", 2);
                  this.$message.success(this.$t("changeSucc"));
                })
                .catch(() => {});
              this.$store.dispatch("app/getClusterList");
            }
            this.requestIng = false;
          }
        });
      },
      async getCloud() {
        this.cloudList = await getCloudRegion().catch(() => []);
        // 默认选中第一项
        if (this.cloudList.length > 0) {
          let data = this.cloudList[0];
          this.info.cloud_id = data.value;
          this.info.region_id = data.regions[0]?.value;
        }
      },
      next() {},
    },
  };
</script>

<style></style>
