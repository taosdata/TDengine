<template>
  <section>
    <p class="tip">
      {{ $t("vpc.howToSetUp") }}<a style="padding-left: 5px">{{ $t("vpc.viewDocs") }} <el-icon class="el-icon-document"></el-icon></a>
    </p>
    <el-form style="margin-top: 20px" size="small" :model="info" label-position="left" label-width="200px">
      <el-form-item :label="$t('register.CR')">
        <section class="two-block">
          <el-select class="item" v-model="info.cloud_id" :placeholder="$t('dashboard.cloud')" @change="() => (info.region_id = '')">
            <el-option v-for="item in cloudList" :key="item.value" v-bind="item" />
          </el-select>
          <el-select class="item" v-model="info.region_id" :placeholder="$t('dashboard.region')">
            <el-option v-for="item in regionList" :key="item.value" v-bind="item" />
          </el-select>
        </section>
      </el-form-item>
      <template v-if="info.cloud_id == 1">
        <el-form-item :label="$t('vpc.awsAccountId')">
          <section class="two-block">
            <el-input class="item" v-model.trim="info.appId" :placeholder="$t('vpc.awsAccountId')"></el-input>
            <a class="item tip" href="">{{ $t("vpc.howAwsAccountId") }}</a>
          </section> </el-form-item
        ><el-form-item label="VPC ID">
          <section class="two-block">
            <el-input class="item" v-model.trim="info.appId" placeholder="VPC ID"></el-input>
            <p class="item tip" href="">{{ $t("vpc.vpcAwsTip") }}</p>
          </section>
        </el-form-item>
      </template>
      <template v-else>
        <el-form-item :label="$t('vpc.applicationId')">
          <section class="two-block">
            <el-input class="item" v-model.trim="info.appId" :placeholder="$t('vpc.projectId')"></el-input>
            <a class="item tip" href="">{{ $t("vpc.howFindProId") }}</a>
          </section>
        </el-form-item>
        <el-form-item :label="$t('vpc.vpcNetworkName')">
          <section class="two-block">
            <el-input class="item" v-model.trim="info.appId" :placeholder="$t('vpc.projectId')"></el-input>
            <a class="item tip" href="">{{ $t("vpc.howFindNetName") }}</a>
          </section>
        </el-form-item>
      </template>
      <el-form-item label="VPC CIDR">
        <section class="two-block">
          <el-input class="item" v-model.trim="info.appId" :placeholder="$t('vpc.projectId')"></el-input>
          <p class="item tip" href="">
            {{ $t(info.cloud_id == 1 ? "vpc.cidrTip" : "vpc.awsCIDRTip") }}
          </p>
        </section>
      </el-form-item>
      <el-form-item>
        <el-button style="width: 100%" type="primary">{{ $t("create") }}</el-button>
      </el-form-item>
    </el-form>
  </section>
</template>

<script>
export default {
  data() {
    return {
      info: {
        cloud_id: "",
        region_id: "",
      },
    };
  },
  computed: {
    cloudList() {
      return this.$store.state.app.cloudList;
    },
    regionList() {
      return this.cloudList.find(item => item.value == this.info.cloud_id)?.regions || [];
    },
  },
  created() {
    if (this.cloudList.length) {
      let data = this.cloudList[0];
      this.info.cloud_id = data.value;
      this.info.region_id = data.regions[0]?.value;
    }
  },
  methods: {},
};
</script>

<style lang="scss" scoped>
.two-block {
  display: flex;
  justify-content: space-between;
  align-items: center;
  .item {
    width: 48%;
  }
}
.tip {
  font-size: 16px;
}
</style>
