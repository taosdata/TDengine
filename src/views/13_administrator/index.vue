<template>
  <div class="page-wrapper">
    <MainContentHeader :title="$t('route.admin')"></MainContentHeader>
    <div class="content">
      <el-tabs v-model="activeName">
        <el-tab-pane name="user" :label="$t('taosuser.users')">
          <MgUser></MgUser>
        </el-tab-pane>
        <el-tab-pane name="backup" :label="$t('taosuser.backup')" :disabled='taosxDisabled' lazy v-if="getMetaShow('backup_restore')">
          <AdBackup ></AdBackup>
        </el-tab-pane>
        <el-tab-pane name="replication" :label="$t('taosuser.datareplication')" :disabled='taosxDisabled' lazy v-if="isLessThen3_3_3_0 ? getMetaShow('td3.0') : getMetaShow('data_sync')">
          <AdReplication ></AdReplication>
        </el-tab-pane>
        <el-tab-pane name="cluster" :label="$t('route.cluster')" lazy v-if="getMetaShow('dnodes')">
          <Cluster></Cluster>
        </el-tab-pane>
        <el-tab-pane name="license" :label="$t('topic.license')" lazy v-if="!$COMMUNITY">
          <License></License>
        </el-tab-pane>
        <el-tab-pane name="audit" :label="$t('topic.audit')" lazy v-if="getMetaShow('audit')">
          <Audit :activeName="activeName"></Audit>
        </el-tab-pane>
      </el-tabs>
      <el-alert
        v-if="$COMMUNITY && (activeName == 'backup' || activeName == 'replication' || activeName == 'audit')"
        style="margin-top: 8px"
        class="my-alert"
        type="warning"
        :description="$t('communityDemoDataTip')"
        :closable="true"
        center
      />
    </div>
  </div>
</template>
<script>
import MgUser from './views/user.vue'
import AdBackup from './views/backup.vue'
import AdReplication from './views/replication.vue'
import License from './views/license.vue'
import Activities from './views/activities.vue'
import Cluster from '@/views/14_cluster/index.vue'
import Audit from './views/audit.vue'
import SlowSql from './views/slowSql.vue'
import LicenseMixin from "@/mixins/license";
import { compareVersion } from "@/utils";
export default {
  name: "Admin",
  components:{
    MgUser,AdBackup,AdReplication,License,Activities,Cluster,Audit
  },
  mixins: [LicenseMixin],
  data() {
    return {
      message: "这是Admin页面",
      taosxDisabled:false,
      activeName: 'user'
    };
  },
  computed: {
    TDengineVersion() {
      return localStorage.getItem("agent_version");
    },
    isLessThen3_3_3_0() {
      return compareVersion(this.TDengineVersion, '<3.3.3.0')
    }
  },
  created() {
    let version = localStorage.getItem("agent_version");
    let [a, b, c, d] = version.split(".");
    if (a > 3 || (a == 3 && b >= 1 && c >= 2)) {
      this.version_gt_equ_3330 = true;
    }
  }
};
</script>
<style scoped lang="scss">
  .my-alert ::v-deep.el-alert .el-alert__description  {
    font-size: 14px;
  }
</style>