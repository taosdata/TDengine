<template>
  <div class="page-wrapper">
    <MainContentHeader :title="$t('route.admin')"></MainContentHeader>
    <div class="content">
      <el-tabs value="user">
        <el-tab-pane name="user" :label="$t('taosuser.users')">
          <MgUser></MgUser>
        </el-tab-pane>
        <el-tab-pane name="backup" :label="$t('taosuser.backup')" :disabled='taosxDisabled' lazy>
          <AdBackup ></AdBackup>
        </el-tab-pane>
        <el-tab-pane name="replication" :label="$t('taosuser.datareplication')" :disabled='taosxDisabled' lazy>
          <AdReplication ></AdReplication>
        </el-tab-pane>
        <el-tab-pane name="cluster" :label="$t('route.cluster')" lazy>
          <Cluster></Cluster>
        </el-tab-pane>
        <el-tab-pane name="license" :label="$t('topic.license')" lazy>
          <License></License>
        </el-tab-pane>
        <el-tab-pane name="agent" :label="$t('topic.agent')" lazy>
          <Agent></Agent>
        </el-tab-pane>
      </el-tabs>
    </div>
  </div>
</template>
<script>
import MgUser from './views/user.vue'
import AdBackup from './views/backup.vue'
import AdReplication from './views/replication.vue'
import Agent from './views/agents.vue'
import License from './views/license.vue'
import Activities from './views/activities.vue'
import Cluster from '@/views/14_cluster/index.vue'
export default {
  name: "Admin",
  components:{
    MgUser,AdBackup,AdReplication,License,Activities,Cluster,Agent
  },
  data() {
    return {
      message: "这是Admin页面",
      taosxDisabled:false
    };
  }
};
</script>