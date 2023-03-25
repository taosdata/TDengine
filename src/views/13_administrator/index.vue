<template>
  <div class="page-wrapper">
    <MainContentHeader :title="$t('route.admin')"></MainContentHeader>
    <div class="content">
      <el-tabs value="user">
        <el-tab-pane name="user" :label="$t('taosuser.users')">
          <MgUser></MgUser>
        </el-tab-pane>
        <el-tab-pane name="backup" :label="$t('taosuser.backup')">
          <AdBackup></AdBackup>
          <!-- <MgDnodes></MgDnodes> -->
        </el-tab-pane>
        <el-tab-pane name="replication" :label="$t('taosuser.datareplication')">
          <AdReplication></AdReplication>
        </el-tab-pane>
        <!-- <el-tab-pane name="udf" :label="$t('topic.udf')" :disabled='true'>
          <MgUdf></MgUdf>
        </el-tab-pane>
        <el-tab-pane name="activities" :label="$t('topic.activities')" :disabled='true'>
          <Activities></Activities>
        </el-tab-pane> -->
      </el-tabs>
    </div>
  </div>
</template>
<script>
import MgUser from './views/user.vue'
import AdBackup from './views/backup.vue'
import AdReplication from './views/replication.vue'
// import MgQnodes from '../14_cluster/views/qnodes.vue'
import MgUdf from './views/udf.vue'
import Activities from './views/activities.vue'
export default {
  name: "Admin",
  components:{
    MgUser,AdBackup,AdReplication,MgUdf,Activities
  },
  data() {
    return {
      message: "这是Admin页面",
    };
  },
};
</script>