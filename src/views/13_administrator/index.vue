<template>
  <div class="page-wrapper">
    <MainContentHeader :title="$t('route.admin')"></MainContentHeader>
    <div class="content">
      <el-tabs value="user">
        <el-tab-pane name="user" :label="$t('topic.users')">
          <MgUser></MgUser>
        </el-tab-pane>
        <el-tab-pane name="backup" :label="$t('topic.backup')">
          <AdBackup></AdBackup>
          <!-- <MgDnodes></MgDnodes> -->
        </el-tab-pane>
        <el-tab-pane name="replication" :label="$t('topic.replication')">
          <AdReplication></AdReplication>
          <!-- <MgMnodes></MgMnodes> -->
        </el-tab-pane>
      </el-tabs>
    </div>
  </div>
</template>
<script>
import MgUser from './views/user.vue'
import AdBackup from './views/backup.vue'
import AdReplication from './views/replication.vue'
// import MgQnodes from '../14_cluster/views/qnodes.vue'
// import MgUdf from '../14_cluster/views/udf.vue'
export default {
  name: "Admin",
  components:{
    MgUser,AdBackup,AdReplication
  },
  data() {
    return {
      message: "这是Admin页面",
    };
  },
};
</script>