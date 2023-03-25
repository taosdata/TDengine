<template>
  <div class="dnodes">
    <el-tabs value="cpu" type="card">
      <el-tab-pane name="cpu" :label="$t('health.cpu')">
        <HealthCpu></HealthCpu>
      </el-tab-pane>
      <el-tab-pane name="memory" :label="$t('health.memory')">
        <HealthMem></HealthMem>
      </el-tab-pane>
      <el-tab-pane name="disk" :label="$t('health.disk')">
        <HealthDisk></HealthDisk>
      </el-tab-pane>
    </el-tabs>
  </div>
</template>
<script>
import HealthCpu from '../components/healthCpu.vue'
import HealthMem from '../components/healthMem.vue'
import HealthDisk from '../components/healthDisk.vue'
export default {
  name: "DnodeHealth",
  components:{HealthCpu,HealthMem,HealthDisk},
  data() {
    return {};
  },
};
</script>