<template>
  <div class="page-wrapper">
    <!-- <PageHeader :title="$t('dashboard.overview')"></PageHeader> -->
    <div class="content">
      <!-- <docs v-if="!grafanaDashboard" :category="'dashboard'" :lang="'Dashboard'"></docs> -->

      <dnodes v-if="!grafanaDashboard"></dnodes>
      <el-tabs v-else style="height: 100%">
        <el-tab-pane :key="`dashboard-endpoint`" style="height:100%;" :label="$t('dashboard.cluster')">
          <dnodes></dnodes>
        </el-tab-pane>
        <el-tab-pane v-for="(item, index) in grafanaDashboard" :key="`dashboard-${index}`" style="height:100%;" :label="item.key">
          <iframe
            :src="item.url"
            width="100%"
            height="100%"
            frameborder="0"
            scrolling="auto"></iframe>
        </el-tab-pane>
      </el-tabs>
    </div>
  </div>
</template>

<script setup lang="ts">
// import Docs from '@/components/document/docs.vue';
import dnodes from './dnodes.vue'

const grafanaDashboard = ref<any>(null);
const grafana_dashboards = localStorage.getItem("local_grafana");
if (grafana_dashboards) {
  grafanaDashboard.value = JSON.parse(grafana_dashboards);
}

</script>

<style lang="scss" scoped></style>
