<template>
  <div class="page-wrapper">
    <MainContentHeader :title="$t('network')"></MainContentHeader>
    <section class="content">
      <el-tabs value="vpc">
        <el-tab-pane name="vpc" label="VPC">
          <VPC></VPC>
        </el-tab-pane>
        <el-tab-pane name="traffic" label="Traffic">
          <Traffic></Traffic>
        </el-tab-pane>
      </el-tabs>
    </section>
  </div>
</template>

<script>
import VPC from "./views/list";
import Traffic from "./views/traffic";
export default {
  components: {
    VPC,
    Traffic,
  },
};
</script>

<style scoped></style>
