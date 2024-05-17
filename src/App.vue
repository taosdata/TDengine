<template>
  <div id="app">
    <router-view :key="key"></router-view>
    <el-dialog
      :visible.sync="dialogVisible"
      :close-on-click-modal="false"
      v-bind="dialogConfig"
    >
      <component
        :is="dialogComponent"
        v-on="dialogListenter"
        @close="dialogVisible = false"
        v-bind="dialogParams"
      ></component>
    </el-dialog>
    <systemMes v-if="$COMMUNITY && showSystemMes"/>
  </div>
</template>

<script>
import { mapState } from "vuex";
import systemMes from './components/communityMes'
export default {
  name: "App",
  components: {systemMes},
  computed: {
    key() {
      return this.$store.state.app.current_cluster?.id || "";
    },
    ...mapState({
      dialogConfig: (state) => state.dialogConfig,
      dialogParams: (state) => state.dialogParams,
      dialogListenter: (state) => state.dialogListenters,
      dialogComponent: (state) => state.dialogComponent,
      showSystemMes: (state) => state.app.showSystemMes
    }),
    dialogVisible: {
      get() {
        return this.$store.state.dialogVisible;
      },
      set(val) {
        this.$store.commit("SET_DIALOG_VISIBLE", val);
      },
    },
  },
  mounted() {
    this.$nextTick(() => {
      if (
        process.env.VUE_APP_CUS_NAME &&
        process.env.VUE_APP_CUS_NAME !== "TDengine"
      ) {
        //是oem需要单独处理
        let link =
          document.querySelector("link[rel*='icon']") ||
          document.createElement("link");
        let title = document.querySelector("title");
        title.innerText = process.env.VUE_APP_CUS_NAME;
        link.remove();
      }
    });
  },
 

};
</script>

<style lang="scss" scoped>
#app :deep(.CodeMirror-placeholder) {
  color: #c0c4cc;
}
</style>
<style lang="scss">
.el-table th.el-table__cell > .cell{
  white-space: nowrap;
}
</style>
