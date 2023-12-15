<template>
  <div class="page-wrapper">
    <MainContentHeader :title="$t('route.support')">
      <section class="add-work-order">
        <el-button size="mini" plain @click="addSupport()" icon="el-icon-plus">{{ $t("support.addOrder") }}</el-button>
      </section>
    </MainContentHeader>
    <div class="content">
      <router-view></router-view>
    </div>
    <!-- 添加工单 -->
    <el-dialog :title="$t('support.addOrder')" width="1000px" :visible.sync="dialog" :close-on-click-modal="false">
      <!-- <addFrom :typeList="issueTypeList" @close="dialogClose" /> -->
    </el-dialog>
  </div>
</template>

<script>
  import addFrom from "./components/addForm.vue";
  export default {
    components: { addFrom },
    data() {
      return {
        activeTab: "doc",
        dialog: false,
        issueTypeList: [],
        issueTypeObj: {},
      };
    },
    created() {
      // this.$store.dispatch("issues/getIssueTypeList");
    },
    mounted() {},
    computed: {},

    methods: {
      addSupport() {
        this.dialog = true;
      },
      dialogClose() {
        let flag = this.$route.path == "/support";
        flag && this.$store.dispatch("issues/getIssueList");
        this.dialog = false;
        !flag && this.$router.push("/support");
      },
    },
  };
</script>

<style lang="scss" scoped>
  .add-work-order {
    margin-left: 30px;
  }
  .navName {
    font-size: 15px;
    color: #0052cc;
    cursor: pointer;
  }
</style>
