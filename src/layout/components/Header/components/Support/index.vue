<template>
  <div class="support-content">
    <el-tooltip class="item" effect="light" content="Support" placement="bottom">
      <div class="support-block" @click="showSupportDialog">
        <Icon name="support" style="width: 26px; height: 26px; margin-left: 2px;"></Icon>
      </div>
    </el-tooltip>
      
    

    <el-dialog :title="$t('support.addOrder')" width="1000px" :visible.sync="dialog" append-to-body>
      <addFrom :typeList="issueTypeList" @close="dialogClose" />
    </el-dialog>
  </div>
</template>

<script>
import Icon from "@/components/Icon";
import addFrom from "@/views/support/components/addForm.vue";
export default {
  name: "Support",
  components: { Icon, addFrom },
  data() {
    return {
      dialog: false
    };
  },
  created() {
    this.$store.dispatch("issues/getIssueTypeList");
  },
  methods: {
    showSupportDialog() {
      console.log("support");
      this.dialog = true;
    },
    dialogClose() {
      this.dialog = false;
    }
  }
};
</script>

<style lang="scss" scoped>
.support-block {
  border-color: #4259ce;
  color: #4259ce;
  height: 25px;
  margin-right: 20px;
  box-sizing: border-box;
  cursor: pointer;
}
</style>