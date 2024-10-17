<template>
  <div class="support-content">
    <a class="support-block-url" target="_blank" :href="supportUrl">
    <el-tooltip class="item" effect="light" :content="$t('support.support')" placement="bottom">
      <div class="support-block" @click="showSupportDialog">
        <Icon name="support" style="width: 26px; height: 26px; margin-left: 2px;"></Icon>
      </div>
    </el-tooltip>
    </a>
      
    

    <el-dialog :title="$t('support.addOrder')" width="1000px" :visible.sync="dialog" append-to-body :close-on-click-modal="false">
      <!-- <addFrom :typeList="issueTypeList" @close="dialogClose" /> -->
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
   computed: {
    supportUrl() {
      return this.$store.state.language == "en"
        ? "https://tdengine.com/"
        : "https://tdengine.com/";
    }
  },
  created() {
    // this.$store.dispatch("issues/getIssueTypeList");
  },
  methods: {
    showSupportDialog() {
      console.log("support");
      // this.dialog = true;
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