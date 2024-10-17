<template>
  <div class="inter_wrapper">
    <el-dropdown trigger="hover" placement="bottom" @command="handleSetLanguage">
      <div>
        <Icon name="inter" class="inter_svg"></Icon>
      </div>
      <el-dropdown-menu slot="dropdown">
        <el-dropdown-item :disabled="language === 'zh'" command="zh"> 中文 </el-dropdown-item>
        <el-dropdown-item :disabled="language === 'en'" command="en"> English </el-dropdown-item>
      </el-dropdown-menu>
    </el-dropdown>
  </div>
</template>

<script>
  import Icon from "@/components/Icon";

  export default {
    components: { Icon },
    computed: {
      language() {
        return this.$store.state.language;
      },
    },
    methods: {
      handleSetLanguage(lang) {
        this.$i18n.locale = lang;
        this.$store.commit("SET_LANGUAGE", lang);
        this.$message({
          message: "Switch Language Success",
          type: "success",
        });
      },
    },
  };
</script>

<style scoped>
  .inter_wrapper {
    cursor: pointer;
  }

  .inter_svg {
    width: 22px;
    height: 22px;
    margin-top: 6px;
  }
</style>
