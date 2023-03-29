<template>
  <div class="doc-block">
    <a class="drop-block" target="_blank" :href="docsUrl">
      <el-tooltip class="item" effect="light" :content="$t('document')" placement="bottom">
        <Icon
          name="document"
          class="dropdown_icon"
          style="width: 26px; height: 26px; margin-left: 2px;"
        ></Icon>
      </el-tooltip>
    </a>
  </div>
</template>

<script>
export default {
  name: "Dcoument",
  computed: {
    docsUrl() {
      return this.$store.state.language == "en"
        ? "https://docs.tdengine.com/"
        : "https://docs.tdengine.com/";
    },
    discordUrl() {
      return this.$store.state.language == "en"
        ? "https://discord.com/invite/VZdSuUg4pS?utm_id=discord"
        : "https://discord.com/invite/VZdSuUg4pS?utm_id=discord";
    }
  }
};
</script>

<style lang="scss" scoped>
.doc-block {
  border-color: #4259ce;
  color: #4259ce;
  height: 20px;
  margin-right: 20px;
  box-sizing: border-box;
  cursor: pointer;
}
</style>