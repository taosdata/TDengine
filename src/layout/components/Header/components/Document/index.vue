<template>
  <div class="doc-block">
    <el-dropdown trigger="hover" placement="bottom">
      <div class="avatar_block">
        <span>?</span>
      </div>
      <!-- <a class="drop-block" target="_blank" :href="docsUrl"> -->
      <!-- <el-tooltip
          class="item"
          effect="light"
          :content="$t('document')"
          placement="bottom"
        >
          <div class="avatar_block">
            <span>?</span>
          </div>
          <Icon
            name="document"
            class="dropdown_icon"
            style="width: 26px; height: 26px; margin-left: 2px"
          ></Icon>
        </el-tooltip> -->
      <!-- </a> -->
      <el-dropdown-menu slot="dropdown">
        <el-dropdown-item style="display: flex; align-items: center">
          <!-- <router-link class="drop-block" to="/docsUrl"> -->
          <a class="drop-block" target="_blank" :href="docsUrl">
            <div style="display:flex;align-items:center;">
              <Icon
                name="document"
                class="dropdown_icon"
                style="width: 20px; height: 20px; margin-left: 2px"
              ></Icon>
              <span style="color: #4259ce; margin-left: 6px">{{
                $t("document")
              }}</span>
            </div>
          </a>
          <!-- </router-link> -->
        </el-dropdown-item>
      </el-dropdown-menu>
    </el-dropdown>
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
    },
  },
};
</script>

<style lang="scss" scoped>
.doc-block {
  border-color: #4259ce;
  color: #4259ce;
  margin-right: 20px;
  margin-left: 15px;
  box-sizing: border-box;
  cursor: pointer;
  position: relative;
}
.avatar_block {
  margin-top: 4px;
  display: flex;
  align-items: center;
  justify-content: center;
  width: 26px;
  height: 26px;
  border: 1px solid $color-primary;
  border-radius: 50%;
  color: $color-primary;
}
.el-dropdown-menu__item {
  display: flex;
  align-items: center;
}
</style>
