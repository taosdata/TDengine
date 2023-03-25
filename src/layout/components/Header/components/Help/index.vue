<template>
  <div class="Help-wrapper">
    <el-dropdown trigger="hover" placement="bottom">
      <div class="avatar_block">
        <span>?</span>
      </div>
      <el-dropdown-menu slot="dropdown">
        <el-dropdown-item>
          <router-link class="drop-block" to="/support">
            <Icon name="support" class="dropdown_icon"></Icon>
            {{ $t("route.support") }}
          </router-link>
        </el-dropdown-item>
        <el-dropdown-item>
          <a class="drop-block" target="_blank" :href="docsUrl">
            <Icon name="document" class="dropdown_icon"></Icon>
            {{ $t("document") }}</a
          >
        </el-dropdown-item>
        <el-dropdown-item>
          <a class="drop-block" target="_blank" :href="discordUrl">
            <Icon name="discord" class="dropdown_icon"></Icon>
            {{ $t("discord") }}</a
          >
        </el-dropdown-item>
      </el-dropdown-menu>
    </el-dropdown>
  </div>
</template>

<script>
  export default {
    computed: {
      docsUrl() {
        return this.$store.state.language == "en" ? "https://docs.tdengine.com" : "https://docs.tdengine.com";
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
  .Help-wrapper {
    cursor: pointer;
    margin-right: 20px;
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
    text-align: center;
    color: $color-primary;
  }
  .avatar_svg {
    width: 26px;
    height: 26px;
  }
  .drop-block {
    display: flex;
    align-items: center;
    padding: 6px 0;
  }
  .dropdown_icon {
    width: 20px;
    height: 20px;
    margin-right: 8px;
  }
</style>
