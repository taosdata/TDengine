<template>
  <section class="slide-header" :class="sider_style">
    <Logo></Logo>
    <MenuTrigger :class="['menuTrigger', isOem ? 'oem' : '']"></MenuTrigger>
  </section>
</template>

<script>
import { Logo, MenuTrigger } from "./index.js";
export default {
  data() {
    return {
      isOem: process.env.VUE_APP_CUS_NAME !== "",
    };
  },
  components: { MenuTrigger, Logo },
  computed: {
    sider_style() {
      return this.$store.state.sidebar.opened ? "sider_unfold" : "sider_fold";
    },
  },
};
</script>

<style scoped lang="scss">
.slide-header {
  position: relative;
  z-index: 2;
  background-color: #fff;
  transition: width 0.4s ease 0s;
  flex-shrink: 0;
}
.menuTrigger {
  position: absolute;
  right: -10px;
  top: 18px;
}
.menuTrigger.oem {
  top: 60px;
}
.sider_fold {
  width: 60px;
  ::v-deep {
    .oem {
      max-width: 60px;
    }
  }
}

.sider_unfold {
  width: 202px;
  ::v-deep {
    .oem {
      max-width: 200px;
    }
  }
}
</style>
