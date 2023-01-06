<template>
  <div class="page-wrapper">
    <MainContentHeader :title="$t('route.settings')"></MainContentHeader>
    <div class="content">
      <!-- <router-view /> -->
      <ThemeSetting></ThemeSetting>
    </div>
  </div>
</template>
<script>
import ThemeSetting from './views/main.vue'
export default {
  name: "Settings",
  components:{
     ThemeSetting
  },
  data() {
    return {
      message: "这是Settings页面",
    };
  },
};
</script>