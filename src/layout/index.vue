<template>
  <div class="layout_wrapper" :class="sider_style">
    <Sider class="sider"></Sider>
    <div class="main">
      <LayoutHeader :reload="reload"></LayoutHeader>
      <main class="main_content">
        <router-view v-if="isRouterAlive"></router-view>
      </main>
    </div>
    <UpgradeDialog />
    <ContactDialog v-if="contactDialogVisible" v-model="contactDialogVisible" />
  </div>
</template>

<script>
  import { Sider, LayoutHeader } from "./components";
  import ResizeMixin from "./mixin/ResizeHandler";
  import UpgradeDialog from "./components/upgradeDialog.vue";
  import ContactDialog from "./components/ContactUs/popup.vue";
  export default {
    components: {
      Sider,
      LayoutHeader,
      UpgradeDialog,
      ContactDialog,
    },
    mixins: [ResizeMixin],
    data() {
      return {
        isRouterAlive: true,
      }
    },
    computed: {
      sider_style() {
        return this.$store.state.sidebar.opened ? "sider_unfold" : "sider_fold";
      },
      contactDialogVisible: {
        get() {
          return this.$store.state.contactDialogVisible;
        },
        set(val) {
          this.$store.commit("SET_CONTACT_DIALOG_VISIBLE", val);
        },
      },
      timezone() {
        return this.$store.state.app.timeZone
      }
    },
    mounted() {},
    methods: {
      reload() {
        this.isRouterAlive = false
        this.$nextTick(() => {
          this.isRouterAlive = true
        })
      }
    },
    watch: {
      timezone() {
        this.reload()
      }
    }
  };
</script>

<style scoped>
  .layout_wrapper {
    height: 100%;
    display: flex;
    flex-direction: row;
  }

  .sider {
    height: 100%;
    flex-shrink: 0;
  }

  .main {
    display: flex;
    flex-direction: column;
    flex: 1;
    overflow-x: auto;
  }

  .main_content {
    min-height: calc(100% - 58px);
    flex: 1;
    width: 100%;
    background-color: var(--color-background-layout-main);
    padding: 15px;
    overflow-y: auto;
  }
</style>
