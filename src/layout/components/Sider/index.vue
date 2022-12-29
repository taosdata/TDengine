<template>
  <div class="sider" :class="sider_style">
    <SlideHeader />
    <el-menu ref="elMenu" :default-active="activeMenu" :collapse="isCollapse" active-text-color="rgb(37, 61, 172)" router class="sider_menu">
      <SiderMenuItem v-for="(route, index) in permission_routes" :key="route.path" :id="'menu_' + index" :item="route"></SiderMenuItem>
    </el-menu>
  </div>
</template>

<script>
  import { SlideHeader, SiderMenuItem } from "./components";

  export default {
    components: { SlideHeader, SiderMenuItem },
    data() {
      return {
        isCollapse: false,
      };
    },

    computed: {
      sider_style() {
        return this.$store.state.sidebar.opened ? "sider_unfold" : "sider_fold";
      },
      role() {
        return this.$store.getters.role;
      },
      permission_routes() {
        const permission_routes = [
          {
            path: "/dashboard",
            title: this.$t("route.board"),
            icon: "dashboard",
            // role: ["1"],
          },
          {
            path: "/dataIn",
            title: this.$t("route.dataIn"),
            icon: "dataIn",
          },
          {
            path: "/explorer",
            title: this.$t("route.console"),
            icon: "explorer",
          },
          {
            path: "/dataOut",
            title: this.$t("route.dataOut"),
            icon: "dataOut",
          },
          {
            path: "/visualize",
            title: this.$t("route.visualize"),
            icon: "visualize",
          },
          {
            path: "/stream",
            title: this.$t("route.stream"),
            icon: "stream",
            role: ["1"],
          },
          {
            path: "/topic",
            title: this.$t("route.topic"),
            icon: "topic",
            role: ["1"],
          },
          {
            path: "/replication",
            title: this.$t("route.replication"),
            icon: "replication",
            role: ["1"],
          },
          {
            path: "/tools",
            title: this.$t("route.tool"),
            icon: "tool",
            parting: true,
            role: ["1"],
          },
          // {
          //   path: "/network",
          //   title: this.$t("network"),
          //   icon: "VPC",
          //   role: ["1"],
          // },
          {
            path: "/instances",
            title: this.$t("route.clusters"),
            icon: "cluster",
          },
          {
            path: "/user",
            title: this.$t("route.users"),
            icon: "users",
            role: ["1"],
          },
          {
            path: "/billing",
            title: this.$t("route.billing"),
            icon: "billing",
            role: ["1"],
          },
        ];
        return permission_routes.filter(route => {
          return route?.role?.includes(this.role) || !route.role;
        });
      },
      activeMenu() {
        const route = this.$route;
        const { meta, path } = route;
        if (meta.activeMenu) {
          return meta.activeMenu;
        }
        return "/" + path.split("/")[1];
      },
    },
  };
</script>

<style lang="scss" scoped>
  .sider {
    box-shadow: rgb(0 0 0 / 5%) 0px -9px 9px;
    position: relative;
    height: 100%;
    background-color: #fff;
    transition: width 0.4s ease 0s;
    display: flex;
    flex-direction: column;
  }
  .sider_fold {
    width: 60px;
  }

  .sider_unfold {
    width: 202px;
  }
  .sider_menu {
    border-right: none;
    display: flex;
    flex-direction: column;
    margin-top: 14px;
    overflow-x: hidden;
    flex: 1;
    overflow-y: auto;
  }
</style>
