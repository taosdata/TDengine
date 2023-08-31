<template>
  <div class="sider" :class="sider_style">
    <SlideHeader />
    <el-menu
      ref="elMenu"
      :default-active="activeMenu"
      :collapse="isCollapse"
      active-text-color="rgb(37, 61, 172)"
      router
      class="sider_menu"
    >
      <template v-for="(route, index) in permission_routes">
        <SiderMenuItem
          v-show="route.meta.show"
          :key="route.path"
          :id="'menu_' + index"
          :item="route"
        ></SiderMenuItem
      ></template>
    </el-menu>
  </div>
</template>

<script>
import { SlideHeader, SiderMenuItem } from "./components";
import _ from "lodash";
import i18n from "@/lang";
const flag =
  !_.isEmpty(process.env.VUE_APP_CUS_NAME) &&
  process.env.VUE_APP_CUS_NAME !== "TDengine";
export default {
  components: { SlideHeader, SiderMenuItem },
  data() {
    return {
      isCollapse: false,
      language: window.navigator.language,
      permission_routes: [
          {
            path: "/dashboard",
            title: i18n.t("route.board"),
            icon: "dashboard",
            meta: {
              show: flag ? false : true,
            },
            // role: ["1"],
          },
          {
            path: "/dataIn",
            title: i18n.t("route.dataIn"),
            icon: "dataIn",
            meta: {
              show: flag ? false : true, //目前oem暂时不支持datain，后续根据taosx修改需要开放
            },
          },
          {
            path: "/explorer",
            title: i18n.t("route.console"),
            icon: "explorer",
            meta: {
              show: true,
            },
          },
          {
            path: "/dataOut",
            title: i18n.t("route.dataOut"),
            icon: "dataOut",
            meta: {
              show: flag ? false : true,
            },
          },
          {
            path: "/visualize",
            title: i18n.t("route.visualize"),
            icon: "visualize",
            meta: {
              show: flag ? false : true,
            },
          },
          {
            path: "/stream",
            title: i18n.t("route.stream"),
            icon: "stream",
            role: ["1"],
            meta: {
              show: true,
            },
          },
          {
            path: "/topic",
            title: i18n.t("route.topic"),
            icon: "topic",
            role: ["1"],
            meta: {
              show: true,
            },
          },
          {
            path: "/programming",
            title: i18n.t("route.programming"),
            icon: "programming",
            parting: false,
            meta: {
              show: flag ? false : true,
            },
          },
          {
            path: "/tools",
            title: i18n.t("route.tool"),
            icon: "tool",
            parting: true,
            meta: {
              show: flag ? false : true,
            },
            role: ["1"],
          },

          {
            path: "/management",
            title: i18n.t("route.admin"),
            icon: "users",
            parting: false,
            meta: {
              show: localStorage.getItem("username") == "root" ? true : false,
            },
          },
        ]
    };
  },

  computed: {
    sider_style() {
      return this.$store.state.sidebar.opened ? "sider_unfold" : "sider_fold";
    },
    role() {
      return this.$store.getters.role;
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
 
  mounted() {
    if (
      process.env.VUE_APP_CUS_CONFIG &&
      flag &&
      JSON.parse(process.env.VUE_APP_CUS_CONFIG).menus
    ) {
      let menus = JSON.parse(process.env.VUE_APP_CUS_CONFIG).menus;
      this.permission_routes = this.permission_routes.map((item) => {
        if (Object.keys(menus).includes(item.path.replace("/", ""))) {
          item.title = this.language.includes("en")
            ? menus[item.path.replace("/", "")].en
            : menus[item.path.replace("/", "")].zh;
        }
        return item;
      });
    }
  },
};
</script>

<style lang="scss" scoped>
.sider {
  box-shadow: rgba(0, 0, 0, 0.05) 0px -9px 9px;
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
