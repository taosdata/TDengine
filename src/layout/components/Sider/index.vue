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
const flag = !_.isEmpty(process.env.VUE_APP_CUS_NAME) && process.env.VUE_APP_CUS_NAME !=='TDengine' ;
export default {
  components: { SlideHeader, SiderMenuItem },
  data() {
    return {
      isCollapse: false,
      isRoot:localStorage.getItem('username')
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
          meta: {
            show: flag ? false : true,
          },
          // role: ["1"],
        },
        {
          path: "/dataIn",
          title: this.$t("route.dataIn"),
          icon: "dataIn",
          meta:{
            show:true
          }
        },
        {
          path: "/explorer",
          title: this.$t("route.console"),
          icon: "explorer",
          meta:{
            show:true
          }
        },
        {
          path: "/dataOut",
          title: this.$t("route.dataOut"),
          icon: "dataOut",
          meta: {
            show: flag ? false : true,
          },
        },
        {
          path: "/visualize",
          title: this.$t("route.visualize"),
          icon: "visualize",
          meta:{
            show: flag ? false : true,
          }
        },
        {
          path: "/stream",
          title: this.$t("route.stream"),
          icon: "stream",
          role: ["1"],
          meta:{
            show:true
          }
        },
        {
          path: "/topic",
          title: this.$t("route.topic"),
          icon: "topic",
          role: ["1"],
          meta:{
            show:true
          }
        },
        // {
        //   path: "/replication",
        //   title: this.$t("route.replication"),
        //   icon: "replication",
        //   role: ["1"],
        // },
        {
          path: "/programming",
          title: this.$t("route.programming"),
          icon: "programming",
          parting: false,
          meta: {
            show: flag ? false : true,
          },
        },
        // {
        //   path: "/healthreport",
        //   title: this.$t("route.healthreport"),
        //   icon: "healthreport",
        //   parting: false,
        //   meta: {
        //     show: flag ? false : true,
        //   },
        // },
        {
          path: "/tools",
          title: this.$t("route.tool"),
          icon: "tool",
          parting: true,
          meta: {
            show: flag ? false : true,
          },
          role: ["1"],
        },
        // {
        //   path: "/backup",
        //   title: this.$t("route.backup"),
        //   icon: "backup",
        //   parting: true
        // },
        // {
        //   path: "/users",
        //   title: this.$t("route.users"),
        //   icon: "users",
        //   parting: true
        // },

        // {
        //   path: "/qnodes",
        //   title: this.$t("route.qnodes"),
        //   icon: "qnodes",
        //   parting: true
        // },
        // {
        //   path: "/mnodes",
        //   title: this.$t("route.mnodes"),
        //   icon: "mnodes",
        //   parting: true
        // },
        // {
        //   path: "/dnodes",
        //   title: this.$t("route.dnodes"),
        //   icon: "dnodes",
        //   parting: true
        // },

        {
          path: "/admin",
          title: this.$t("route.admin"),
          icon: "users",
          parting: false,
          meta:{
            show:this.isRoot=='root'? true:false
          }
        },
        // {
        //   path: "/cluster",
        //   title: this.$t("route.cluster"),
        //   icon: "cluster",
        //   parting: false,
        //   meta:{
        //     show:true
        //   }
        // },

        // {
        //   path: "/settings",
        //   title: this.$t("route.settings"),
        //   icon: "settings",
        //   parting: true,
        //   meta: {
        //     show: flag ? false : true,
        //   },
        // },
        // {
        //   path: "/network",
        //   title: this.$t("network"),
        //   icon: "VPC",
        //   role: ["1"],
        // },
        // {
        //   path: "/instances",
        //   title: this.$t("route.clusters"),
        //   icon: "cluster",
        // },
        // {
        //   path: "/user",
        //   title: this.$t("route.users"),
        //   icon: "users",
        //   role: ["1"],
        // },
        // {
        //   path: "/billing",
        //   title: this.$t("route.billing"),
        //   icon: "billing",
        //   role: ["1"],
        // },
      ];
      let result =permission_routes.filter((route) => {
        return route?.meta?.show;
      })
      return result;
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
  mounted(){
    // console.log(process.env.VUE_APP_CUS_INFO,'可配置的彩蛋项目');
  }
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
