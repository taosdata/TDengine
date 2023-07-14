<template>
  <div class="menu_item">
    <el-tooltip
      :disabled="opened"
      class="item"
      effect="dark"
      :content="item.title"
      placement="right"
    >
      <el-menu-item
        :index="item.path"
        @contextmenu.native.prevent="menuRight(item.path)"
        :disabled="!isDisabled(item.path)"
        class="menuItem"
        @click="menuClick(item.title)"
      >
        <div :aria-data="item.path">
          <span>
            <Icon
              :name="item.icon"
              class="menuItem_icon"
              :class="{ menuItem_icon_unfold: opened }"
            ></Icon>
          </span>
          <span class="menuItem_title" v-if="opened">
            {{ item.title }}
          </span>
        </div>
      </el-menu-item>
    </el-tooltip>
    <el-divider style="margin-bottom: 10px" v-if="item.parting"></el-divider>
  </div>
</template>

<script>
import { mapState } from "vuex";
import { BaseRoute } from "@/const";
import { OpenNewTab } from "@/utils";
export default {
  name: "MenuItem",
  props: ["item"],
  computed: mapState("sidebar", ["opened"]),
  data() {
    return {};
  },
  methods: {
    isDisabled(path) {
      // if (!this.$store.getters.hasCluster) return false;
      // return BaseRoute.includes(path) || this.$store.getters.operate;
      return true;
    },
    menuRight(path) {
      OpenNewTab(path);
    },
    open2() {
      this.$notify({
        title: "警告",
        dangerouslyUseHTMLString: true,
        duration: 0,
        message: `<div>
          ${this.$t("dashboard.warnigtip")}
          </div>`,
        type: "warning",
      });
    },
    menuClick(val) {
      let url = localStorage.getItem("local_grafana");
      if (val === "Dashboard" || val === "面板") {
        // this.$notify({
        //   title: 'Tips:',
        //   message: 'You can use Grafana to monitor the TDengine running status, please follow the steps below: ',
        //   duration: 0
        // });
        // window.open('https://docs.taosdata.com/reference/tdinsight/#','_blank')
        if (url) {
          OpenNewTab(url);
        } else {
          // OpenNewTab(null);
          // this.open2()
        }
      }
    },
  },
};
</script>

<style lang="scss" scoped>
.menu_item {
  margin-top: 10px;
  // background: 0px 0px no-repeat padding-box padding-box rgb(232, 239, 255);
}

.menuItem_icon {
  width: 24px;
  height: 24px;
  position: absolute;
  top: 50%;
  transform: translateY(-50%);
}

.menuItem_icon_unfold {
  left: 50px;
}

.menuItem_title {
  position: absolute;
  left: 90px;
  font-size: 16px;
  font-weight: 500;
  top: 50%;
  transform: translateY(-50%);
}

.menuItem {
  position: relative;
}
</style>
