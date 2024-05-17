<template>
  <div class="page-wrapper">
    <MainContentHeader :title="$t('route.console')">
      <router-link slot="right" to="/tools/docs/tool/TDengine%20CLI" v-if="!oemName">{{ $t("data.exportDataViaCli") }}</router-link>
    </MainContentHeader>
    <div class="console-content block-style">
      <div id="left" class="left">
        <TreeView :addSql.sync="addSql"></TreeView>
      </div>
      <div id="bar_row" class="bar_row"></div>
      <PartView></PartView>
    </div>
  </div>
</template>

<script>
  import TreeView from "./views/tree.vue";
  import PartView from "./views/part.vue";
  export default {
    name: "tables",
    components: {
      TreeView,
      PartView,
    },
    data() {
      return {
        addSql: "",
        panelData: {},
        oemName:
        process.env.VUE_APP_CUS_NAME &&
        process.env.VUE_APP_CUS_NAME !== "TDengine",
      };
    },
    computed: {},
    created() {
      if (this.$COMMUNITY) {
        this.$store.commit('app/SET_SHOW_SYSTEM_MES', true)
      }
      
      // 获取当前用户的控制台执行记录
      this.$store.state.console.history = localStorage.getItem("record_history") //+ this.$store.getters.appId)
        ? JSON.parse(localStorage.getItem("record_history"))// + this.$store.getters.appId))
        : [];
    },
    mounted() {
      this.dragChangeWidth("bar_row", "left");
    },
    methods: {
      dragChangeWidth(drag, panel) {
        let dragEl = document.getElementById(drag);
        let panelEl = document.getElementById(panel);
        dragEl.onmousedown = ev => {
          let disW = panelEl.offsetWidth;
          let disX = ev.clientX;

          document.onmousemove = ev => {
            panelEl.style.width = disW + (ev.clientX - disX) + "px";
          };
          document.onmouseup = () => {
            document.onmousemove = document.onmouseup = null;
          };
          return false;
        };
      },
    },
    beforeDestroy() {
      this.$store.state.console.partActive = "sql";
    },
  };
</script>

<style lang="scss" scoped>
  $bar-color: #f5f5f5;
  $bar-light-color: #dcdfe6;
  .console-content {
    display: flex;
    flex-direction: row;
    flex: 1;
    overflow: hidden;
    border: none !important;
  }
  .top {
    flex: 1;
    height: 300px;
    min-height: 200px;
    display: flex;
    flex-direction: row;
    border: 1px solid #dcdfe6;
  }
  .bar_row {
    width: 10px;
    height: 100%;
    cursor: e-resize;
    background-color: $bar-color;
  }
  .bar_row:hover {
    background-color: $bar-light-color;
  }

  .left {
    width: 20%;
    flex-shrink: 0;
    border: 1px solid #dcdfe6;
    box-shadow: 0 2px 4px 0 rgba(0, 0, 0 , 12%), 0 0 6px 0 rgba(0, 0, 0 , 4%);
  }
  .right {
    background-color: #ffffff;
    width: 100%;
    flex: 1;
    // height:80%;
  }
  .bottom {
    flex: 1;
    min-height: 20%;
  }
</style>
