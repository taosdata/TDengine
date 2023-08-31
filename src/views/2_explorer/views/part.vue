<template>
  <div class="part">
    <div v-show="$store.state.console.partActive == 'sql' || $store.state.console.partActive == 'wizard'" class="sql-btn">
      <el-button 
        v-show="$store.state.console.partActive == 'wizard'"
        :disabled="previewBtn" 
        @click="getPreviewSql" 
        size="mini"
      >
        {{ $t('sqlPreview') }}
      </el-button>
      <el-tooltip class="item" effect="light" placement="bottom-end">
        <div slot="content" class="flexCenter">
          <span>{{ $t("data.runSqlTip") }}</span>
          <Icon class="icon-shift" name="shift" />
          <Icon class="icon-shift" name="enter" />
        </div>
        <el-button
          :disabled="
            $store.state.console.partActive == 'sql' 
            ? (!sqlStr || requestIng) 
            : (previewBtn || requestIng)"
          type="primary"
          icon="el-icon-caret-right"
          :loading="requestIng"
          @click="handleSendSQL"
          size="mini"
        >
          <span>{{ $t("run") }}</span>
        </el-button>
      </el-tooltip>

      <el-button 
        :disabled=" 
          $store.state.console.partActive == 'sql' 
          ? (!sqlStr || requestIng) 
          : (previewBtn || requestIng)" type="success" @click="toggleFavorite" size="mini">
        <template v-if="!favorited">
          <el-icon class="el-icon-star-on" />
          <span class="add_favorite_text">{{ $t("console.addFavorites") }}</span>
        </template>
        <template v-else>
          <el-icon class="el-icon-star-off" />
          <span class="add_favorite_text">{{ $t("saved") }}</span>
        </template>
      </el-button>
    </div>
    <el-tabs @tab-click="tabClick" v-model="$store.state.console.partActive" type="border-card">
      <el-tab-pane name="wizard" label="Wizard">
        <section class="sql-wrapper">
          <Wizard ref="wizard"></Wizard>
          <div id="bar" class="bar"></div>
          <PanelView @refresh="refresh"></PanelView>
        </section>
      </el-tab-pane>      
      <el-tab-pane name="sql" label="Sql">
        <section class="sql-wrapper">
          <Sql ref="sql"></Sql>
          <div id="bar" class="bar"></div>
          <PanelView @refresh="refresh"></PanelView>
        </section>
      </el-tab-pane>
      <!-- <el-tab-pane name="xterm" label="Shell">
        <Xterm></Xterm>
      </el-tab-pane> -->
      <el-tab-pane name="detail" v-if="tabName" :label="tabName">
        <Detail></Detail>
      </el-tab-pane>
    </el-tabs>
  </div>
</template>

<script>
  import Detail from "./components/detail.vue";
  import Sql from "./components/sql";
  import Wizard from './components/wizard.vue'
  import PanelView from "./components/panel.vue";
  import { addFavorite, delFavorite } from "@/api/gateway/console";
  import moment from 'moment'
  import { mapState } from "vuex";
  import Xterm from "./components/xterm";
  export default {
    components: { Sql, Wizard, PanelView, Detail, Xterm },
    data() {
      return {
        requestIng: false,
      };
    },
    computed: {
      ...mapState({
        tabName: state => state.console.tabName,
        sqlStr: state => state.console.sqlStr,
        favorites: state => state.console.favorites,
        previewBtn: state => state.console.previewBtn
      }),
      favorited() {
        return this.favorites?(this.favorites.find(item => item.sql == this.sqlStr)?.id || ""):"";
      },
    },
    mounted() {
      this.dragChangeHeight("bar", "sql");
    },
    methods: {
      refresh() {
        this.$refs.SqlView.handleSendSQL();
      },
      dragChangeHeight(drag, panel) {
        let dragEl = document.getElementById(drag);
        let panelEl = document.getElementById(panel);
        dragEl.onmousedown = ev => {
          let disH = panelEl.offsetHeight;
          let disY = ev.clientY;
          document.onmousemove = ev => {
            panelEl.style.height = disH + (ev.clientY - disY) + "px";
            this.height = panelEl.style.height;
          };
          document.onmouseup = () => {
            document.onmousemove = document.onmouseup = null;
          };
          return false;
        };
      },
      getPreviewSql() {
        this.$refs.wizard.getPreviewSql()
      },
       handleSendSQL() {
        if (this.requestIng) return;
        this.requestIng = true;
        if (this.$store.state.console.partActive == 'sql') {
          this.$refs.sql.handleSendSQL();
        } else {
          this.$refs.wizard.handleSendSQL()
        }
        this.$store.commit("console/CHANGE_TREE_KEY");
        this.requestIng = false;
      },
      async toggleFavorite() {

        let obj={
          accountId:this.$store.state.app.token,
          appId:this.$store.state.app.token,
          create_time:moment().format('YYYY-MM-DD HH:mm:ss'),
          created_by:this.$store.state.app.token,
          id:new Date().getTime(),
          sql:this.$store.state.console.partActive =='sql' 
            ? this.sqlStr 
            : this.$refs.wizard.generateSql(),
          update_time:moment().format('YYYY-MM-DD HH:mm:ss'),
          updated_by:this.$store.state.app.token,
          userId:this.$store.state.app.token
        }
        if( localStorage.getItem('favorite_record')){
          let favorites=JSON.parse(localStorage.getItem('favorite_record'))
          favorites.push(obj)
          localStorage.setItem('favorite_record',JSON.stringify(favorites))
        }else{
          localStorage.setItem('favorite_record',JSON.stringify([].concat(obj)))
        }
        // this.favorited
        //   ? await delFavorite(this.favorited)
        //       .then(() => this.$message.success(this.$t("operateSucc")))
        //       .catch(() => {})
        //   : await addFavorite(this.sqlStr)
        //       .then(() => this.$message.success(this.$t("operateSucc")))
        //       .catch(() => {});
        this.$store.commit("console/SET_ACTIVE_TAB", "favorites");
        this.$store.commit("console/SET_FAVORITE",JSON.parse(localStorage.getItem('favorite_record')));
        // this.$store.dispatch("console/getFavorites");
      },
      tabClick({ name }) {
        if (name == "detail") return;
        this.$bus.emit(`console/${name}/focus`);
      },
    },
  };
</script>

<style lang="scss" scoped>
  $bar-color: #f5f5f5;
  $bar-light-color: #dcdfe6;
  .part {
    flex: 1;
    height: 100%;
    overflow-x: hidden;
    position: relative;
  }
  .part::v-deep .el-tabs {
    min-height: 100%;
    display: flex;
    flex-direction: column;
  }
  .sql-btn {
    position: absolute;
    right: 20px;
    z-index: 20;
    top: 8px;
    &::v-deep .el-button--mini {
      padding: 3px 8px;
    }
  }
  .sql-wrapper {
    display: flex;
    flex-direction: column;
    height: 100%;
  }
  .part::v-deep .el-tabs__content {
    flex: 1;
    overflow: auto;
    padding: 15px 0;
  }
  .part::v-deep .el-tabs__header {
    flex-shrink: 0;
  }
  .part::v-deep .el-tab-pane {
    left: 0;
    top: 15px;
    bottom: 15px;
    right: 0;
    position: absolute;
  }
  .icon-shift {
    width: 20px;
    height: 20px;
  }
  .bar {
    width: 100%;
    height: 10px;
    flex-shrink: 0;
    cursor: n-resize;
    background-color: $bar-color;
  }
  .bar:hover {
    background-color: $bar-light-color;
  }
</style>
