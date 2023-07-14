<template>
  <div>
    <div class="flexEnd">
      <el-button plain @click="refresh" size="small" icon="el-icon-refresh" :disabled="requestIng" style="font-size:14px;">{{
        $t("refresh")
      }}</el-button>
      <el-button class="big-button" plain @click="dialog = true" size="small" icon="el-icon-plus">{{ $t("topic.createTopic") }}</el-button>
    </div>
    <el-table style="margin-top: 20px" :data="topicList" size="mini">
      <el-table-column width="150" :label="$t('topic.topicName')" prop="topic_name"></el-table-column>
      <el-table-column width="150" :label="$t('topic.DBName')" prop="db_name"></el-table-column>
      <el-table-column width="210" :label="$t('createTime')" prop="create_time">
        <span slot-scope="scope">{{ parsinginZone(scope.row.create_time) }}</span>
      </el-table-column>
      <el-table-column min-width="200" label="SQL" prop="sql">
        <template slot-scope="scope">
          <pre v-highlight class="nowrap sql-code pre-code" slot="reference">
          <code class="language-sql" style="overflow:hidden">{{ scope.row.sql }} </code>
        </pre>
        </template>
      </el-table-column>
      <el-table-column :label="$t('topic.action')" width="80">
        <template slot-scope="scope">
          <el-button  plain size="small" @click="del(scope.row)" icon="el-icon-delete"></el-button>
        </template>
      </el-table-column>
    </el-table>
    <el-pagination
      class="pagination"
      layout="total, prev, pager, next"
      :current-page.sync="currentPage"
      :page-size="pageSize"
      :hide-on-single-page="true"
      :total="total"
      @current-change="handlePageChange"
    >
    </el-pagination>
    <p class="default-tip" v-html="learnMoreTip" v-if="!isOEM"></p>
     <el-dialog align="center" :close-on-click-modal="false" :title="title" :width="width" :visible.sync="dialog" @close='closeDialog'
      :destroy-on-close='true'>
      <component :is="dialogComp" v-bind="dialogParams" @close="close"></component>
    </el-dialog>
    <!-- <el-dialog align="center" :title="$t('topic.createTopic')" width="800px" :visible.sync="dialog">
      <el-input size="small" @input="errorText = ''" :placeholder="sqlTip" v-model="sql">
        <template slot="prepend">{{ sqlPrefix }}</template>
        <template slot="append">
          <el-tooltip class="item" effect="light">
            <div v-html="$t('topic.topicTip')" slot="content"></div>
            <i class="el-icon-info"></i>
          </el-tooltip>
        </template>
      </el-input>
      <p class="errorText">{{ errorText }}</p>
      <el-row style="margin-top: 20px">
        <el-col :span="11">
          <el-button size="small" @click="dialog = false" class="w100">{{ $t("cancel") }}</el-button>
        </el-col>
        <el-col :span="11" :offset="1">
          <el-button size="small" :disabled="requestIng || !sql" @click="handleCreateTopic" class="w100" type="primary">{{
            $t("confirm")
          }}</el-button>
        </el-col>
      </el-row>
    </el-dialog> -->
  </div>
</template>

<script>
  import { createTopic, getTopics, delTopic } from "@/api/topic";
  import { SubscriptionDocsUrl } from "@/const";
  import { parsinginZone } from '@/utils'
  
  export default {
    components: {
      AddTopic: () => import("../components/addTopic.vue"),
      ManageTopic: () => import("../components/manageTopic.vue"),
    },
    data() {
      return {
        isOEM:
        process.env.VUE_APP_CUS_NAME &&
        process.env.VUE_APP_CUS_NAME !== "TDengine",
        requestIng: false,
        dialog: false,
        sql: "",
        sqlPrefix: "CREATE TOPIC ",
        errorText: "",
        topicList: [],
        currentPage: 1,
        pageSize: 10,
        total: 0,
        dialogType: "0",
        sqlTip: "[IF NOT EXISTS] topic_name AS {subquery | DATABASE db_name | STABLE stb_name }",
        dialogParams: {},
        parsinginZone
      };
    },
    computed: {
      learnMoreTip() {
        return this.$t("topic.learnMoreTip").replace(/docsUrl/, SubscriptionDocsUrl);
      },
      dialogComp() {
        return {
          0: "AddTopic",
          1: "ManageTopic",
        }[this.dialogType];
      },
       title() {
        return {
          0: this.$t("topic.createTopic"),
          1: this.$t("topic.manageTopic"),
        }[this.dialogType];
      },
      width() {
        return {
          0: "750px",
          1: "380px",
        }[this.dialogType];
      },
      userId() {
        return this.$root.currentInfo.user.id;
      },
      addBtnShow() {
        return this.$store.getters.currentServerLevel || (!this.$store.getters.currentServerLevel && !this.topicList.length);
      }
    },
    async created() {
      this.getTopics();
      // for (let i = 0; i < 100; i++) {
      //   await createTopic(`CREATE TOPIC topic${i} as SELECT count(*)FROM testa.meters`);
      // }
    },
    methods: {
      closeDialog(){
        this.dialog=false
      },
      refresh(){
        this.getTopics();
      },
      async getTopics() {
        if (this.requestIng) return;
        this.requestIng = true;
        [this.topicList, this.total] = await getTopics({ currentPage: this.currentPage, pageSize: this.pageSize });
        this.requestIng = false;
      },
      handleCreateTopic() {
        this.errorText = "";
        if (!this.sql) return (this.errorText = this.$t("sqlError"));
        if (this.requestIng) return;
        createTopic(this.sqlPrefix + this.sql)
          .then(() => {
            this.$message.success(this.$t("addSucc"));
            this.currentPage = 1;
            this.getTopics();
            this.dialog = false;
          })
          .catch(err => (this.errorText = err?.desc))
          .finally(() => {
            this.requestIng = false;
          });
      },
      del(data) {
        if (this.requestIng) return;
        this.$confirm(this.$t("topic.delTopic") + "：" + data.topic_name + "?", this.$t("tips"), {
          confirmButtonText: this.$t("confirm"),
          cancelButtonText: this.$t("cancel"),
          type: "warning",
        }).then(async () => {
          this.requestIng = true;
          await delTopic(data.topic_name)
            .then(() => {
              this.$message.success(this.$t("delSucc"));
            })
            .finally(() => {
              this.requestIng = false;
              this.currentPage = 1;
              this.getTopics();
            })
            .catch((res) => {
              this.$message.error(res?.desc);
            })
        });
      },
      handlePageChange() {
        this.getTopics();
      },
      add() {
        this.dialogType = "0";
        this.dialogParams = { topicList: this.topicList };
        this.dialog = true;
      },
      close() {
        this.dialog = false;
        this.getTopics();
      },
    },
  };
</script>

<style lang="scss">
  .sql-code {
    position: relative;
    text-align: left;
    padding: 3px 0;
    font-size: 16px;
  }
</style>
