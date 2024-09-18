<template>
  <div>
    <div class="flexEnd">
      <el-button plain type="primary" @click="refresh" size="small" icon="el-icon-refresh" :disabled="requestIng" style="font-size:14px;">{{
        $t("refresh")
      }}</el-button>
      <el-button class="big-button" plain type="primary" @click="dialog = true" size="small" icon="el-icon-plus">{{ $t("topic.createTopic") }}</el-button>
    </div>
    <el-table style="margin-top: 20px" :data="topicList" size="mini" row-key="topic_name">
      <el-table-column width="150" :label="$t('topic.topicName')" prop="topic_name" show-overflow-tooltip></el-table-column>
      <el-table-column width="150" :label="$t('topic.DBName')" prop="db_name" show-overflow-tooltip></el-table-column>
      <el-table-column min-width="200" label="SQL" prop="sql">
        <template slot-scope="scope">
          <el-tooltip
            placement="left-start"
            :content="scope.row.sql"
            popper-class="my-popper"
            :open-delay="1000"
          >
          <span>
            <pre v-highlight class="nowrap sql-code pre-code" slot="reference">
            <code class="language-sql" style="overflow:hidden">{{ scope.row.sql }} </code>
          </pre>

          </span>
        </el-tooltip>
        </template>
      </el-table-column>
      <el-table-column width="90" :label="$t('getDsn')" prop="dsn">
        <template slot-scope="scope">
          <el-tooltip :content="scope.row.dsn" placement="top-start">
            <!-- <copy-text :text="scope.row.dsn" isShowBtnText></copy-text> -->
            <el-button class="copy-btn" size="mini" @click="copyDsn(scope.row.dsn)">
              <el-icon class="el-icon-copy-document"></el-icon>
              {{ $t('copy') }}
            </el-button>
          </el-tooltip>
        </template>
      </el-table-column>
      <el-table-column width="210" :label="$t('createTime')" prop="create_time" show-overflow-tooltip>
        <span slot-scope="scope">{{ parsinginZone(scope.row.create_time) }}</span>
      </el-table-column>
     <!-- 云服务有自己的用户管理，企业版没有，从 schema 中无法获取 topic 的创建用户 -->
      <el-table-column :label="$t('topic.action')" width="140">
        <template slot-scope="scope">
          <el-tooltip
            effect="light"
            :content="$t('topic.sampleCode')"
            placement="top"
          >
            <el-button
              class="mini-btn"
              size="mini"
              @click="document(scope.row)"
              icon="el-icon-copy-document"
            ></el-button>
          </el-tooltip>
          <el-tooltip
            effect="light"
            :content="$t('topic.shareTopic')"
            placement="top"
          >
            <el-button
              class="mini-btn"
              size="mini"
              @click="manage(scope.row)"
              icon="el-icon-share"
            ></el-button>
          </el-tooltip>
          <el-tooltip
            effect="light"
            :content="$t('delete')"
            placement="top"
          >
          <el-button  plain size="small" @click="del(scope.row)" icon="el-icon-delete"></el-button>
          </el-tooltip>
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
  import { getDSN } from "@/utils/index";
  import { SubscriptionDocsUrl } from "@/const";
  import { parsinginZone, copy } from '@/utils'
  import CopyText from '@/components/CopyText.vue';
  
  export default {
    components: {
      AddTopic: () => import("../components/addTopic.vue"),
      ManageTopic: () => import("../components/manageTopic.vue"),
      CopyText
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
        return this.$t("topic.learnMoreTip").replace(/docsUrl/, `${this.$t('urlPart')}/advanced/subscription/`);
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
        this.topicList.forEach((item) => {
          console.log(item);
          item.dsn = getDSN("tmq") + "/" + item.topic_name;
        });
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
              this.$error(res?.desc);
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
      manage(data) {
      this.$router.push({
        path: '/topic/share',
        query: {
          topicId: data.topicId
        }
      });
    },
    document(data) {
      this.$router.push({
        path: '/topic/example',
        query: {
          topicId: data.topicId
        }
      });
    },
    copyDsn(dsn) {
      copy(dsn);
    },
    },
  };
</script>

<style lang="scss" scoped>
  .sql-code {
    position: relative;
    text-align: left;
    padding: 3px 0;
    font-size: 16px;
  }
  .language-sql {
    white-space: inherit !important;
  }
  .copy-btn {
    cursor: pointer;
  }
</style>

<style lang="scss"> 
   .my-popper {
    max-width: 600px;
    max-height: 600px;
    overflow-y: auto;
    overflow-x: hidden;
  }
</style>
