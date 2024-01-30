<template>
  <div>
    <div class="flexEnd">
      <el-button plain @click="refresh" size="small" icon="el-icon-refresh" :disabled="requestIng" style="font-size:14px;">{{
        $t("refresh")
      }}</el-button>
      <el-button class="big-button" plain @click="dialog = true" size="small" icon="el-icon-plus">{{ $t("topic.createTopic") }}</el-button>
    </div>
    <el-table style="margin-top: 20px" :data="topicList" size="mini" row-key="topic_name">
      <el-table-column width="150" :label="$t('topic.topicName')" prop="topic_name"></el-table-column>
      <el-table-column width="150" :label="$t('topic.DBName')" prop="db_name"></el-table-column>
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
      <el-table-column min-width="200" label="DSN" prop="dsn" show-overflow-tooltip>
        <template slot-scope="scope">
        <copy-text :text="scope.row.dsn" isShowBtnText></copy-text>
        </template>
      </el-table-column>
      <el-table-column width="210" :label="$t('createTime')" prop="create_time">
        <span slot-scope="scope">{{ parsinginZone(scope.row.create_time) }}</span>
      </el-table-column>
     <!-- 云服务有自己的用户管理，企业版没有，从 schema 中无法获取 topic 的创建用户 -->
      <el-table-column :label="$t('topic.action')" width="140">
        <template slot-scope="scope">
          <el-tooltip
            effect="light"
            :content="$t('sampleCode')"
            placement="top"
          >
            <el-button
              class="mini-btn"
              size="mini"
              @click="document(scope.row)"
              icon="el-icon-document"
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
  import { parsinginZone } from '@/utils'
  import CopyText from '@/components/CopyText.vue'
  
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
        return this.$t("topic.learnMoreTip").replace(/docsUrl/, `${this.$t('urlPart')}/taos-sql/tmq/#create-a-topic`);
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
        this.topicList.push({
          topic_name: 'test',
          sql: 'create topic cutleafseg_qsbproc_proc_topic as select\n    ts as ts,\n    czjyc_zscj_zs13_ys117cut1_0tempvalue as czjyc_zscj_zs13_ys117cut1_0tempvalue,\n    czjyc_zscj_zs13_ys117cut1_0xaccespeedrmsavg as czjyc_zscj_zs13_ys117cut1_0xaccespeedrmsavg,\n    czjyc_zscj_zs13_ys117cut1_0xspeedrmsavg as czjyc_zscj_zs13_ys117cut1_0xspeedrmsavg,\n    czjyc_zscj_zs13_ys117cut1_0yaccespeedrmsavg as czjyc_zscj_zs13_ys117cut1_0yaccespeedrmsavg,\n    czjyc_zscj_zs13_ys117cut1_0yspeedrmsavg as czjyc_zscj_zs13_ys117cut1_0yspeedrmsavg,\n    czjyc_zscj_zs13_ys117cut1_0zaccespeedrmsavg as czjyc_zscj_zs13_ys117cut1_0zaccespeedrmsavg,\n    czjyc_zscj_zs13_ys117cut1_0zspeedrmsavg as czjyc_zscj_zs13_ys117cut1_0zspeedrmsavg,\n    czjyc_zscj_zs13_ys117cut1_1tempvalue as czjyc_zscj_zs13_ys117cut1_1tempvalue,\n    czjyc_zscj_zs13_ys117cut1_1xaccespeedrmsavg as czjyc_zscj_zs13_ys117cut1_1xaccespeedrmsavg,\n    czjyc_zscj_zs13_ys117cut1_1xspeedrmsavg as czjyc_zscj_zs13_ys117cut1_1xspeedrmsavg,\n    czjyc_zscj_zs13_ys117cut1_1yaccespeedrmsavg as czjyc_zscj_zs13_ys117cut1_1yaccespeedrmsavg,\n    czjyc_zscj_zs13_ys117cut1_1yspeedrmsavg as czjyc_zscj_zs13_ys117cut1_1yspeedrmsavg,\n    czjyc_zscj_zs13_ys117cut1_1zaccespeedrmsavg as czjyc_zscj_zs13_ys117cut1_1zaccespeedrmsavg,\n    czjyc_zscj_zs13_ys117cut1_1zspeedrmsavg as czjyc_zscj_zs13_ys117cut1_1zspeedrmsavg,\n    czjyc_zscj_zs13_ys117cut1_2tempvalue as czjyc_zscj_zs13_ys117cut1_2tempvalue,\n    czjyc_zscj_zs13_ys117cut1_2xaccespeedrmsavg as czjyc_zscj_zs13_ys117cut1_2xaccespeedrmsavg,\n    czjyc_zscj_zs13_ys117cut1_2xspeedrmsavg as czjyc_zscj_zs13_ys117cut1_2xspeedrmsavg,\n    czjyc_zscj_zs13_ys117cut1_2yaccespeedrmsavg as czjyc_zscj_zs13_ys117cut1_2yaccespeedrmsavg,\n    czjyc_zscj_zs13_ys117cut1_2yspeedrmsavg as czjyc_zscj_zs13_ys117cut1_2yspeedrmsavg,\n    czjyc_zscj_zs13_ys117cut1_2zaccespeedrmsavg as czjyc_zscj_zs13_ys117cut1_2zaccespeedrmsavg,\n    czjyc_zscj_zs13_ys117cut1_2zspeedrmsavg as czjyc_zscj_zs13_ys117cut1_2zspeedrmsavg,\n    czjyc_zscj_zs13_ys117cut1_bladesurplus as czjyc_zscj_zs13_ys117cut1_bladesurplus,\n    czjyc_zscj_zs13_ys117cut1_bottomchaincurrent as czjyc_zscj_zs13_ys117cut1_bottomchaincurrent,\n    czjyc_zscj_zs13_ys117cut1_bottomchainspeed as czjyc_zscj_zs13_ys117cut1_bottomchainspeed,\n    czjyc_zscj_zs13_ys117cut1_electriccurrent as czjyc_zscj_zs13_ys117cut1_electriccurrent,\n    czjyc_zscj_zs13_ys117cut1_flux as czjyc_zscj_zs13_ys117cut1_flux,\n    czjyc_zscj_zs13_ys117cut1_grindingwheelelectriccurrent as czjyc_zscj_zs13_ys117cut1_grindingwheelelectriccurrent,\n    czjyc_zscj_zs13_ys117cut1_grindingwheelelectricspeed as czjyc_zscj_zs13_ys117cut1_grindingwheelelectricspeed,\n    czjyc_zscj_zs13_ys117cut1_grindingwheelsurplus as czjyc_zscj_zs13_ys117cut1_grindingwheelsurplus,\n    czjyc_zscj_zs13_ys117cut1_height as czjyc_zscj_zs13_ys117cut1_height,\n    czjyc_zscj_zs13_ys117cut1_knifepressure as czjyc_zscj_zs13_ys117cut1_knifepressure,\n    czjyc_zscj_zs13_ys117cut1_repeateelectriccurrent as czjyc_zscj_zs13_ys117cut1_repeateelectriccurrent,\n    czjyc_zscj_zs13_ys117cut1_repeateelectricspeed as czjyc_zscj_zs13_ys117cut1_repeateelectricspeed,\n    czjyc_zscj_zs13_ys117cut1_speed as czjyc_zscj_zs13_ys117cut1_speed,\n    czjyc_zscj_zs13_ys117cut1_upchaincurrent as czjyc_zscj_zs13_ys117cut1_upchaincurrent,\n    czjyc_zscj_zs13_ys117cut1_upchainspeed as czjyc_zscj_zs13_ys117cut1_upchainspeed,\n    czjyc_zscj_zs13_ys117cut1_width as czjyc_zscj_zs13_ys117cut1_width,\n    czjyc_zscj_zs13_ys117cut1_widthtrim as czjyc_zscj_zs13_ys117cut1_widthtrim,\n    czjyc_zscj_zs13_ys117cut2_bladesurplus as czjyc_zscj_zs13_ys117cut2_bladesurplus,\n    czjyc_zscj_zs13_ys117cut2_bottomchaincurrent as czjyc_zscj_zs13_ys117cut2_bottomchaincurrent,\n    czjyc_zscj_zs13_ys117cut2_bottomchainspeed as czjyc_zscj_zs13_ys117cut2_bottomchainspeed,\n    czjyc_zscj_zs13_ys117cut2_electriccurrent as czjyc_zscj_zs13_ys117cut2_electriccurrent,\n    czjyc_zscj_zs13_ys117cut2_flux as czjyc_zscj_zs13_ys117cut2_flux,\n    czjyc_zscj_zs13_ys117cut2_grindingwheelelectriccurrent as czjyc_zscj_zs13_ys117cut2_grindingwheelelectriccurrent,\n    czjyc_zscj_zs13_ys117cut2_grindingwheelelectricspeed as czjyc_zscj_zs13_ys117cut2_grindingwheelelectricspeed,\n    czjyc_zscj_zs13_ys117cut2_grindingwheelsurplus as czjyc_zscj_zs13_ys117cut2_grindingwheelsurplus,\n    czjyc_zscj_zs13_ys117cut2_height as czjyc_zscj_zs13_ys117cut2_height,\n    czjyc_zscj_zs13_ys117cut2_knifepressure as czjyc_zscj_zs13_ys117cut2_knifepressure,\n    czjyc_zscj_zs13_ys117cut2_repeateelectriccurrent as czjyc_zscj_zs13_ys117cut2_repeateelectriccurrent,\n    czjyc_zscj_zs13_ys117cut2_repeateelectricspeed as czjyc_zscj_zs13_ys117cut2_repeateelectricspeed,\n    czjyc_zscj_zs13_ys117cut2_speed as czjyc_zscj_zs13_ys117cut2_speed,\n    czjyc_zscj_zs13_ys117cut2_upchaincurrent as czjyc_zscj_zs13_ys117cut2_upchaincurrent,\n    czjyc_zscj_zs13_ys117cut2_upchainspeed as czjyc_zscj_zs13_ys117cut2_upchainspeed,\n    czjyc_zscj_zs13_ys117cut2_width as czjyc_zscj_zs13_ys117cut2_width,\n    czjyc_zscj_zs13_ys117cut2_widthtrim as czjyc_zscj_zs13_ys117cut2_widthtrim,\n    czjyc_zscj_zs13_ys111_flux as czjyc_zscj_zs13_ys111_flux,\n    czjyc_zscj_zs13_ys111_spflow as czjyc_zscj_zs13_ys111_spflow,\n    czjyc_zscj_zs13_ys111_total as czjyc_zscj_zs13_ys111_total,\n    czjyc_zscj_zs13_ys112_moisture as czjyc_zscj_zs13_ys112_moisture,\n    czjyc_zscj_zs13_ys112_moisturetemp as czjyc_zscj_zs13_ys112_moisturetemp,\n    czjyc_zscj_zs13_ys112_moisturezero as czjyc_zscj_zs13_ys112_moisturezero,\n    czjyc_zscj_zs13_unit4_batch as czjyc_zscj_zs13_unit4_batch,\n\tbrand as brand,\n    czjyc_zscj_zs13_unit4_brandname as czjyc_zscj_zs13_unit4_brandname,\n    czjyc_zscj_zs13_unit4_grade as czjyc_zscj_zs13_unit4_grade,\n    czjyc_zscj_zs13_unit4_group as czjyc_zscj_zs13_unit4_group,\n    czjyc_zscj_zs13_unit4_infraredresistancetemp as czjyc_zscj_zs13_unit4_infraredresistancetemp,\n    czjyc_zscj_zs13_unit4_matid as czjyc_zscj_zs13_unit4_matid,\n    czjyc_zscj_zs13_unit4_productioncapacity as czjyc_zscj_zs13_unit4_productioncapacity,\n    czjyc_zscj_zs13_unit4_shredbeltspeedb as czjyc_zscj_zs13_unit4_shredbeltspeedb,\n    czjyc_zscj_zs13_unit4_shredrateb as czjyc_zscj_zs13_unit4_shredrateb from cy_cuttobac.cutleafseg_qsbproc_proc'
        })
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
    }
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
</style>

<style lang="scss"> 
   .my-popper {
    max-width: 600px;
    max-height: 600px;
    overflow-y: auto;
    overflow-x: hidden;
  }
</style>
