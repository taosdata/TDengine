<template>
  <div class="page-wrapper">
    <MainContentHeader :title="$t('stream.pageTitle')"></MainContentHeader>
    <section class="content">
      <div>
        <div class="flexEnd">
          <el-button class="big-button" @click="dialog = true" v-permission plain size="small" icon="el-icon-plus">{{
            $t("stream.createStream")
          }}</el-button>
        </div>
        <el-table style="margin-top: 20px" size="mini" :data="streamList">
          <el-table-column :label="$t('stream.streamName')" width="200" prop="stream_name"></el-table-column>
          <el-table-column :label="$t('createTime')" width="200" prop="create_time">
            <span slot-scope="scope">{{ parsinginZone(scope.row.create_time) }}</span>
          </el-table-column>
          <el-table-column label="sql" min-width="200" prop="sql">
            <template slot-scope="scope">
              <pre v-highlight class="nowrap sql-code pre-code" slot="reference">
          <code class="language-sql" style="overflow:hidden">{{ scope.row.sql }} </code>
        </pre>
            </template>
          </el-table-column>
          <el-table-column width="100" :label="$t('status')" prop="status"></el-table-column>
          <el-table-column width="120" :label="$t('stream.sourceDB')" prop="source_db"></el-table-column>
          <el-table-column width="120" :label="$t('stream.targetDB')" prop="target_db"></el-table-column>
          <el-table-column width="120" :label="$t('stream.targetTable')" prop="target_table"></el-table-column>
          <el-table-column width="100" :label="watermarkdetail" prop="watermark"></el-table-column>
          <el-table-column width="100" :label="$t('stream.trigger')" prop="trigger"></el-table-column>

          <el-table-column :label="$t('operate')" width="80">
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
      </div>
    </section>
    <!-- <el-dialog align="center" :title="$t('stream.createStream')" width="800px" :visible.sync="dialog">
      <el-input
        size="small"
        @input="errorText = ''"
        :autofocus="true"
        placeholder="[IF NOT EXISTS] stream_name [stream_options] INTO stb_name AS subquery"
        v-model="sql"
      >
        <template slot="prepend">{{ sqlPrefix }}</template>
        <template slot="append">
          <el-tooltip class="item" :content="sqlTip" effect="light">
            <pre v-highlight class="pre-show" slot="content">
              <code>{{sqlTip}}</code>
            </pre>
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
          <el-button size="small" @click="createStream" class="w100" type="primary">{{ $t("confirm") }}</el-button>
        </el-col>
      </el-row>
    </el-dialog> -->
    <el-dialog :close-on-click-modal="false" align="center" :title="$t('stream.createStream')" width="800px" :visible.sync="dialog"  @close='closeDialog'
      :destroy-on-close='true'>
      <AddForm type="stream" @close="close" :stream-list="streamList" ref="stream"/>
    </el-dialog>
  </div>
</template>

<script>
  import AddForm from "./components/addStream.vue";
  import { getStreams, createStream, delStream } from "@/api/stream";
  import { StreamDocsUrl } from "@/const";
  import { parsinginZone } from '@/utils';
  export default {
    components: {
      AddForm,
    },
    provide(){
      return {
        parentName:this.name
      }
    },
    data() {
      return {
        watermarkdetail:this.$t("stream.watermark")+"（ms）",
        name:'Stream',
        isOEM:
        process.env.VUE_APP_CUS_NAME &&
        process.env.VUE_APP_CUS_NAME !== "TDengine",
        dialog: false,
        sql: "",
        sqlPrefix: "CREATE STREAM ",
        streamList: [],
        requestIng: false,
        errorText: "",
        currentPage: 1,
        pageSize: 10,
        total: 0,
        sqlTip: `CREATE STREAM [IF NOT EXISTS] stream_name [stream_options] INTO stb_name AS subquery`,
        parsinginZone
      };
    },
    computed: {
      learnMoreTip() {
        return this.$t("stream.learnMoreTip").replace(/docsUrl/, StreamDocsUrl);
      },
    },
    async created() {
      this.getStreams();
    },
    methods: {
      closeDialog(){
        this.dialog=false
      },
      async getStreams() {
        if (this.requestIng) return;
        this.requestIng = true;
        [this.streamList, this.total] = await getStreams({ currentPage: this.currentPage, pageSize: this.pageSize });
        this.requestIng = false;
      },
      createStream() {
        this.errorText = "";
        if (!this.sql) return (this.errorText = this.$t("sqlError"));
        if (this.requestIng) return;
        createStream(this.sqlPrefix + this.sql)
          .then(() => {
            this.$message.success(this.$t("addSucc"));
            this.currentPage = 1;
            this.getStreams();
            this.dialog = false;
          })
          .catch(err => (this.errorText = err.desc))
          .finally(() => {
            this.requestIng = false;
          });
      },
      del(data) {
        if (this.requestIng) return;
        this.$confirm(this.$t("stream.delStream") + "：" + data.stream_name + "?", this.$t("tips"), {
          confirmButtonText: this.$t("confirm"),
          cancelButtonText: this.$t("cancel"),
          type: "warning",
        }).then(async () => {
          this.requestIng = true;
          await delStream(`\`${data.stream_name}\``)
            .then(() => {
              this.$message.success(this.$t("delSucc"));
            })
            .finally(() => {
              this.requestIng = false;
              this.currentPage = 1;
              this.getStreams();
            })
            .catch(res => {
              this.$message.error(res?.desc)
            })
        });
      },
      handlePageChange() {
        this.getStreams();
      },
      close(){
        this.getStreams();
        this.dialog = false;
      }
    },
  };
</script>

<style lang='scss'>
  .sql-code {
    position: relative;
    text-align: left;
    padding: 3px 0;
    font-size: 16px;

  }
  :deep(.CodeMirror) {
    height: 100px;
    .CodeMirror-placeholder {
      color: #c0c4cc;
    }
  }
  
  
</style>
