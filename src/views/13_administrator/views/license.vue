<template>
  <div class="dnode-block">
    <div class="flexEnd">
      <!-- <el-button
        plain
        @click="dialog = true"
        size="small"
        icon="el-icon-plus"
        >{{ $t("add") }}</el-button
      > -->
      <el-button
        plain
        @click="refresh"
        size="small"
        icon="el-icon-refresh"
        :disabled="loading"
        >{{ $t("refresh") }}</el-button
      >
    </div>
    <el-table style="margin-top: 20px" :data="licenseList" size="mini">
      <el-table-column
        :label="$t('topic.accounts')"
        prop="accounts"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.connections')"
        prop="connections"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.cpu_cores')"
        prop="cpu_cores"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.databases')"
        prop="databases"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.dnodes')"
        prop="dnodes"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.expire_time')"
        prop="expire_time"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.expired')"
        prop="expired"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.querytime')"
        prop="querytime"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.speed')"
        prop="speed"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.storage')"
        prop="storage"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.streams')"
        prop="streams"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.timeseries')"
        prop="timeseries"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.users')"
        prop="users"
      ></el-table-column>
      <el-table-column
        :label="$t('topic.version')"
        prop="version"
      ></el-table-column>

      <!-- <el-table-column label="Action" width="65">
        <template slot-scope="scope">
          <el-button
            plain
            size="small"
            @click="del(scope.row)"
            icon="el-icon-delete"
          ></el-button>
        </template>
      </el-table-column> -->
    </el-table>
    <el-pagination
      class="pagination"
      layout="total, prev, pager, next"
      :current-page.sync="currentPage"
      :page-size="pageSize"
      :hide-on-single-page="true"
      :total="total"
      @current-change="handlePageChange"
    ></el-pagination>
    <el-dialog
      align="center"
      :title="$t('topic.addsource')"
      width="600px"
      :visible.sync="dialog"
    >
      <el-form
        :model="ruleForm"
        :rules="rules"
        ref="ruleForm"
        size="mini"
        label-width="auto"
        class="demo-ruleForm"
      >
        <el-form-item label="UDFName" prop="name" required>
          <el-input v-model.trim="ruleForm.name"></el-input>
        </el-form-item>
        <el-form-item label="Language" prop="language" required>
          <el-select
            v-model="ruleForm.language"
            placeholder="Please Select Language"
          >
            <el-option label="Nodejs" value="nodejs"></el-option>
            <el-option label="Java" value="java"></el-option>
            <el-option label="Rust" value="rust"></el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="Content" prop="content" required>
          <el-input v-model.trim="ruleForm.content"></el-input>
        </el-form-item>
      </el-form>

      <el-row style="margin-top: 20px">
        <el-col :span="5" :offset="6">
          <el-button size="small" @click="dialog = false" class="w100">{{
            $t("cancel")
          }}</el-button>
        </el-col>
        <el-col :span="5" :push="4">
          <el-button
            size="small"
            :disabled="confirmStatus"
            @click="addUdf"
            class="w100"
            type="primary"
            >{{ $t("confirm") }}</el-button
          >
        </el-col>
      </el-row>
    </el-dialog>
  </div>
</template>
<script>
import { sendSQLReq } from "@/api/gateway/console";
export default {
  data() {
    return {
      pageSize: 10,
      currentPage: 1,
      total: 10,
      dialog: false,
      loading: false,
      ruleForm: {
        name: "",
        language: "",
        content: "",
      },
      rules: {
        name: [
          {
            message: "Please enter the name",
            trigger: "blur",
          },
        ],
        language: [
          {
            message: "Please select the language",
            trigger: "change",
          },
        ],
        content: [
          {
            message: "Please enter the content",
            trigger: "blur",
          },
        ],
      },
      licenseList: [],
    };
  },
  computed: {
    confirmStatus() {
      if (!this.ruleForm.name) {
        return true;
      }
      if (!this.ruleForm.language) {
        return true;
      }
      if (!this.ruleForm.content) {
        return true;
      }
      return false;
    },
  },
  created() {
    this.getData();
  },
  methods: {
    handlePageChange() {},
    del(data) {
      this.$confirm("Are you sure  to delete " + data.name + "?", "Warning", {
        confirmButtonText: "Ok",
        cancelButtonText: "Cancle",
        type: "warning",
      });
    },
    refresh() {
      this.loading = true;
      this.getData();
    },
    addUdf() {},
    async getData() {
      try {
        await sendSQLReq(`show grants;`).then((res) => {
          this.licenseList = res.data.map((data) => {
            return Object.fromEntries(
              res.column_meta.map((item, index) => {
                return [item[0], data[index]];
              })
            );
          });
        });
        this.loading = false;
      } catch (error) {
        this.loading = false;
        console.log();
      }
    },
  },
};
</script>
<style lang="scss" scoped>
:v-deep {
  .el-form-item__content {
    display: flex;
  }
  .el-select.el-select--mini {
    flex: 1;
  }
}
</style>