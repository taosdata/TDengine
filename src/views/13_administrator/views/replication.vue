<template>
  <div class="dnode-block">
    <div class="flexEnd">
      <el-button plain @click="refresh" size="small" icon="el-icon-refresh">
        {{
        $t("refresh")
        }}
      </el-button>
      <el-button plain @click="add" size="small" icon="el-icon-plus">Add New Replication</el-button>
    </div>
    <el-table style="margin-top: 20px" :data="topicList" size="mini">
      <el-table-column label="ID" width="80" prop="id"></el-table-column>
      <el-table-column label="From Database" prop="fromdb"></el-table-column>
      <el-table-column label="To Instance" prop="hostport"></el-table-column>
      <el-table-column label="To Database" prop="db"></el-table-column>

      <el-table-column label="Status" prop="status"></el-table-column>
      <el-table-column label="Reason" prop="reason"></el-table-column>
      <el-table-column label="Finished At" prop="finished_at"></el-table-column>
      <el-table-column label="Create At" prop="created_at"></el-table-column>
      <el-table-column label="Operation" width="110">
        <template slot-scope="scope">
          <el-switch
            :value="scope.row.status.toLowerCase() == 'running'"
            active-color="rgb(66, 89, 206)"
            inactive-color="#dcdfe6"
            @change="switchOperation($event, scope.row)"
          ></el-switch>
          <!-- <el-button
            plain
            size="small"
            @click="edit(scope.row)"
            icon="el-icon-edit"
          ></el-button>
          <el-button
            plain
            size="small"
            @click="start(scope.row, scope.$index)"
            icon="el-icon-qidong"
          ></el-button>
          <el-button
            plain
            size="small"
            @click="stop(scope.row, scope.$index)"
            icon="el-icon-tingzhi"
          ></el-button>-->
          <el-button plain size="small" @click="del(scope.row, scope.$index)" icon="el-icon-delete"></el-button>
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
    ></el-pagination>
    <el-dialog align="center" title="Add New Replication" width="600px" :visible.sync="dialog">
      <el-form
        :model="ruleForm"
        :rules="rules"
        ref="ruleForm"
        size="mini"
        label-width="auto"
        class="demo-ruleForm"
      >
        <el-form-item label="From Source" prop="source" required>
          <!-- <el-input v-model.trim="ruleForm.source"></el-input> -->
          <el-select v-model="ruleForm.source" placeholder="Please select">
            <el-option v-for="db in dblist" :key="db['node-key']" :label="db.name" :value="db.name"></el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="Target DSN" prop="target" required>
          <el-input v-model.trim="ruleForm.target" placeholder="taos://192.168.0.1:6030/db2"></el-input>
        </el-form-item>
      </el-form>

      <el-row style="margin-top: 20px">
        <el-col :span="5" :offset="6">
          <el-button size="small" @click="dialog = false" class="w100">
            {{
            $t("cancel")
            }}
          </el-button>
        </el-col>
        <el-col :span="5" :push="4">
          <el-button
            size="small"
            :disabled="confirmStatus"
            @click="addReplication"
            class="w100"
            type="primary"
          >{{ $t("confirm") }}</el-button>
        </el-col>
      </el-row>
    </el-dialog>
  </div>
</template>
<script>
import { format } from "date-fns";
import { Message } from "element-ui";
import { getDBListReq } from "@/api/gateway/data/dbs.js";
import taosbenchmarkVue from "@/utils/config/mdx/en/taosbenchmark.vue";
export default {
  data() {
    return {
      pageSize: 10,
      currentPage: 1,
      total: 10,
      dialog: false,
      dblist: [],
      ruleForm: {
        source: "",
        target: ""
      },
      rules: {
        source: [
          {
            message: "Please select the source",
            trigger: "change"
          }
        ],
        target: [
          {
            message: "Please enter the target dsn",
            trigger: "blur"
          }
        ]
      },
      topicList: []
    };
  },
  computed: {
    confirmStatus() {
      if (!this.ruleForm.source) {
        return true;
      }
      if (!this.ruleForm.target) {
        return true;
      }
      return false;
    }
  },
  methods: {
    handlePageChange() {},
    add() {
      this.dialog = true;
      this.ruleForm.source = "";
      this.ruleForm.target = "";
    },
    del(data) {
      this.$confirm("Are you sure  to delete " + data.source + "?", "Warning", {
        confirmButtonText: "Ok",
        cancelButtonText: "Cancle",
        type: "warning"
      }).then(() => {
        fetch(`http://192.168.0.201:6050/tasks/${data.id}`, {
          method: "delete",
        }).then((res) => {
          if (res.status == 200) {
            Message({
              type: "success",
              message: "Deleted Successfully",
            });
            this.getReplication();
          }
        });
      });
    },
    refresh(data) {
      this.getReplication()
    },
    async addReplication() {
      try {
        await fetch("http://192.168.0.201:6050/tasks", {
          method: "post",
          headers: {
            "Content-Type": "application/json"
          },
          body: JSON.stringify({
            name: "replication",
            labels: [
              "type::replication",
              `cluster-id::${localStorage.getItem("local_clusterID")}`
            ],
            to: `local:${this.ruleForm.target}`,
            from: `tmq+${localStorage.getItem("base_url")}/${
              this.ruleForm.source
            }`
          })
        }).then(res => {
          console.log(res, "replication----addd");

          if (res.ok || res.status == 201) {
            Message.success("Created Successfully!");
            this.getReplication();
            this.dialog = false;
          }
        });
      } catch (err) {
        err.desc && Message.error(err.desc);
        return Promise.reject(err);
      }
    },
    edit(data) {
      this.dialog = taosbenchmarkVue;
      this.ruleForm.source = data.source;
      this.ruleForm.target = data.target;
    },
    async start(val, data) {
      try {
        await fetch(`http://192.168.0.201:6050/tasks/${data.id}/start`, {
          method: "post"
        }).then(res => {
          if (res.status == 200) {
            Message.success("Operation Successfully Completed!");
            this.getReplication();
          }
        });
      } catch (err) {
        err.desc && Message.error(err.desc);
        return Promise.reject(err);
      }
    },
    async stop(val, data) {
      try {
        await fetch(`http://192.168.0.201:6050/tasks/${data.id}/stop`, {
          method: "post"
        }).then(res => {
          if (res.status == 200) {
            Message.success("Operation Successfully Completed!");
            this.getReplication();
          }
        });
      } catch (err) {
        err.desc && Message.error(err.desc);
        return Promise.reject(err);
      }
    },
    switchOperation(val, data) {
      if (val) {
        this.start(val, data);
      } else {
        this.stop(val, data);
      }
    },
    async getReplication() {
      try {
        let id = localStorage.getItem("local_clusterID");
        await fetch(
          `http://192.168.0.201:6050/tasks?detail=true&labels=type::replication,cluster-id::${id}`,
          {
            method: "get"
          }
        )
          .then(res => res.json())
          .then(result => {
            this.topicList = result.map(item => {
              item["fromdb"] = item.from.split("/").at(-1);
              item["hostport"] = item.from_expand?(item.from_expand.host+':'+item.from_expand.port):'';
              item["db"] = item.from_expand?item.from_expand.subject:'';
              return item;
            });
          });
      } catch (err) {
        err.desc && Message.error(err.desc);
        return Promise.reject(err);
      }
    },
    async getDatabases() {
      try {
        this.dblist = await getDBListReq();

        console.log(this.dblist, "获取数据库---");
      } catch (error) {
        console.log(error);
      }
    }
  },
  created() {
    this.getDatabases();
    this.getReplication();
  }
};
</script>
<style lang="scss" scoped>
.el-select {
  width: 100%;
}
.el-switch {
  margin-right: 10px;
}
</style>