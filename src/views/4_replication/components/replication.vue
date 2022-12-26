<template>
  <!-- <ul class="destination">
    <el-empty v-if="!taskList.length" :image-size="100" :description="$t('replication.noData')"></el-empty>
    <li v-for="(item, index) in taskList" :key="index" class="destination-item">
      <section class="title">
        {{ item.type }}: {{ item.type == "file" ? item.name : item.url }}
        <i class="el-icon-edit icon-edit"></i>
      </section>
      <section class="time">{{ $t("data.createAt") }} 2022-2-5</section>
      <p class="status">Activated</p>
      <section class="operate-btn">
        <i class="el-icon-delete icon-delete"></i>
      </section>
    </li>
  </ul> -->
  <section class="replication-list">
    <el-table tooltip-effect="light" size="mini" :data="taskList" style="width: 100%">
      <el-table-column show-overflow-tooltip prop="id" label="ID" width="80"> </el-table-column>
      <!-- <el-table-column prop="from_cluster" :label="$t('replication.fromIns')" min-width="180"> </el-table-column> -->
      <el-table-column prop="fromToken" :label="$t('replication.fromInstanceToken')" width="160">
        <template slot-scope="{ row }">
          <CopyText :text="row.fromToken" />
        </template>
      </el-table-column>
      <el-table-column show-overflow-tooltip prop="fromDb" :label="$t('replication.fromDB')" min-width="120"> </el-table-column>
      <el-table-column show-overflow-tooltip prop="toClusterAlias" :label="$t('replication.toIns')" min-width="180">
        <template slot-scope="{ row }"> {{ row.toCloudName }}/{{ row.toRegionName }}/{{ row.toClusterAlias }} </template>
      </el-table-column>
      <el-table-column show-overflow-tooltip prop="toDb" :label="$t('replication.toDB')" min-width="150"> </el-table-column>
      <el-table-column prop="toToken" :label="$t('replication.toInstanceToken')" min-width="150">
        <template slot-scope="{ row }">
          <CopyText :text="row.toToken" />
        </template>
      </el-table-column>
      <el-table-column prop="status" :label="$t('status')" width="150"> </el-table-column>
      <el-table-column show-overflow-tooltip prop="reason" :label="$t('reason')" width="150">
        <template slot-scope="{ row }">
          <CopyText :text="row.status != 'running' ? row.reason : ''" />
        </template>
      </el-table-column>
      <el-table-column prop="finished_at" :label="$t('finishedAt')" width="160">
        <template slot-scope="{ row }">
          {{ row.status == "running" ? "" : row.finished_at }}
        </template>
      </el-table-column>
      <el-table-column prop="created_at" :label="$t('support.createAt')" width="160"> </el-table-column>
      <el-table-column fixed="right" :label="$t('operate')" width="100">
        <template slot-scope="{ row }">
          <el-switch
            size="mini"
            :disabled="getStatusDisabled(row.status)"
            :value="getTaskStatus(row.status)"
            @change="handleTaskStatus($event, row)"
          ></el-switch>
          <el-button style="margin-left: 10px" size="mini" type="danger" icon="el-icon-delete" @click="handleDelete(row)"></el-button>
        </template>
      </el-table-column>
    </el-table>
  </section>
</template>

<script>
  import { copy } from "@/utils";
  import { deleteTask, startTask, stopTask } from "@/api/replication";
  import { ReplicationTaskCanStopStatus, ReplicationTaskCanStartStatus } from "@/const";
  export default {
    data() {
      return {
        requestIng: false,
      };
    },
    computed: {
      taskList() {
        return this.$store.state.replication.taskList;
      },
      appId() {
        return this.$store.getters.appId;
      },
    },
    methods: {
      copyToken(token) {
        copy(token);
      },
      handleTaskStatus(val, row) {
        this.$confirm(
          `${this.$t(val ? "replication.start" : "replication.stop")} ${this.$t("replication.theTask")} ${this.$t("withID")} ${row.id}?`,
          this.$t("wraning"),
          {
            confirmButtonText: this.$t("confirm"),
            cancelButtonText: this.$t("cancel"),
            type: "warning",
          }
        )
          .then(() => {
            const fn = val ? startTask : stopTask;
            fn(this.appId, row.id)
              .then(() => {
                this.$message.success(this.$t("operateSucc"));
              })
              .finally(() => {
                this.requestIng = false;
                this.$store.dispatch("replication/getTaskList");
              });
          })
          .catch(() => {});
      },
      handleDelete(row) {
        if (this.requestIng) return;
        this.$confirm(`${this.$t("del")} ${this.$t("replication.theTask")} ${this.$t("withID")} ${row.id}?`, this.$t("wraning"), {
          confirmButtonText: this.$t("confirm"),
          cancelButtonText: this.$t("cancel"),
          type: "warning",
        })
          .then(() => {
            this.requestIng = true;
            deleteTask(this.appId, row.id).then(() => {
              this.$message.success(this.$t("delSucc"));
            });
          })
          .finally(() => {
            this.requestIng = false;
            this.$store.dispatch("replication/getTaskList");
          });
      },
      getTaskStatus(status) {
        return ReplicationTaskCanStopStatus.includes(status);
      },
      getStatusDisabled(status) {
        if (this.requestIng) return true;
        return !this.getTaskStatus(status) && !ReplicationTaskCanStartStatus.includes(status);
      },
    },
  };
</script>

<style lang="scss" scoped>
  .replication-list {
    margin-top: 20px;
  }
  .destination-item {
    display: flex;
    align-items: center;
    position: relative;
    padding: 5px 10px;
    border-bottom: 1px solid $divider-color;
    padding-right: 100px;
    cursor: pointer;
    &:hover {
      .operate-btn,
      .icon-edit {
        display: block !important;
      }
    }
    .title {
      flex: 1.5 1 200px;
      position: relative;
      padding-right: 30px;
      .icon-edit {
        position: absolute;
        display: none;
        right: 0;
        top: 50%;
        transform: translateY(-50%);
        cursor: pointer;
      }
    }
    .time {
      padding-left: 50px;
      flex: 0.5 0 200px;
    }
    .operate-btn {
      display: none;
      position: absolute;
      right: 10px;
    }
  }
</style>
