<template>
  <div class="dnode-block mt20">
    <div class="primary-tip flexBetween">
      <span>{{ $t('dataIn.agents') }}</span>
      <div>
        <el-button
          plain
          @click="refresh"
          type="text"
          size="small"
          class="medium-btn"
          icon="el-icon-refresh"
          :disabled="requestIng"
          >{{ $t('refresh') }}</el-button
        >
        <el-button
          plain
          @click="update()"
          type="text"
          size="small"
          class="medium-btn"
          icon="el-icon-plus"
          >{{ $t('dataIn.createNewAgent') }}</el-button
        >
      </div>
    </div>
    <el-table
      class="box-tb"
      :data="agentList"
      size="mini"
      row-key="id"
      :expand-row-keys="expandRowKeys"
      @expand-change="expandChange"
    >
      <el-table-column type="expand">
        <template slot-scope="scope">
          <Activities
            :id="scope.row.id"
            :select-id="selectedId"
            :type="'agent'"
          ></Activities>
        </template>
      </el-table-column>
      <el-table-column
        label="ID"
        prop="id"
        show-overflow-tooltip
        min-width="160"
      ></el-table-column>

      <!-- <el-table-column
        :label="$t('dataIn.clusterId')"
        prop="cluster_id"
        show-overflow-tooltip
        width="200"
      ></el-table-column> -->
      <el-table-column
        :label="$t('name')"
        prop="name"
        show-overflow-tooltip
        min-width="200"
      ></el-table-column>

      <!-- <el-table-column
        :label="$t('dataIn.connector')"
        prop="connectors"
        show-overflow-tooltip
        width="200"
      >
        <template slot-scope="scope"> <RoleDisplay :roles="scope.row.connectors" /> </template
      ></el-table-column> -->
      <el-table-column
        :label="$t('data.createAt')"
        prop="created_at"
        show-overflow-tooltip
        width="250"
      >
        <span slot-scope="scope">{{ parseTime(scope.row.created_at, 'YYYY-MM-DD HH:mm:ss') }}</span>
      </el-table-column>
      <el-table-column
        :label="$t('status')"
        prop="status"
        show-overflow-tooltip
        width="250"
      ></el-table-column>
      <el-table-column
        :label="$t('operation')"
        width="100"
      >
        <template slot-scope="scope">
          <el-tooltip
            effect="light"
            :content="$t('edit')"
          >
            <el-button
              plain
              class="mini-btn"
              @click="update(scope.row)"
              icon="el-icon-edit"
            ></el-button>
          </el-tooltip>

          <el-tooltip
            effect="light"
            :content="$t('del')"
          >
            <el-button
              plain
              class="mini-btn"
              @click="del(scope.row)"
              icon="el-icon-delete"
            ></el-button>
          </el-tooltip>
        </template>
      </el-table-column>
    </el-table>
    <el-drawer
      :title="$t('dataIn.agentUsageDocs')"
      :withHeader="false"
      :visible.sync="drawer"
    >
      <AgentDocs style="padding: 10px 20px" />
    </el-drawer>
  </div>
</template>
<script>
import AgentDocs from './taosxAgent.vue';
// import RoleDisplay from '@/views/14_organization/components/roleDisplay.vue';
import { parseTime } from '@/utils';
import Activities from './activities.vue';
export default {
  components: {
    Activities,
    AgentDocs
  },
  data() {
    return {
      expireTimeOPtion: {
        disabledDate(time) {
          return time.getTime() < Date.now();
        }
      },
      agenttoken: '',
      requestIng: false,
      dblist: [],
      isEditDialog: false,
      dialogTitle: 'Create New Agent',
      dialog: false,
      copyDialog: false,
      operateStatus: true,
      currentRow: null,
      clusterid: localStorage.getItem('local_clusterID'),
      ruleForm: {
        name: '',
        connectors: '',
        expire_date: ''
      },
      drawer: false,
      expandRowKeys: [],
      selectedId: -1
    };
  },
  computed: {
    agentList() {
      return this.$store.state.dataIn.agentList;
    },
    confirmStatus() {
      if (!this.ruleForm.name) {
        return true;
      }

      if (this.ruleForm.connectors == '') {
        return true;
      }
      if (!this.ruleForm.expire_date) {
        return true;
      }

      return false;
    }
  },
  created() {
    // this.getAgents();
  },
  methods: {
    parseTime,
    closeDialog() {
      this.$refs.ruleForm.resetFields();
      this.$refs.ruleForm.clearValidate();
      this.dialog = false;
    },
    del(data) {
      this.$confirm(this.$t('dataIn.deletetip', [data.name]), this.$t('warning'), {
        confirmButtonText: this.$t('confirm'),
        cancelButtonText: this.$t('cancel'),
        type: 'warning'
      }).then(() => {
        
      });
    },
    update(data) {
      this.$emit('update', data);
    },
    showToken(token) {
      // const h = this.$createElement.bind(this);
      const t = this.$t.bind(this);
      this.$alert(
        `<p class='warning-tip'>${t('dataIn.copyTokenTip')}</p>
        <p>${token}</p>`,
        t('dataIn.agentTokenCopyTip'),
        {
          confirmButtonText: t('copy'),
          customClass: 'copy-agent',
          dangerouslyUseHTMLString: true,
          beforeClose: (_, __, done) => {
            this.copyToken(token);
            done();
          }
        }
      );
    },
    refresh() {
      this.getAgents();
    },
    //切换状态
    switchOperation(val, data) {
      if (val) {
        this.$confirm(this.$t('replication.backupTip').replace('{operate}', 'start').replace('{id}', data.id), this.$t('warning'), {
          confirmButtonText: this.$t('confirm'),
          cancelButtonText: this.$t('cancel'),
          type: 'warning'
        }).then(() => {
          this.start(val, data);
        });
      } else {
        this.$confirm(this.$t('replication.backupTip').replace('{operate}', 'stop').replace('{id}', data.id), this.$t('warning'), {
          confirmButtonText: this.$t('confirm'),
          cancelButtonText: this.$t('cancel'),
          type: 'warning'
        }).then(() => {
          this.stop(val, data);
        });
      }
    },
    viewDocument() {
      this.drawer = true;
    },
    getAgents() {
      this.$store.dispatch('dataIn/getAgentList');
    },
    async expandChange(row) {
      if (row.id == this.expandRowKeys[0]) {
        this.expandRowKeys = [];
        this.selectedId = -1;
        return;
      }
      this.expandRowKeys = [row.id];
      this.selectedId = row.id;
    }
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

.agent-token {
  white-space: nowrap;
  text-overflow: ellipsis;
  overflow: hidden;
  display: inline-block;
}
.copy-icon {
  visibility: hidden;
  display: flex;
  align-items: center;
  white-space: nowrap;
  cursor: pointer;
  color: #4259ce;
}
.agentcopy {
  display: flex;
  &:hover {
    .copy-icon {
      visibility: visible;
    }
  }
}
::v-deep {
  .el-dialog__wrapper.copy-agent {
    .el-dialog__header {
      display: flex;
    }
  }
}
</style>
<style lang="scss">
.copy-agent {
  width: 600px;
  p {
    word-break: break-all;
  }
}
</style>
