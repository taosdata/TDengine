<template>
  <div class="detail">
    <section class="header">
      <el-form :inline="true">
        <el-form-item :label="$t('firstName') + ':'">{{ userInfo.firstname }}</el-form-item>
        <el-form-item :label="$t('lastName') + ':'">{{ userInfo.lastname }}</el-form-item>
        <el-form-item :label="$t('email') + ':'">{{ userInfo.email }}</el-form-item>
        <el-form-item :label="$t('role') + ':'">{{ userInfo.role }}</el-form-item>
        <el-form-item :label="$t('status') + ':'">{{ currentStatus.text }}</el-form-item>
      </el-form>
      <el-radio-group size="small" v-model="status" @change="changeStatus">
        <el-radio-button :label="1">{{ $t("enable") }}</el-radio-button>
        <el-radio-button :label="3">{{ $t("disable") }}</el-radio-button>
      </el-radio-group>
    </section>
    <section class="operate-btn">
      <el-button size="mini" @click="add" :disabled="addDisabled" plain icon="el-icon-plus" class="big-button">{{
        $t("users.addNewCluster")
      }}</el-button>
    </section>
    <el-empty :description="$t('users.noConfigPermission')" v-if="!userInfo.privileges.length"></el-empty>
    <Cluster @update="getDetail" v-for="item in userInfo.privileges" :id="id" :key="item.name" :cluster="item" />
    <el-dialog :visible.sync="dialog" width="700px">
      <Cascader :filterList="filterList" ref="cascader" />
      <span slot="footer" class="dialog-footer">
        <el-button size="small" @click="dialog = false">{{ $t("cancel") }}</el-button>
        <el-button type="primary" size="small" @click="addCluster">{{ $t("confirm") }}</el-button>
      </span>
    </el-dialog>
  </div>
</template>

<script>
  import Cluster from "../components/clusterDB";
  import { getUserPermission, addUserPermission, enableUser, disableUser } from "@/api/user";
  import Cascader from "../components/cascader";
  export default {
    props: {
      id: {
        type: String,
        default: "",
      },
    },
    components: { Cluster, Cascader },
    data() {
      this.statusObj = {
        0: {
          type: "danger",
          text: this.$t("inactivated"),
        },
        1: {
          type: "success",
          text: this.$t("activated"),
        },
        2: {
          type: "danger",
          text: this.$t("incomplete"),
        },
        3: {
          type: "info",
          text: this.$t("disabled"),
        },
      };
      return {
        dialog: false,
        userInfo: {
          privileges: [],
        },
        filterList: [],
        requestIng: false,
        status: 3,
        realStatus: 3,
      };
    },
    computed: {
      clusterList() {
        return this.$store.state.app.clusters.reduce((pre, cur) => {
          pre[cur.id] = {
            name: cur.alias || cur.name,
            disabled: cur.cluster_status !== "Running",
          };
          return pre;
        }, {});
      },
      currentStatus() {
        return this.statusObj[this.realStatus];
      },
      runningCluster() {
        return this.$store.state.app.clusters.filter(item => item.cluster_status == "Running");
      },
      addDisabled() {
        return this.requestIng || this.runningCluster.length === this.filterList.length;
      },
    },
    created() {
      this.getDetail();
    },
    methods: {
      getDetail() {
        getUserPermission(this.id)
          .then(res => {
            let privileges = {};
            res.privileges.forEach(async item => {
              if (!privileges[item.appId]) {
                privileges[item.appId] = {
                  name: this.clusterList[item.appId]?.name,
                  dbList: [],
                  value: item.appId,
                  disabled: this.clusterList[item.appId]?.disabled,
                  checked: true,
                  disabledSelectAll: true,
                };
              }
              let permission = item.wildcard.split(":");
              privileges[item.appId].dbList.push({
                name: permission[1],
                read: permission[2].includes("r"),
                write: permission[2].includes("w"),
                id: item.id,
              });
            });
            res.privileges = Object.values(privileges);
            this.filterList = Object.keys(privileges);
            this.status = res.status == 3 ? 3 : 1;
            this.realStatus = res.status;
            this.userInfo = res;
          })
          .catch(() => {
            this.userInfo = { privileges: [] };
          });
      },
      add() {
        this.dialog = true;
      },
      addCluster() {
        if (this.requestIng) return;
        let cluster = this.$refs.cascader.getValue();
        if (!cluster.length) return (this.dialog = false);
        this.requestIng = true;
        addUserPermission({ user_id: this.id, privileges: cluster })
          .then(() => {
            this.dialog = false;
            this.getDetail();
            this.$message.success(this.$t("addSucc"));
          })
          .finally(() => {
            this.requestIng = false;
          });
      },
      async changeStatus(val) {
        if (this.requestIng) return;
        this.requestIng = true;
        let fn = val == 1 ? enableUser : disableUser;
        await fn(this.id)
          .then(() => {
            this.$message.success(this.$t("operateSucc"));
          })
          .catch(() => {});
        this.getDetail();
        this.requestIng = false;
      },
    },
  };
</script>

<style lang="scss" scoped>
  .header {
    margin-bottom: 10px;
    display: flex;
    justify-content: space-between;
    align-items: center;
  }
  .operate-btn {
    text-align: right;
  }
</style>
