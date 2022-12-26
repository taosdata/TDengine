<template>
  <el-card class="cluster-db">
    <section class="flexBetween">
      <h1 class="title">{{ cluster.name }}</h1>

      <div>
        <el-button :disabled="disabled" plain size="mini" @click="dialog = true" icon="el-icon-plus">{{ $t("users.addNewDB") }}</el-button>
        <el-button size="mini" plain @click="delCluster" icon="el-icon-delete"></el-button>
      </div>
    </section>

    <el-table v-if="cluster.dbList.length" size="mini" :show-header="false" :data="cluster.dbList">
      <el-table-column width="100" :label="$t('name')" prop="name"></el-table-column>

      <el-table-column :label="$t('users.read')" prop="name">
        <template slot-scope="scope">
          {{ $t("users.read") }}
          <el-switch
            :disabled="disabled"
            style="margin-left: 10px"
            v-model="scope.row.read"
            active-color="#13ce66"
            @change="() => change(scope.row)"
            inactive-color="#ff4949"
          >
          </el-switch>
        </template>
      </el-table-column>

      <el-table-column :label="$t('users.write')" prop="name">
        <template slot-scope="scope">
          {{ $t("users.write") }}
          <el-switch
            :disabled="disabled"
            style="margin-left: 10px"
            v-model="scope.row.write"
            active-color="#13ce66"
            @change="() => change(scope.row)"
            inactive-color="#ff4949"
          >
          </el-switch>
        </template>
      </el-table-column>

      <!-- <el-table-column :label="$t('users.duration')" prop="name">
        <template slot-scope="scope">
          <el-date-picker
            v-model="scope.row.duration"
            size="mini"
            type="datetimerange"
            range-separator="-"
            :start-placeholder="$t('start')"
            :end-placeholder="$t('end')"
          >
          </el-date-picker>
        </template>
      </el-table-column> -->

      <el-table-column fixed="right" align="right" width="100" :label="$t('operate')" prop="name">
        <template slot-scope="scope">
          <el-button v-if="!disabled" :disabled="disabled" @click="del(scope.row, scope.$index)" size="mini" plain icon="el-icon-delete"></el-button>
        </template>
      </el-table-column>
    </el-table>
    <el-empty v-else :image-size="100"></el-empty>
    <el-dialog :title="$t('users.addNewDB')" :visible.sync="dialog" width="700px">
      <Cascader ref="cascader" :cluster="cluster" />
      <span slot="footer" class="dialog-footer">
        <el-button size="small" @click="dialog = false">{{ $t("cancel") }}</el-button>
        <el-button type="primary" size="small" @click="add">{{ $t("confirm") }}</el-button>
      </span>
    </el-dialog>
  </el-card>
</template>

<script>
  import { updateUserPermission, addUserPermission, deleteUserPermission } from "@/api/user";
  import Cascader from "./cascader.vue";
  export default {
    props: {
      cluster: {
        type: Object,
        default: () => ({}),
      },
      id: {
        type: String,
        default: "",
      },
    },
    components: { Cascader },
    data() {
      return {
        requestIng: false,
        dialog: false,
      };
    },
    computed: {
      disabled() {
        return this.cluster.disabled;
      },
    },
    methods: {
      del(row, update = false) {
        if (this.requestIng) return;
        this.$confirm(this.$t("data.delDatabase") + ":" + row.name + "?", this.$t("tips"), {
          confirmButtonText: this.$t("confirm"),
          cancelButtonText: this.$t("cancel"),
          type: "warning",
        })
          .then(async () => {
            this.requesting = true;
            deleteUserPermission({ id: [row.id] })
              .then(() => {
                this.$message.success(this.$t("delSucc"));
                this.$emit("update");
              })
              .finally(() => (this.requestIng = false));
          })
          .catch(() => {
            update && this.$emit("update");
          });
      },
      add() {
        if (this.requestIng) return;
        const privileges = this.$refs.cascader.getValue();
        if (!privileges.length) {
          return (this.dialog = false);
        }
        this.requestIng = true;
        addUserPermission({ user_id: this.id, privileges })
          .then(() => {
            this.dialog = false;
            this.$message.success(this.$t("addSucc"));
          })
          .finally(() => {
            this.$emit("update");
            this.requestIng = false;
          });
      },
      change(row) {
        if (this.disabled) return;
        if (!row.read && !row.write) return this.del(row, true);
        if (this.requestIng) return;
        this.requestIng = true;
        updateUserPermission({
          privilege_id: row.id,
          privilege: this.cluster.value + ":" + row.name + ":" + (row.read ? "r" : "") + (row.write ? "w" : ""),
        })
          .then(() => {
            this.$message.success(this.$t("changeSucc"));
          })
          .catch(() => {
            this.$emit("update");
          })
          .finally(() => {
            this.requestIng = false;
          });
      },
      delCluster() {
        if (this.requestIng) return;
        this.$confirm(this.$t("delCluster") + ":" + this.cluster.name + "?", this.$t("tips"), {
          confirmButtonText: this.$t("confirm"),
          cancelButtonText: this.$t("cancel"),
          type: "warning",
        }).then(async () => {
          this.requesting = true;
          let id = this.cluster.dbList.map(item => item.id);
          deleteUserPermission({ id })
            .then(() => {
              this.$message.success(this.$t("delSucc"));
              this.$emit("update");
            })
            .finally(() => (this.requestIng = false));
        });
      },
    },
  };
</script>

<style lang="scss" scoped>
  $content-padding: 20px;
  .cluster-db {
    margin-top: 20px;
  }
  .flexBetween {
    // border-top: 1px solid $divider-color;
    border-bottom: 1px solid $divider-color;
    padding: 0 10px 10px;
  }
  .select-all {
    margin-top: 20px;
    font-size: 16px;
    line-height: 30px;
    text-align: center;
  }
  .db-item {
    display: flex;
    align-items: center;
    .name {
      font-size: 16px;
      min-width: 100px;
    }
    .label {
      font-size: 14px;
      display: inline-block;
      margin-right: 10px;
    }
    .permission {
      display: flex;
      align-items: center;
      min-width: 100px;
    }
  }
</style>
