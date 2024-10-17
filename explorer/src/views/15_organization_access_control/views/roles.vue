<template>
  <div class="role-list">
    <div class="flexEnd">
      <el-button v-permission.noCheck="'role:add'" class="medium-btn" @click="add" plain size="small" icon="el-icon-plus">{{
        $t("accessControl.addNewRole")
      }}</el-button>
    </div>
    <el-table :max-height="height" class="box-table" size="mini" :data="options">
      <el-table-column fixed="left" label="" width="250" prop="name">
        <template slot-scope="{ row, $index }">
          <span :class="{ parent: !options[$index].type, child: options[$index].type }">{{ options[$index].label }}</span>
        </template>
      </el-table-column>
      <el-table-column v-for="item in roleList" :key="item.id + roleKey" :label="item.name" min-width="80" prop="name">
        <template slot="header" slot-scope="{}">
          <div class="role-header">
            <el-tooltip effect="light" :content="item.name" placement="top">
              <span class="left">{{ item.name }}</span>
            </el-tooltip>
            <span v-if="item.account_id != '0'" class="right">
              <i v-permission.noCheck="'role:update'" @click="edit(item)" class="el-icon-edit"></i>
              <i v-permission="'role:delete'" @click="del(item)" class="el-icon-delete"></i>
            </span>
          </div>
        </template>
        <template slot-scope="{ $index }">
          <i :class="isHasPrivilege(item, options[$index])"></i>
        </template>
      </el-table-column>
      <!-- <el-table-column fixed="right" :label="$t('operation')" width="100">
        <template slot-scope="{ row }">
          <el-button v-if="row.account_id != '0'" @click="edit(row)" type="primary" size="mini" icon="el-icon-edit"></el-button>
          <el-button
            v-if="row.account_id != '0'"
            v-permission="'role:delete'"
            :disabled="row.account_id == '0'"
            size="mini"
            @click="del(row)"
            icon="el-icon-delete"
          ></el-button>
        </template>
      </el-table-column> -->
    </el-table>
    <!-- <el-pagination
      class="pagination"
      layout="total, prev, pager, next"
      :current-page.sync="currentPage"
      :page-size="pageSize"
      :hide-on-single-page="true"
      :total="total"
      @current-change="handlePageChange"
    >
    </el-pagination> -->
    <el-dialog :close-on-click-modal="false" align="center" :title="title" width="400px" :visible.sync="dialog">
      <component :is="comp" @close="close" v-bind="dialogParmas" />
    </el-dialog>
  </div>
</template>

<script>
  import { getRoleList, deleteRole } from "@/api/role";
  import AddForm from "../components/roleEditForm";
  import { getPrivilegeTypeMap } from "../components/privilege";
  import PrivilegeDisplay from "@/views/14_organization/components/privilegeDisplay";

  export default {
    components: {
      AddForm,
      PrivilegeDisplay,
    },
    data() {
      return {
        roleList: [],
        currentPage: 1,
        pageSize: 10,
        total: 0,
        requestIng: false,
        dialog: false,
        dialogParmas: {},
        dialogType: 0,
        height: "500px",
        roleKey: 0,
      };
    },
    computed: {
      comp() {
        return {
          0: "AddForm",
          1: "AddForm",
        }[this.dialogType];
      },
      title() {
        return {
          0: this.$t("accessControl.addNewRole"),
          1: this.$t("accessControl.editRole"),
        }[this.dialogType];
      },
      options() {
        const typeMap = getPrivilegeTypeMap();

        return Object.keys(typeMap).reduce((pre, cur) => {
          pre.push({ id: cur, label: this.$t(cur) + " Level" });
          if (typeMap[cur]) {
            typeMap[cur].forEach(item => {
              pre.push(item);
            });
          }
          return pre;
        }, []);
      },
    },
    created() {
      this.getData();
      this.height = Math.max(window.innerHeight - 300, 500) + "px";
    },
    methods: {
      handlePageChange(val) {
        this.currentPage = val;
        this.getData();
      },
      add() {
        this.dialogType = 0;
        this.dialogParmas = {};
        this.dialog = true;
      },
      edit(data) {
        this.dialogType = 1;
        this.dialogParmas = {
          info: {
            name: data.name,
            id: data.id,
            privileges: {
              common: data.common.map(item => item.id),
              org: data.org
                .filter(item => !data.instance.some(ite => ite.id == item.id))
                .filter(item => !data.db.some(ite => ite.id == item.id))
                .map(item => item.id),
              instance: data.instance.map(item => item.id),
              db: data.db.map(item => item.id),
            },
            accountId: data.account_id,
            version: data.version,
          },
        };
        this.dialog = true;
      },
      close() {
        this.getData();
        this.dialog = false;
      },
      isHasPrivilege(row, privilege) {
        const type = privilege.type;
        if (!type) return "";
        if (!row[type] || !row[type].some(item => item.id == privilege.id)) return "el-icon-close";
        return "el-icon-check";
      },
      getData() {
        if (this.requestIng) return;
        getRoleList({
          current_page: this.currentPage,
          page_size: this.pageSize,
        })
          .then(data => {
            this.roleKey++;
            this.roleList = data.filter(item => item.id != "9").reverse();
            // this.total = total;
          })
          .catch(() => {
            this.roleList = [];
            // this.total = 0;
          })
          .finally(() => {
            this.requestIng = false;
          });
      },
      del(row) {
        if (this.requestIng) return;
        this.$confirm(this.$t("del") + ":" + row.name, this.$t("warning"), {
          confirmButtonText: this.$t("confirm"),
          cancelButtonText: this.$t("cancel"),
          type: "warning",
        })
          .then(() => {
            this.requestIng = true;
            deleteRole(row.id)
              .then(() => {
                this.$message.success(this.$t("delSucc"));
              })
              .finally(() => {
                this.requestIng = false;
                this.getData();
              });
          })
          .catch(() => {});
      },
      handlePrivilegeList(data) {
        return data.map(item => item.name);
      },
    },
  };
</script>

<style lang="scss" scoped>
  .parent {
    font-weight: bold;
  }
  .child {
    padding-left: 10px;
  }
  .role-list {
    &:deep(.role-header) {
      display: flex;
      align-items: center;
      word-break: keep-all;
      position: relative;
      .left {
        flex: 1;
        padding-right: 5px;
      }
      .right {
        background-color: #fff;
        position: absolute;
        right: 0px;
        flex-shrink: 0;
        font-size: 13px;
        display: none;
        span + span {
          margin-left: 5px;
        }
      }
      &:hover {
        .right {
          display: block;
          color: $color-primary;
        }
      }
    }
  }
</style>
