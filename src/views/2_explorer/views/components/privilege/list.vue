<template>
  <div class="">
    <section class="flexEnd">
      <el-button class="medium-btn" @click="add" icon="el-icon-plus" plain>{{
        $t("accessControl.addAccess")
      }}</el-button>
    </section>
    <el-table
      :max-height="height"
      size="mini"
      style="margin-top: 10px"
      :data="list"
      class="w100"
    >
      <template v-if="isUser">
        <el-table-column
          :label="$t('explorer.user_name')"
          min-width="100"
          prop="user_name"
        ></el-table-column>
        <el-table-column
          :label="$t('explorer.privilege')"
          min-width="100"
          prop="privilege"
        ></el-table-column>
        <!-- <el-table-column
          :label="$t('email')"
          prop="email"
          min-width="200"
        ></el-table-column> -->
      </template>
      <!-- <el-table-column
        v-else
        :label="$t('accessControl.groupName')"
        min-width="200"
        prop="group_name"
      >
        <template slot-scope="{ row }">
          <a @click.stop.prevent="manageUser(row)" class="default-link">{{
            row.group_name + `(${row.num})`
          }}</a>
        </template>
      </el-table-column> -->
      <!-- <el-table-column
        :label="$t('accessControl.resources')"
        min-width="100"
        prop="num"
      >
        <template>
          <a @click.prevent class="default-link">{{
            $t("accessControl.resources")
          }}</a>
        </template>
      </el-table-column> -->
      <!-- <el-table-column
        :label="$t(isUser ? 'accessControl.joinDate' : 'createTime')"
        width="160"
        :prop="isUser ? 'joinDate' : 'create_time'"
      >
      </el-table-column> -->
      <el-table-column fixed="right" :label="$t('operate')" width="100">
        <template slot-scope="{ row }">
          <el-switch
            @click.native.stop
            :disabled="row.user_name==='root'"
            size="mini"
            v-permission
            :value="row.status != 'DISABLED'"
            @change="(val) => statusChange(val, row)"
          />
          <!-- <el-button
            style="margin-left: 10px"
            v-permission
            :disabled="currentUserId == row.userId"
            @click.stop="del(row)"
            icon="el-icon-delete"
            size="mini"
          ></el-button> -->
        </template>
      </el-table-column>
    </el-table>
    <el-dialog
      :close-on-click-modal="false"
      :title="title"
      :visible.sync="dialog"
      center
      :width="width"
    >
      <component
        :is="comp"
        :key="dialogKey"
        @update="refresh"
        :type="type"
        @close="close"
        v-bind="dialogParams"
      ></component>
    </el-dialog>
  </div>
</template>

<script>
import { sendSQLReq } from "@/api/gateway/console";
import AddForm from "./add";
// import UserList from "@/views/15_organization_access_control/components/groupUserList";
import ResourceList from "./resourceList.vue";
import {
  disableDBUser,
  disableDBUserGroup,
  enableDBUser,
  enableDBUserGroup,
  deleteDBUser,
  deleteDBUserGroup,
} from "@/api/gateway/data/dbs";
import { Message } from 'element-ui';
export default {
  props: {
    type: {
      type: String,
      default: "user",
    },
  },
  inject: ["dbPrivilege"],
  components: { AddForm, ResourceList },
  data() {
    return {
      dialog: false,
      dialogType: 0,
      dialogParams: {},
      currentSelect: {},
      height: "500px",
      dialogKey: 0,
      list:[]
    };
  },
  computed: {
    isUser() {
      return this.type === "user";
    },

    currentDB() {
      return this.$store.state.dbs.selected_db;
    },
    comp() {
      return {
        0: "AddForm",
        1: "ResourceList",
      }[this.dialogType];
    },
    title() {
      return {
        0: this.$t("accessControl.addAccess"),
        1: {
          user: this.$t("accessControl.resourceBy", [
            this.$t("user"),
            `[${this.$t("usernameTep", [
              this.currentSelect.firstName,
              this.currentSelect.lastName,
            ])}]`,
          ]),
          group: this.$t("accessControl.resourceBy", [
            this.$t("userGroup"),
            `[${this.currentSelect.group_name}]`,
          ]),
        }[this.type],
        2: this.$t("accessControl.listGroupUser").replace(
          "Group",
          this.currentSelect.group_name
        ),
      }[this.dialogType];
    },
    width() {
      return {
        0: "480px",
        1: "800px",
      }[this.dialogType];
    },
    disabledFn() {
      return this.isUser ? disableDBUser : disableDBUserGroup;
    },
    enabledFn() {
      return this.isUser ? enableDBUser : enableDBUserGroup;
    },
    deleteFn() {
      return this.isUser ? deleteDBUser : deleteDBUserGroup;
    },

    currentUserId() {
      return this.$store.state.profile.userInfo?.id;
    },
  },
  watch: {},
  created() {
    this.height = Math.max(window.innerHeight - 300, 500) + "px";
  },
  mounted() {
    this.getData()
  },
  methods: {
    async getData(){
      try {
        await sendSQLReq(`select user_name, privilege from information_schema.ins_user_privileges where privilege in ('all', 'read', 'write') and db_name in ('${this.currentDB}', 'all');`).then(res=>{
          this.list= res.data.map((data) => {
            return Object.fromEntries(
              res.column_meta.map((item, index) => {
                return [item[0], data[index]];
              })
            );
          });

        })
      } catch (error) {
        console.log(error);
      }
    },
    add() {
      this.dialogType = 0;
      this.dialogParams = {};
      this.dialog = true;
    },
    rowClick(row) {
      this.dialogParams = {
        id: row.userId || row.id,
        level: "instance",
        email: row.email,
        disabled: row.status == "DISABLED",
      };
      this.currentSelect = row;
      this.dialogType = 1;
      this.dialogKey++;
      this.dialog = true;
    },
    close() {
      this.refresh();
      this.dialog = false;
    },
    statusChange(val, data) {
      this.$confirm(
        this.getTipName(data, val ? this.$t("enable") : this.$t("disable")),
        this.$t("tips"),
        {
          confirmButtonText: this.$t("confirm"),
          cancelButtonText: this.$t("cancel"),
          type: "warning",
        }
      ).then(async () => {
        this.requesting = true;
        sendSQLReq(`REVOKE ${data.privilege} ON \`${this.$store.state.dbs.selected_db}\` FROM \`${data.user_name}\``).then(res=>{
          
          if(res&&res.rows==1){
            Message.success(this.$t('operateSucc'))
            this.refresh()
          }
        })
      });
    },
    refresh() {
      this.getData()
    },
    manageUser(row) {
      this.dialogType = 2;
      this.currentSelect = row;
      this.dialogParams = {
        group_id: row.id,
        level: 0,
      };
      this.dialog = true;
    },

    getTipName(data, type) {
      let tip = this.$t("data.changeDBPrivilegeTip").replace(`{type}`, type);
      tip = tip.replace(`{listType}`, this.type);
      tip = tip.replace(`{dbName}`, this.currentDB);
      if (this.isUser) {
        if (data.email) {
          tip = tip.replace(`{email}`, data.email);
        } else {
          tip = tip.replace(`{email}`, data.firstName + " " + data.lastName);
        }
      } else {
        tip = tip.replace(`{groupName}`, this.group_name || data.groupName);
      }
      return tip || "the " + data.roleName + " role";
    },
    del(data) {
      if (this.requesting) return;
      this.$confirm(this.getTipName(data, "delete"), this.$t("tips"), {
        confirmButtonText: this.$t("confirm"),
        cancelButtonText: this.$t("cancel"),
        type: "warning",
      }).then(async () => {
        this.requesting = true;
        this.deleteFn(data.userId || data.id, this.currentDB)
          .then(() => {
            this.$message.success(this.$t("delSucc"));
          })
          .finally(() => {
            this.requesting = false;
            this.refresh();
          });
      });
    },
    // hasPrivilege(privilege) {
    //   return this.dbPrivilege.some(item => item.name == `${this.type}-role:${privilege}`);
    // },
  },
};
</script>

<style scoped lang="scss">
.el-button.medium-btn{
  padding:6px 20px;
}
</style>
