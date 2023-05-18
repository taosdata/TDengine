<template>
  <div class="add-user-permission">
    <el-form
      style="text-align: left"
      ref="form"
      :model="info"
      label-position="left"
      label-width="120px"
    >
      <el-form-item :label="$t('user')" prop="user_name" required>
        <el-select v-model="info.user_name" style="width: 100%">
          <el-option
            v-for="item in userList"
            :key="item.name"
            :label="item.name"
            :value="item.name"
          ></el-option>
        </el-select>
      </el-form-item>
      <el-form-item :label="$t('role')" prop="roles">
        <el-checkbox-group v-model="checkList" class="check-role" :min="1">
          <el-checkbox label="Data Reader"></el-checkbox>
          <el-checkbox label="Data Writer"></el-checkbox>
          <!-- <el-checkbox label="Database Admin"></el-checkbox> -->
        </el-checkbox-group>
      </el-form-item>
      <el-form-item label=" ">
        <el-button
          class="w100"
          v-permission
          @click="add"
          type="primary"
          size="small"
          >{{ $t("add") }}</el-button
        >
      </el-form-item>
    </el-form>
  </div>
</template>

<script>
import { sendSQLReq } from "@/api/gateway/console";
import { Message } from "element-ui";
export default {
  props: {
    type: {
      type: String,
      default: "user",
    },
  },
  data() {
    return {
      checkList: ["Data Reader"],
      info: {
        user_name: "",
        roles: [],
      },
      requestIng: false,
      roles: [],
      userList: [],

      grantMap: new Map([
        ["Data Writer", "WRITE"],
        // ["Database Admin", "ALL"],
        ["Data Reader", "READ"],
      ]),
    };
  },
  created() {
    this.getUsers();
  },
  methods: {
    async getUsers() {
      try {
        await sendSQLReq("show users;").then((res) => {
          this.userList = res.data
            .map((data) => {
              return Object.fromEntries(
                res.column_meta.map((item, index) => {
                  return [item[0], data[index]];
                })
              );
            })
            .filter((val) => val.name != "root");
        });
      } catch (error) {
        console.log(error);
      }
    },
    add() {
      try {
        this.$refs.form.validate(async (valid) => {
          if (valid) {
            this.requestIng = true;
            let grantArr = this.checkList.map((item) => {
              return this.grantMap.get(item);
            });
            let grantStr = grantArr.includes("ALL")
              ? "ALL"
              : grantArr.filter((val) => val !== "ALL").join(",");
            let sql = `grant ${grantStr}  ON ${this.$store.state.dbs.selected_db}.* TO ${this.info.user_name};`;
            await sendSQLReq(sql).then((res) => {
              if (res && res.rows == 1) {
                Message.success(this.$t('addSucc'));
              }else{
                Message.error(this.$t('operateFail'));
              }
              this.$emit('close',true)
            });
          } else {
            return false;
          }
        });
      } catch (error) {
        console.log(error);
      }
    },
  },
};
</script>

<style scoped lang="scss">
.el-checkbox-group.check-role {
  display: flex;
  flex-direction: column;
}
</style>
