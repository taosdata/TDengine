<template>
  <div>
    <el-form size="small" ref="form" :rules="rules" :model="info" label-width="auto" label-position="right">
      <!-- <el-form-item :label="$t('name')" prop="username">
        <el-input v-model.trim="info.username"></el-input>
      </el-form-item> -->
      <el-form-item :label="$t('email')" prop="email" required>
        <el-input v-model.trim="info.email"></el-input>
      </el-form-item>
      <el-form-item :label="$t('currentCluster')" prop="privileges">
        <Cascader ref="cascader" />
      </el-form-item>
      <el-form-item>
        <el-button style="width: 100%" @click="inviteUser" type="primary">{{ $t("add") }}</el-button>
      </el-form-item>
    </el-form>
  </div>
</template>

<script>
  import Cascader from "./cascader";
  import { validEmail } from "@/utils/validate.js";
  import { inviteUser } from "@/api/user";
  export default {
    components: { Cascader },
    data() {
      var checkEmail = async (_, value, callback) => {
        if (!value || !validEmail(value)) {
          return callback(new Error(this.$t("emailError")));
        }
      };

      return {
        info: {
          email: "",
          privileges: [],
        },
        requestIng: false,
        rules: {
          email: [{ validator: checkEmail, trigger: "blur" }],
          privileges: [
            {
              required: true,
              validator: (_, val, callback) => {
                callback(val?.length ? undefined : this.$t("users.noConfigPermission"));
              },
              trigger: "validate",
            },
          ],
        },
      };
    },
    methods: {
      inviteUser() {
        if (this.requestIng) return;
        this.info.privileges = this.$refs.cascader.getValue();
        this.$refs.form.validate(valid => {
          if (valid) {
            this.requestIng = true;
            inviteUser(this.info)
              .then(() => {
                this.$emit("close");
                this.$store.dispatch("user/getUserList");
                this.$message.success(this.$t("addSucc"));
              })
              .finally(() => {
                this.requestIng = false;
              });
          }
        });
      },
    },
  };
</script>

<style></style>
