<template>
  <el-form ref="perfect" :model="perfectForm" :status-icon="true" :rules="rules" :hide-required-asterisk="true">
    <el-form-item :label="$t('fullName')" prop="username">
      <el-input v-model.trim="perfectForm.username" />
    </el-form-item>

    <el-form-item :label="$t('password')" prop="password">
      <el-popover trigger="hover" popper-class="password-tip" placement="right">
        <div v-html="$t('passwordTip')"></div>

        <el-input
          slot="reference"
          v-model.trim="perfectForm.password"
          @keyup.enter.native="update"
          minlength="8"
          show-password
          maxlength="16"
        ></el-input>
      </el-popover>
    </el-form-item>

    <el-form-item :label="$t('confirmPass')" prop="confirm">
      <el-input v-model.trim="perfectForm.confirm" minlength="8" show-password maxlength="16" @keyup.enter.native="update"></el-input>
    </el-form-item>

    <p class="errorText">{{ errorText }}</p>

    <el-form-item label=" ">
      <section class="login-block">
        <el-button class="loginBtn" @click="update" type="primary">{{ $t("save") }}</el-button>
      </section>
    </el-form-item>
  </el-form>
</template>

<script>
  import { validPassword } from "@/utils/validate.js";
  import { putUserInfo } from "@/api/register";
  export default {
    data() {
      var checkPassword = async (_, value, callback) => {
        this.errorText = "";
        if (!validPassword(value)) {
          return callback(new Error(this.$t("passwordError")));
        }
      };
      let cheakConfirmPassword = async (_, value, callback) => {
        this.errorText = "";
        if (value != this.perfectForm.password || !value) return callback(new Error(this.$t("twoPassError")));
      };
      return {
        perfectForm: {
          username: "",
          password: "",
          confirm: "",
        },
        rules: {
          username: [
            {
              required: true,
              message: this.$t("register.nameError"),
              trigger: "blur",
            },
            {
              min: 4,
              max: 32,
              message: this.$t("register.nameError"),
              trigger: "blur",
            },
          ],
          password: [{ validator: checkPassword, trigger: "blur" }],
          confirm: [{ validator: cheakConfirmPassword, trigger: "blur" }],
        },
        requestIng: false,
        errorText: "",
      };
    },
    methods: {
      update() {
        if (this.requestIng) return;
        this.$refs.perfect.validate(valid => {
          if (valid) {
            this.requestIng = true;
            putUserInfo(this.perfectForm)
              .then(async () => {
                await this.$store.dispatch("app/getUserInfo");
                this.$router.push("/instances");
              })
              .catch(err => {
                this.errorText = err.msg || err.message;
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
