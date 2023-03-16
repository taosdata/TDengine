<template>
  <div class="page-wrapper profile">
    <MainContentHeader :title="$t('setting.profile')"> </MainContentHeader>
    <section class="content">
      <ChangePass @close="isPassword = false" class="formStyle" :class="{ edit: isPassword }" />
      <!-- <el-form
        v-else
        ref="userInfo"
        :model="userInfo"
        :rules="rules"
        label-position="top"
        label-width="auto"
        size="small"
        class="formStyle"
        :class="{ edit: isEdit }"
      >
        <el-form-item :label="$t('email') + ':'">
          <el-input v-model="email" :placeholder="$t('email')" disabled></el-input>
        </el-form-item>
        <el-row>
          <el-col :span="11">
            <el-form-item :label="$t('firstName') + ':'" prop="firstname">
              <el-input v-model="userInfo.firstname" :disabled="!isEdit" :placeholder="$t('firstName')"></el-input>
            </el-form-item>
          </el-col>
          <el-col :span="11" :offset="2">
            <el-form-item :label="$t('lastName') + ':'" prop="lastname">
              <el-input v-model="userInfo.lastname" :disabled="!isEdit" :placeholder="$t('lastName')"></el-input>
            </el-form-item>
          </el-col>
        </el-row>
        <el-form-item v-if="role" :label="$t('country') + ':'" prop="country_code">
          <el-select class="selectInput" :disabled="!isEdit" filterable v-model="userInfo.country_code" :placeholder="$t('country')">
            <el-option v-for="country in countrys" :key="country.value" :label="country.label" :value="country.value">
              <span style="float: left">{{ country.label }}</span>
              <span style="float: right; color: #8492a6; font-size: 13px">{{ country.value }}</span>
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item v-if="role" :label="$t('phone') + ':'" prop="phone">
          <el-input :placeholder="$t('phone')" :disabled="!isEdit" type="tel" v-model="userInfo.phone">
            <template slot="prepend">{{ phonePre }}</template>
          </el-input>
        </el-form-item>
        <template v-if="userInfo.main_type == 2">
          <el-form-item :label="$t('companyName') + ':'" prop="company_name">
            <el-input v-model="userInfo.company_name" :disabled="!isEdit" :placeholder="$t('companyName')" />
          </el-form-item>
        </template>
        <p v-show="errorText" class="errorText">{{ errorText }}</p>
        <el-form-item>
          <div class="btn-wrapper">
            <el-button v-if="!isEdit && !isThird" @click="isPassword = true" plain>{{ $t("setting.changePass") }}</el-button>
            <el-button v-if="isEdit" :loading="requestIng" :disabled="requestIng" @click="updateUserInfo" type="primary">{{
              $t("setting.saveChange")
            }}</el-button>
            <el-button @click="isEdit = true" v-else-if="!isEdit" plain>{{ $t("setting.changeProfile") }}</el-button>
            <el-button v-if="isEdit" plain @click="cancel">{{ $t("cancel") }}</el-button>
          </div>
        </el-form-item>
      </el-form> -->
    </section>
  </div>
</template>

<script>
  import { mapState } from "vuex";
  import { isMobilePhone } from "validator";
  import ChangePass from "./components/changePassword.vue";
  import { validUsername } from "@/utils/validate.js";
  export default {
    components: {
      ChangePass,
    },
    data() {
      this.languageList = window.languageList;
      this.isMobilePhone = (_, value, callback) => {
        if (!value) return callback();
        // let local = this.userInfo.country_code || "zh-CN";
        if (!isMobilePhone(this.phonePre + value + "")) {
          this.errorText = "";
          callback(new Error(this.$t("phoneError")));
        } else {
          callback();
        }
      };
      this.checkFirstname = (_, val, callback) => {
        if (val && !validUsername(val.trim())) {
          callback(new Error(this.$t("register.nameError")));
        } else {
          callback();
        }
      };
      this.checkLastname = (_, val, callback) => {
        if (!validUsername(val.trim())) {
          callback(new Error(this.$t("register.nameError")));
        } else {
          callback();
        }
      };
      return {
        requestIng: false,
        userInfo: {
          firstname: "",
          lastname: "",
          country_code: "",
          // main_type: "",
          // nickname: "",
          phone: "",
          // position: "",
          // detailed_street: "",
          company_name: "",
          // company_detailed_street: "",
          // industry_code: "",
          // cloud_id: "",
          // region_id: "",
          // language: "en",
          // hasDemoData: true,
        },
        email: "",
        isEdit: false,
        isPassword: false,
        errorText: "",
      };
    },
    computed: {
      ...mapState({
        positions: state => state.profile.positions,
        industrys: state => state.profile.industrys,
        countrys: state => state.profile.countrys,
        role: state => state.app.userInfo?.role_id == "1",
      }),
      rules() {
        return {
          country_code: [
            {
              required: true,
              trigger: "blur",
              message: this.$t("country") + this.$t("requiredMessage"),
            },
          ],
          firstname: [
            {
              required: true,
              trigger: "blur",
              message: this.$t("firstName") + this.$t("requiredMessage"),
            },
            {
              validator: this.checkFirstname,
              trigger: "blur",
            },
          ],
          lastname: [
            {
              required: true,
              trigger: "blur",
              message: this.$t("lastName") + this.$t("requiredMessage"),
            },
            {
              validator: this.checkLastname,
              trigger: "blur",
            },
          ],
          phone: [{ validator: this.isMobilePhone, trigger: "blur" }],
          company_name: [
            {
              required: true,
              trigger: "blur",
              message: this.$t("companyName") + this.$t("requiredMessage"),
            },
          ],
          password: [
            {
              required: true,
              trigger: "blur",
              message: this.$t("password") + this.$t("requiredMessage"),
            },
            { validator: this.checkPassword, trigger: "blur" },
          ],
          confirm: [
            {
              required: true,
              trigger: "blur",
              message: this.$t("confirmPass") + this.$t("requiredMessage"),
            },
            { validator: this.cheakConfirmPassword, trigger: "blur" },
          ],
        };
      },
      phonePre() {
        return this.countrys.find(item => item.value == this.userInfo.country_code)?.dialing || "+86";
      },
      countryName() {
        return this.countrys.find(item => item.value == this.userInfo.country_code)?.label;
      },
      isThird() {
        return this.$store.state.app.userInfo?.origin;
      },
    },
    watch: {
      // "$store.state.app.userInfo": {
      //   handler(newval) {
      //     // 此处避免修改全局的信息
      //     Object.keys(this.userInfo).forEach(item => {
      //       this.userInfo[item] = newval[item] || "";
      //     });
      //     // this.userInfo.company_detailed_street = newval.address.detailed_street;
      //     this.userInfo.company_name = newval.company?.company_name;
      //     // this.userInfo.hasDemoData = true;
      //     // this.userInfo.industry_code = newval.address.industry_code;
      //     // TODO: 添加密码和确认密码
      //     this.email = newval.email;
      //   },
      //   deep: true,
      //   immediate: true,
      // },
    },
    methods: {
      updateUserInfo() {
        if (this.requestIng) return;
        this.$refs.userInfo.validate(async valid => {
          if (valid) {
            this.requestIng = true;
            this.errorText = "";
            this.userInfo.lastname = this.userInfo.lastname.trim();
            this.userInfo.firstname = this.userInfo.firstname.trim();
            await this.$store
              .dispatch("profile/putUserInfo", this.userInfo)
              .then(() => {
                this.$message.success(this.$t("login.changeSucc"));
                this.isEdit = false;
              })
              .catch(err => {
                this.errorText = err.msg + "：" + (err.data?.map(item => item.field).join(",") || "");
              });
            this.requestIng = false;
          }
        });
      },
      languageChange(lang) {
        this.$i18n.locale = lang;
        this.$store.commit("SET_LANGUAGE", lang);
      },
      cancel() {
        this.isEdit = false;
        this.errorText = "";
        this.$refs.userInfo.resetFields();
      },
    },
  };
</script>

<style lang="scss" scoped>
  .profile {
    // height: auto;
    &::v-deep .el-form-item--small.el-form-item {
      margin-bottom: 10px;
    }
    &::v-deep .edit .el-form-item--small.el-form-item {
      margin-bottom: 20px;
    }
    &::v-deep .el-input.is-disabled .el-input__inner {
      color: #16191f;
    }
  }

  .selectInput {
    width: 100%;
  }
  .formStyle {
    width: 600px;
    &::v-deep .el-form-item__label {
      font-weight: 500;
    }
  }
  .btn-wrapper {
    margin-top: 10px;
  }
</style>
