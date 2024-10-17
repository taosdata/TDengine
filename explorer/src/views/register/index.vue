<template>
  <div class="register">
    <div class="register-content">
      <h2 class="step-title">{{ $t("register.beforeUse") }}</h2>
      <el-form
        ref="registerForm"
        label-position="left"
        label-width="200px"
        :model="info"
        :rules="rules"
        :style="{ width: formWidth, margin: 'auto' }"
      >
        <section class="form-content">
          <el-form-item>
            <title-com :title="$t('register.basicInfo')"> </title-com>
          </el-form-item>
          <el-form-item :label="$t('fullName')" prop="username">
            <el-input v-model="info.username" :placeholder="$t('fullName')" />
          </el-form-item>
          <el-form-item :label="$t('country')" prop="country_code" required>
            <el-select v-model="info.country_code" filterable :placeholder="$t('country')">
              <el-option v-for="item in countryList" :key="item.value" v-bind="item">
                <span style="float: left">{{ item.label }}</span>
                <span style="float: right; color: #8492a6; font-size: 13px">{{ item.value }}</span>
              </el-option>
            </el-select>
          </el-form-item>
          <!-- <el-form-item :label="$t('language')">
              <el-select
                v-model="info.language"
                :placeholder="$t('language')"
                @change="languageChange"
              >
                <el-option
                  v-for="item in languageList"
                  :key="item.value"
                  v-bind="item"
                ></el-option>
              </el-select>
            </el-form-item> -->
          <el-form-item :label="$t('phone')" prop="phone" :rules="[{ validator: isMobilePhone, trigger: 'blur' }]">
            <el-input v-model="info.phone" :placeholder="$t('phone')">
              <template slot="prepend">{{ currentCountryCode }}</template>
            </el-input>
          </el-form-item>
          <!-- <el-form-item :label="$t('industry')">
              <el-select
                v-model="info.industry_type"
                :placeholder="$t('industry')"
              >
                <el-option
                  v-for="item in professionList"
                  :key="item.value"
                  v-bind="item"
                ></el-option>
              </el-select>
            </el-form-item>
            <el-form-item :label="$t('position')">
              <el-select v-model="info.position" :placeholder="$t('position')">
                <el-option
                  v-for="item in positionList"
                  :key="item.value"
                  v-bind="item"
                ></el-option>
              </el-select>
            </el-form-item> -->

          <template v-if="info.main_type == 2">
            <el-form-item :label="$t('companyName')" required>
              <el-input v-model="info.company_name" :placeholder="$t('companyName')" />
            </el-form-item>
          </template>
          <el-form-item :label="$t('password')" prop="password" required>
            <el-popover trigger="click" popper-class="password-tip" placement="right-end">
              <div v-html="$t('passwordTip')"></div>
              <el-input
                slot="reference"
                v-model.trim="info.password"
                maxlength="16"
                show-password
                minlength="8"
                :placeholder="$t('password')"
              ></el-input>
            </el-popover>
          </el-form-item>
          <el-form-item :label="$t('confirmPass')" prop="confirm" required>
            <el-input v-model.trim="info.confirm" show-password maxlength="16" minlength="8" :placeholder="$t('confirmPass')"></el-input>
          </el-form-item>
          <!-- <el-form-item :label="$t('location')">
              <el-input
                v-if="info.main_type == 2"
                v-model="info.company_detailed_street"
                :placeholder="$t('location')"
              />
              <el-input
                v-else
                v-model="info.detailed_street"
                :placeholder="$t('location')"
              ></el-input>
            </el-form-item> -->

          <!-- <el-form-item>
              <el-checkbox v-model="isRead">
                <span v-html="agreement"></span>
              </el-checkbox>
            </el-form-item> -->
        </section>
        <section class="form-content">
          <el-form-item>
            <title-com :title="$t('clusterInfo')"> </title-com>
          </el-form-item>
          <el-form-item :label="$t('clusterName')" prop="cluster_name" required>
            <el-input v-model.trim="info.cluster_name" maxlength="16" :placeholder="$t('clusterName')"></el-input>
          </el-form-item>
          <el-form-item :label="$t('register.demoData')">
            <el-checkbox v-model="info.hasDemoData">{{ $t("register.demoDataText") }} </el-checkbox>
          </el-form-item>
          <el-form-item :label="$t('register.CR')" required>
            <el-select v-model="info.cloud_id" :placeholder="$t('dashboard.cloud')" @change="() => (info.region_id = '')" style="width: 48%">
              <el-option v-for="item in cloudList" :key="item.value" v-bind="item" />
            </el-select>
            <el-select v-model="info.region_id" :placeholder="$t('dashboard.region')" style="width: 48%; margin-left: 4%">
              <el-option v-for="item in regionList" :key="item.value" v-bind="item" />
            </el-select>
          </el-form-item>
          <p class="errorText" v-show="errorText">{{ errorText }}</p>
        </section>
      </el-form>
      <div class="continue-btn">
        <el-button :disabled="requestIng" style="font-size: 16px" :style="{ width: '455px', marginLeft: '207px' }" @click="submit" type="primary">{{
          $t("continue")
        }}</el-button>
      </div>
    </div>
  </div>
</template>

<script>
  import titleCom from "./components/title.vue";
  import * as $api from "@/api/register";
  import { debounce } from "@/utils";
  import { isMobilePhone } from "validator";
  import { validPassword, validUsername } from "@/utils/validate";
  export default {
    components: { titleCom },
    data() {
      this.languageList = window.languageList;
      this.agreementUrl = process.env.VUE_APP_AGREEMENT_URL;
      this.isMobilePhone = (_, value, callback) => {
        if (!value) return callback();
        // let local = this.userInfo.country_code || "zh-CN";
        if (!isMobilePhone(this.currentCountryCode + value + "")) {
          callback(new Error(this.$t("phoneError")));
        } else {
          callback();
        }
      };
      this.checkPassword = async (_, value, callback) => {
        if (!validPassword(value)) {
          return callback(new Error(this.$t("passwordError")));
        }
      };
      this.cheakConfirmPassword = async (_, value, callback) => {
        if (value != this.info.password || !value) return callback(new Error(this.$t("twoPassError")));
      };
      this.validUsername = (_, val, callback) => {
        if (!validUsername(val)) {
          callback(new Error(this.$t("register.nameError")));
        } else {
          callback();
        }
      };
      return {
        formWidth: "700px",
        info: {
          language: "en",
          username: "",
          nickname: "",
          sex: 1,
          phone: "",
          country_code: "",
          position: "",
          main_type: "2",
          detailed_street: "",
          company_name: "",
          company_detailed_street: "",
          industry_code: "",
          cloud_id: "",
          region_id: "",
          cluster_name: "",
          hasDemoData: true,
        },
        loading: false,
        requestIng: false,
        countryList: [],
        professionList: [],
        positionList: [],
        isRead: false,
        cloudList: [],
        errorText: "",
      };
    },
    created() {
      this.info.username = this.$store.state.app.userInfo.username;
      this.getCountryList();
      // this.getProfession();
      // this.getPosition();
      this.getCloud();
    },
    computed: {
      currentCountryCode() {
        return this.countryList.find(item => item.value == this.info.country_code)?.dialing || "+86";
      },
      rules() {
        return {
          username: [
            {
              required: true,
              validator: this.validUsername,
              trigger: "blur",
            },
          ],
          password: [{ validator: this.checkPassword, trigger: "blur" }],
          confirm: [{ validator: this.cheakConfirmPassword, trigger: "blur" }],
        };
      },
      regionList() {
        return this.cloudList.find(item => item.value == this.info.cloud_id)?.regions || [];
      },
    },
    methods: {
      // 获取国家列表
      async getCountryList() {
        let result = await $api.getCountryList().catch(() => []);
        let country = this.$store.state.language == "en" ? "US" : "CN";
        let countryIndex = result.findIndex(item => item.value == country);
        country = result[countryIndex];
        result.splice(countryIndex, 1);
        result.unshift(country);
        this.countryList = result;
        this.info.country_code = this.countryList[0].value;
      },
      // 获取行业信息
      async getProfession() {
        this.professionList = await $api.getProfessionList().catch(() => ({ data: [] }));
      },
      async getPosition() {
        this.positionList = await $api.getPositionList().catch(() => []);
      },
      async getCloud() {
        this.cloudList = await $api.getCloudRegion().catch(() => []);
        // 默认选中第一项
        if (this.cloudList.length > 0) {
          let data = this.cloudList[0];
          this.info.cloud_id = data.value;
          this.info.region_id = data.regions[0]?.value;
        }
      },
      languageChange(lang) {
        this.$i18n.locale = lang;
        this.$store.commit("SET_LANGUAGE", lang);
      },
      submit: debounce(
        function () {
          if (this.requestIng) return;
          this.$refs.registerForm.validate(async valid => {
            if (valid) {
              this.requestIng = true;
              let data = await $api.putUserInfo(this.info).catch(err => {
                if (err.data && err.data.length) {
                  this.errorText = err.data.map(item => item.field).join(",") + ":" + this.$t("formatWrong");
                } else {
                  this.errorText = err.msg;
                }
                return false;
              });
              if (data !== false) {
                //完善后更新一下现在的信息
                await this.$store.dispatch("app/getUserInfo");
                this.$router.push({
                  path: "/instanceStatus",
                  query: {
                    region: this.regionList.find(item => item.value == this.info.region_id)?.label,
                    cloud: this.cloudList.find(item => item.value == this.info.cloud_id)?.label,
                    clusterName: this.info.cluster_name,
                  },
                });
              }
            }
            this.requestIng = false;
          });
        },
        2000,
        true
      ),
    },
  };
</script>

<style lang="scss" scoped>
  .register {
    background-color: #f9fbfa;
    width: 100vw;
    min-height: 100vh;
    .register-content {
      @include content-padding;
      width: 1200px;
      margin: auto;
    }
    .step-title {
      font-size: 24px;
      font-weight: normal;
      text-align: center;
      margin-top: 30px;
      // color: $color-primary;
    }
  }
  .content ::v-deep .el-card {
    margin-top: 10px;
  }
  .form-content {
    padding: 10px 26px;
  }
  .form-content ::v-deep .el-select {
    width: 100%;
  }
  .form-content ::v-deep .el-form-item {
    margin-bottom: 22px;
  }
  .form-content ::v-deep .el-form-item__label {
    font-weight: normal;
  }
  // .el-dropdown-link {

  // }
  .custom-class ::v-deep .el-input {
    font-size: 20px;
    font-weight: bold;
  }
  .is-used {
    font-size: 18px;
    font-weight: bold;
    display: flex;
    align-items: center;
    h1 {
      margin-right: 20px;
    }
  }
  .continue-btn {
    // margin-top: 30px;
    text-align: center;
  }
</style>
<style lang="scss">
  a.agreement {
    color: $color-primary;
    text-decoration: underline !important;
  }
</style>
