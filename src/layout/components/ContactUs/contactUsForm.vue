<template>
  <el-form class="contactUsForm" ref="contactUsForm" :model="submitForm" label-width="0px">
    <el-form-item prop="name" required>
      <el-input v-model="submitForm.name" :placeholder="$t('name')" style="width: 100%"></el-input>
    </el-form-item>
    <el-form-item prop="company" required>
      <el-input v-model="submitForm.company" :placeholder="$t('companyName')" style="width: 100%"></el-input>
    </el-form-item>
    <el-form-item prop="email" required>
      <el-input v-model="submitForm.email" :placeholder="$t('email')" style="width: 100%"></el-input>
    </el-form-item>
    <el-form-item prop="phone" required>
      <el-input v-model="submitForm.phone" :placeholder="$t('phone')" style="width: 100%"></el-input>
    </el-form-item>
    <el-form-item prop="category" required>
      <el-select v-model="submitForm.category" placeholder="" style="width: 100%">
        <el-option v-for="item in category" :key="item" :value="item"></el-option>
      </el-select>
    </el-form-item>
    <el-form-item prop="message" required>
      <el-input v-model="submitForm.message" type="textarea" :placeholder="$t('footerComp.typeMeg')" style="width: 100%"></el-input>
    </el-form-item>
    <el-form-item>
      <div class="flexCenter">
        <el-button :loading="requestIng" @click="subMit" type="primary">{{ $t("submit") }}</el-button>
      </div>
    </el-form-item>
  </el-form>
</template>

<script>
  import { sendEmail } from "@/api/footer";
  import { validEmail } from "@/utils/validate.js";

  var checkEmail = async (_, value, callback) => {
    if (!value || !validEmail(value)) {
      return callback(new Error(this.$t("emailError")));
    }
  };
  export default {
    data() {
      this.category = [
        "Cloud service partner",
        "Enterprise Edition Consulting",
        "Channel partner",
        "Integration & Technology Partners",
        "OEM partner",
      ];
      return {
        submitForm: {
          name: "",
          company: "",
          email: "",
          phone: "",
          category: "Cloud service partner",
          message: "",
          sucessMsg: "Successfully contacted sales",
          flag: "sale",
        },
        requestIng: false,
      };
    },
    computed: {
      rules() {
        return {
          email: [{ validator: checkEmail, trigger: "blur" }],
        };
      },
    },
    methods: {
      subMit() {
        if (this.requestIng) return;
        this.$refs.contactUsForm.validate(valid => {
          if (valid) {
            this.requestIng = true;
            let postData = {
              from: "support@taosdata.com",
              fromname: this.submitForm.email,
              to: "jhtao@taosdata.com",
              subject: "Contact Sales",
              message: Object.keys(this.submitForm).reduce((pre, cur) => {
                return pre.replace(`{${cur}}`, this.submitForm[cur]);
              }, this.$t("footerComp.toCompanyMegTemp")),
              category: this.submitForm.category,
              successmsg: "Successfully contacted sales",
              errormsg: "Apologies, unable to contact sales at the time",
            };
            let formData = new FormData();
            for (let key in postData) {
              formData.append(key, postData[key]);
            }

            sendEmail(formData)
              .then(data => {
                if (data[0].status == "success") {
                  let postData = {
                    from: "support@taosdata.com",
                    fromname: "TAOS Data Support",
                    to: this.submitForm.email,
                    subject: "Contact Sales Confirmation",
                    message: this.$t("footerComp.messageTemp").replace("{msg}", this.submitForm.message),
                  };
                  let formData = new FormData();
                  for (let key in postData) {
                    formData.append(key, postData[key]);
                  }
                  return sendEmail(formData).then(() => {
                    this.$message.success(this.$t("sendSucc"));
                    this.$emit("close");
                  });
                }
              })
              .finally(() => (this.requestIng = false));
          }
        });
      },
    },
  };
</script>
<style lang="scss">
  .contactUsForm .el-form-item {
    margin-bottom: 20px;
  }
</style>
