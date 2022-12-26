<template>
  <div class="payment-email">
    <p>{{ $t("billing.paymentEmailTip") }}</p>
    <section class="email-content">
      <ul class="email-list">
        <li class="email-item">
          <a :href="'mailto:' + accountEmail" class="email">{{ accountEmail }}</a>
          <el-tag style="flex-shrink: 0" size="mini" type="info">{{ $t("billing.acctountEmail") }}</el-tag>
        </li>
        <li v-for="item in emailList" :key="item.billEmailId" class="email-item">
          <a :href="'mailto:' + item.email" class="email">{{ item.email }}</a>
          <section class="opt-btn">
            <el-button @click="copy(item.email)" icon="el-icon-copy-document" size="mini"></el-button>
            <el-button :disabled="requestIng" @click="del(item)" icon="el-icon-delete" size="mini"></el-button>
          </section>
        </li>
        <li class="add-block">
          <el-button class="w100" size="mini" @click="addEmail" icon="el-icon-plus">{{ $t("billing.addNewEmail") }}</el-button>
        </li>
      </ul>
    </section>
    <el-dialog align="center" :title="$t('billing.addNewEmail')" width="400px" :visible.sync="dialog">
      <el-form ref="form" size="small" :rules="rule" label-position="left" label-width="auto" :model="createInfo">
        <el-form-item :label="$t('email')" prop="email">
          <el-input v-model="createInfo.email"></el-input>
        </el-form-item>
        <el-form-item>
          <el-row>
            <el-col :span="11">
              <el-button class="w100" size="small" @click="dialog = false">{{ $t("cancel") }}</el-button>
            </el-col>
            <el-col :span="11" :offset="2">
              <el-button class="w100" size="small" type="primary" @click="add">{{ $t("confirm") }}</el-button>
            </el-col>
          </el-row>
        </el-form-item>
      </el-form>
    </el-dialog>
  </div>
</template>

<script>
  import { validEmail } from "@/utils/validate";
  import { getPaymentEmail, deletePaymentEmail, addPaymentEmail } from "api/billing";
  import { copy } from "@/utils";
  export default {
    data() {
      this.validEmail = (_, val, callback) => {
        if (!validEmail(val)) {
          callback(new Error(this.$t("emailError")));
        } else {
          callback();
        }
      };
      return {
        dialog: false,
        createInfo: {
          email: "",
        },
        requestIng: false,
        rule: {
          email: [{ validator: this.validEmail, trigger: "blur" }],
        },
        emailList: [],
      };
    },
    computed: {
      accountEmail() {
        return this.$store.getters.userInfo.email;
      },
    },
    created() {
      this.getData();
    },
    methods: {
      addEmail() {
        this.dialog = true;
      },
      getData() {
        if (this.requestIng) return;
        this.requestIng = true;
        getPaymentEmail()
          .then(res => {
            this.emailList = res;
          })
          .finally(() => {
            this.requestIng = false;
          });
      },
      del(data) {
        if (this.requestIng) return;
        this.$confirm(this.$t("billing.delEmail") + ":" + data.email + "?", this.$t("tips"), {
          confirmButtonText: this.$t("confirm"),
          cancelButtonText: this.$t("cancel"),
          type: "warning",
        }).then(async () => {
          this.requesting = true;
          deletePaymentEmail(data.billEmailId)
            .then(() => {
              this.$message.success(this.$t("delSucc"));
            })
            .finally(() => {
              this.requestIng = false;
              this.getData();
            });
        });
      },
      add() {
        if (this.requestIng) return;
        this.$refs.form.validate(async valid => {
          if (valid) {
            this.requestIng = true;
            addPaymentEmail(this.createInfo)
              .then(() => {
                this.dialog = false;
                this.$message.success(this.$t("addSucc"));
                this.createInfo = {
                  email: "",
                };
              })
              .finally(() => {
                this.requestIng = false;
                this.getData();
              });
          }
        });
      },
      copy(text) {
        copy(text);
      },
    },
  };
</script>

<style lang="scss" scoped>
  .payment-email {
  }
  .add-block {
    margin: 10px 0;
    text-align: center;
  }
  .num {
    font-size: 20px;
    font-weight: bold;
    line-height: 30px;
  }
  .invoice-card {
    margin: 20px 0;
  }
  .custom-card {
    // margin-top: 20px;
    height: 100%;
  }
  .title {
    font-size: 18px;
    margin-bottom: 10px;
  }
  .email-content {
    width: 500px;
    margin: 20px 0;
  }
  .email-list {
    .email-item {
      padding: 10px;
      @extend .flexBetween;
      border: 1px solid $divider-color;
      border-left: none;
      border-right: none;
      & + .email-item {
        border-top: none;
      }
      .email {
        flex: 1;
        @extend .nowrap;
      }
      .opt-btn {
        flex-shrink: 0;
      }
    }
  }
</style>
