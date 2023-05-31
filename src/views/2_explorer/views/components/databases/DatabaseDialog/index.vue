<template>
  <el-dialog
    :title="dialogTitle"
    :visible.sync="dialogFormVisible"
    :before-close="handleCloseDialog"
  >
    <el-form
      size="small"
      label-width="100px"
      class="form_style"
      ref="dbForm"
      :rules="rules"
      :model="db_form"
    >
      <el-form-item label="Name" prop="name">
        <span slot="label" style="margin-right: 15px">
          {{ $t("data.name") }}
        </span>
        <el-input
          v-model="db_form.name"
          class="form_item"
          :disabled="formStatus == 'update'"
        />
      </el-form-item>
      <el-form-item>
        <span slot="label">
          {{ $t("data.keep") }}
          <el-tooltip placement="bottom" effect="light">
            <div slot="content" v-html="$t('data.keepTip')"></div>
            <Icon name="info" class="lableTips_icon"></Icon>
          </el-tooltip>
        </span>
        <el-input-number
          size="small"
          class="form_item"
          controls-position="right"
          v-model="db_form.keep"
          :min="keepMin"
          :max="3650"
        >
        </el-input-number>
      </el-form-item>
      <el-form-item>
        <span slot="label">
          {{ $t("data.update") }}
          <el-tooltip placement="bottom" effect="light">
            <div slot="content" v-html="$t('data.updateTip')"></div>
            <Icon name="info" class="lableTips_icon"></Icon>
          </el-tooltip>
        </span>
        <el-input-number
          size="small"
          controls-position="right"
          class="form_item"
          v-model="db_form.update"
          :min="0"
          :max="2"
        >
        </el-input-number>
      </el-form-item>
      <div class="moreConfText" @click="toDbCreatePage">
        <i class="el-icon-setting moreConfIcon"></i>{{ $t("data.moreConfig") }}
      </div>
      <el-form-item class="confirm_line" size="medium">
        <el-button @click="handleCloseDialog">{{ $t("cancel") }}</el-button>
        <el-button type="primary" @click="onSubmit">{{
          $t("confirm")
        }}</el-button>
      </el-form-item>
    </el-form>
  </el-dialog>
</template>

<script>
import { mapState } from "vuex";
import Icon from "@/components/Icon/index";

export default {
  components: { Icon },
  data() {
    return {};
  },
  computed: {
    ...mapState({
      dialogFormVisible: state => state.dbs.dialogFormVisible,
      db_form: state => state.dbs.db_form,
      formStatus: state => state.dbs.formStatus
    }),
    dialogTitle() {
      if (this.formStatus == "create") {
        return this.$t("data.createDatabase");
      } else {
        return this.$t("data.editDatabase");
      }
    },
    rules() {
      return {
        name: [
          {
            required: true,
            message: this.$t("data.nameTip").replace('/name/',this.$t('data.databases')),
            trigger: "blur"
          }
        ]
      };
    },
    keepMin() {
      return Number(this.db_form.days) > 30 ? Number(this.db_form.days) : 30;
    }
  },
  methods: {
    handleCloseDialog() {
      this.$store.commit("dbs/HANDLE_CLOSE_DIALOG");
    },
    toDbCreatePage() {
      this.$store.commit("dbs/HANDLE_CLOSE_DIALOG");
      this.$store.commit("console/SET_TAB_NAME", this.$t("add"));
      this.$store.state.console.partActive = "detail";
      this.$store.state.console.currentComponent = "DatabaseCreate";
    },
    onSubmit() {
      this.$refs["dbForm"].validate(valid => {
        if (valid) {
          this.$store
            .dispatch("dbs/createDatabase", false)
            .then(() => {
              this.$message.success(this.$t("createSucc"));
            })
            .catch(() => {
              this.$message.error(this.$t("data.checkFail"));
            });
        }
      });
    }
  }
};
</script>

<style lang="scss" scoped>
.form_style {
  padding-left: 20px;
  padding-right: 60px;
}

.form_item {
  width: 100%;
  overflow: hidden;
}

.confirm_line {
  margin-top: 20px;
  display: flex;
  justify-content: flex-end;
}

.lableTips_icon {
  width: 16px;
  height: 16px;
  color: #bfbfbf;
  top: 3px;
  position: relative;
  cursor: pointer;
}

.moreConfText {
  color: #1652f0;
  margin-top: 30px;
  text-align: left;
  margin-left: 15px;
  cursor: pointer;
}

.moreConfIcon {
  margin-right: 6px;
}
</style>
