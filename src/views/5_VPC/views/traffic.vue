<template>
  <div>
    <section class="desc">
      <p>{{ $t("vpc.limitTip") }}</p>
      <p>{{ $t("vpc.configTip") }}</p>
    </section>
    <section class="flexEnd">
      <section class="add-btn">
        <el-button class="big-button" :disabled="cannotAdd" size="small" @click="dialog = true" icon="el-icon-plus" plain>{{ $t("add") }}</el-button>
      </section>
    </section>
    <el-table size="mini">
      <el-table-column :label="$t('vpc.ipAddress') + '*'" prop="ip"> </el-table-column>
      <el-table-column :label="$t('vpc.desc')" prop="desc"> </el-table-column>
      <el-table-column fixed="right" width="30px">
        <template slot-scope="scope">
          <el-button icon="el-icon-delete" size="small" plain type="danger" @click="handleDeleteToken(scope.row)"></el-button>
        </template>
      </el-table-column>
    </el-table>
    <p v-if="cannotAdd" @click="upgrade" class="default-tip" v-html="$t('vpc.upgradeTip')"></p>
    <el-dialog :visible.sync="dialog" width="500px">
      <el-form size="small" ref="createForm" :model="trafficForm">
        <el-form-item :label="$t('vpc.ipAddress')" prop="ip" required>
          <el-input placeholder="192.168.1.1" v-model="trafficForm.ip"></el-input>
        </el-form-item>
        <el-form-item :label="$t('vpc.desc')">
          <el-input placeholder="" v-model="trafficForm.desc"></el-input>
        </el-form-item>
        <el-form-item>
          <el-row>
            <el-col :span="11">
              <el-button @click="dialog = false" class="w100">{{ $t("cancel") }}</el-button>
            </el-col>
            <el-col :span="11" :offset="1">
              <el-button class="w100" type="primary">{{ $t("confirm") }}</el-button>
            </el-col>
          </el-row>
        </el-form-item>
      </el-form>
    </el-dialog>
  </div>
</template>

<script>
  export default {
    data() {
      return {
        trafficForm: {
          ip: "",
          desc: "",
        },
        dialog: false,
      };
    },
    computed: {
      cannotAdd() {
        return !this.$store.getters.currentServerLevel;
      },
    },
    methods: {
      handleDeleteToken() {},
      upgrade(e) {
        if (e.target.tagName == "A") {
          this.$store.commit("SET_UPGRADE_DIALOG_VISIBLE", true);
        }
      },
    },
  };
</script>

<style lang="scss" scoped>
  .desc {
    line-height: 40px;
    font-size: 16px;
  }
  .default-rule {
    font-size: 14px;
    font-weight: bold;
    margin: 20px 0;
  }
  .filter-header {
    font-size: 16px;
    font-weight: bold;
  }
  .filter-content {
    margin-top: 20px;
  }
  .TokenCard_NameDelIcon {
    font-size: 20px;
    color: #606266;
    cursor: pointer;
    &:hover {
      color: $color-primary;
    }
  }
  .btn-list {
    line-height: 40px;
  }
  .add-btn,
  .update-btn {
    text-align: right;
  }
  .update-btn {
    margin-top: 20px;
  }
</style>
