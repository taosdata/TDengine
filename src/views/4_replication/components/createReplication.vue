<template>
  <el-form ref="form" label-position="left" label-width="auto" :disabled="requestIng" :model="info">
    <el-form-item :label="$t('replication.fromDB')" prop="from_db" required>
      <el-select class="w100" v-model="info.from_db" :placeholder="$t('replication.fromDBTip')">
        <el-option v-for="item in currentInstanceDBList" :key="item.name" :label="item.name" :value="item.name"></el-option>
      </el-select>
    </el-form-item>
    <el-form-item :label="$t('replication.toInstanceToken')" prop="to_token" required>
      <el-input v-model="info.to_token" :placeholder="$t('replication.toInstanceTokenTip')" @change="getTargetDBList()"></el-input>
    </el-form-item>
    <el-form-item :label="$t('replication.toDB')" prop="to_db" required>
      <el-select
        class="w100"
        v-model="info.to_db"
        :disabled="!info.to_token || !destinationInstanceDBList.length"
        :placeholder="$t('replication.toDBTip')"
        :default-first-option="true"
        filterable
        :loading="requestIng"
      >
        <el-option v-for="item in destinationInstanceDBList" :key="item.name" :label="item.name" :value="item.name"></el-option>
      </el-select>
    </el-form-item>
    <el-form-item label=" ">
      <el-button class="w100" :disabled="createBtnDisabled" type="primary" @click="create">{{ $t("create") }}</el-button>
    </el-form-item>
  </el-form>
</template>

<script>
  import { getTargetDBList, createTask } from "@/api/replication";
  export default {
    props: {},
    data() {
      return {
        info: {
          from_db: "",
          to_token: "",
          to_db: "",
        },
        destinationInstanceDBList: [],
        requestIng: false,
      };
    },
    computed: {
      createBtnDisabled() {
        return !this.info.from_db || !this.info.to_token || !this.info.to_db;
      },
      currentInstanceDBList() {
        return this.$store.state.replication.dbList;
      },
      appId() {
        return this.$store.getters.appId;
      },
    },
    created() {
      this.$store.dispatch("replication/getDBList");
    },
    methods: {
      create() {
        if (this.requestIng) return;
        this.$refs.form.validate(valid => {
          if (valid) {
            this.requestIng = true;
            createTask(this.info, this.appId)
              .then(() => {
                this.$store.dispatch("replication/getTaskList");
                this.$emit("close");
                this.$message.success(this.$t("createSucc"));
              })
              .finally(() => {
                this.requestIng = false;
              });
          }
        });
      },
      getTargetDBList() {
        if (this.requestIng) return;
        this.requestIng = true;
        getTargetDBList(this.info.to_token)
          .then(data => {
            this.destinationInstanceDBList = data;
          })
          .catch(() => {
            this.destinationInstanceDBList = [];
          })
          .finally(() => {
            this.requestIng = false;
          });
      },
    },
  };
</script>

<style></style>
