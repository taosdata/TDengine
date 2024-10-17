<template>
  <div class="user-select-wrapper">
    <el-select
      class="w100"
      clearable
      :placeholder="placeholder"
      @change="change"
      :loading="loading"
      collapse-tags
      :multiple="multiple"
      v-model="selectValue"
    >
      <el-option v-for="item in options" :key="item[id]" :label="item[displayField]" :value="item[id]"> </el-option>
    </el-select>
  </div>
</template>

<script>
  import {getOrganizationUser, getAppUser ,getOrganizationGroupResource, getInstanceGroupResource, getDatabaseGroupResource,getOrganizationResource, getInstanceResource, getDBResource 
  ,getGroupList, getInstanceGroup} from "@/api/gateway/data/dbs";
  import { loadPageData } from "@/utils";
  export default {
    props: {
      type: {
        type: String,
        default: "user",
      },
      value: {
        type: [Array, String],
        default: () => [],
      },
      filterList: {
        type: Array,
        default: () => [],
      },
      level: {
        type: Number,
        default: 0,
      },
      dialog: {
        type: Boolean,
        default: false,
      },
      multiple: {
        type: Boolean,
        default: false,
      },
    },
    components: {},
    data() {
      return {
        currentPage: 1,
        total: 0,
        pageSize: 100,
        loading: false,
        tempSelected: [],
        allData: [],
        selectValue: "",
      };
    },
    computed: {
      placeholder() {
        return {
          user: this.$t("accessControl.chooseUserAsTemplate"),
          group: this.$t("accessControl.chooseGroupAsTemplate"),
        }[this.type];
      },
      options() {
        if (!this.filterList.length) return this.allData;
        return this.allData.filter(item => this.filterList.every(ite => ite[this.id] != item[this.id]));
      },
      category() {
        return {
          user: 1,
          group: 2,
        }[this.type];
      },
      resourceFn() {
        return {
          user: [getOrganizationResource, getInstanceResource, getDBResource],
          group: [getOrganizationGroupResource, getInstanceGroupResource, getDatabaseGroupResource],
        }[this.type][this.level];
      },
      displayField() {
        return {
          user: "email",
          group: "group_name",
        }[this.type];
      },
      id() {
        return {
          user: "userId",
          group: "id",
        }[this.type];
      },
      dataFn() {
        return {
          user: [getOrganizationUser, getAppUser],
          group: [getGroupList, getInstanceGroup],
        }[this.type][this.level];
      },
    },
    watch: {
      type: {
        handler() {
          this.currentPage = 1;
          this.total = 0;
          this.pageSize = 100;
          this.getOptions();
        },
        immediate: true,
      },
    },
    mounted() {},
    methods: {
      loadmore() {
        if (this.loading) return;
        if (this.total && this.total <= this.options.length) return;
        this.currentPage++;
        this.getOptions();
      },

      getOptions() {
        if (this.loading) return;
        this.loading = true;
        loadPageData(this.dataFn)
          .then(data => {
            this.allData = data;
          })
          .catch(() => {
            this.allData = [];
          })
          .finally(() => {
            this.loading = false;
          });
      },
      handlePageChange(page) {
        this.currentPage = page;
        this.getOptions();
      },
      async change(val) {
        if (!val) return this.$emit("change", []);
        if (this.requesting) return;
        this.requesting = true;
        const data = await this.resourceFn(val).catch(() => []);
        this.handleResource(data);
        this.$emit("change", this.handleResource(data));
        this.requesting = false;
      },
      handleResource(resource) {
        if (this.type == "user") {
          resource = resource.filter(item => !item.groupId && item.roleId != "1");
        }
        return resource.map(item => {
          const result = {
            role_id: item.roleId,
            expiration: item.expiration,
          };
          if (item.instanceId && item.instanceId !== "-1") {
            result.instance_id = item.instanceId;
          }
          if (item.databaseId && item.databaseId !== "-1") {
            result.database_id = item.databaseId;
          }
          return result;
        });
      },
      handleCheckboxValue(row) {
        return this.tempSelected.some(predicate => predicate[this.id] === row[this.id]);
      },
      checkboxChange(val, row) {
        if (val) {
          this.tempSelected.push(row);
        } else {
          this.tempSelected = this.tempSelected.filter(item => item[this.id] !== row[this.id]);
        }
      },
    },
  };
</script>

<style scoped lang="scss">
  .user-select-wrapper {
    width: 100%;
    display: flex;
    .select-list {
      flex: 1;
    }
    .operate-btn {
      margin-left: 10px;
    }
  }
</style>
