<template>
  <div class="user-select-wrapper">
    <el-table size="mini" :data="options" border class="w100">
      <el-table-column label="" width="55" align="center">
        <template slot-scope="{ row }">
          <el-checkbox
            :value="handleCheckboxValue(row) || getIsAdded(row)"
            :disabled="isDisabled(row)"
            @change="checkboxChange($event, row)"
          ></el-checkbox>
        </template>
      </el-table-column>
      <el-table-column v-for="item in ColumnConfig" :key="item.id" v-bind="item" :label="$t(item.label)"></el-table-column>
      <!-- <el-table-column :label="$t('status')" width="100" prop="status">
          <template slot-scope="{ row }">
            <el-tag size="mini" :type="UserStatusTag[row.status]">{{ row.status }}</el-tag>
          </template>
        </el-table-column> -->
    </el-table>
    <el-pagination
      class="pagination"
      layout="total, prev, pager, next"
      :current-page="currentPage"
      :page-size="pageSize"
      :hide-on-single-page="true"
      :total="total"
      @current-change="handlePageChange"
    >
    </el-pagination>
    <p class="simple-tip">{{ $t("accessControl.addGroupUserTip") }}</p>
    <div class="flexEnd">
      <el-button size="small" @click="cancel">{{ $t("cancel") }}</el-button>
      <el-button :disabled="!tempSelected.length" v-permission="'group-role:grant'" size="small" type="primary" @click="handleConfirm">{{
        $t("confirm")
      }}</el-button>
    </div>
  </div>
</template>

<script>
  import { getAppUser,getOrganizationUser,getGroupList,getInstanceGroup } from "@/api/gateway/data/dbs";
 
  import ColumnConfig from "./columnConfig";
  import { UserStatusTag } from "@/const";
  export default {
    props: {
      type: {
        type: String,
        default: "user",
      },
      selected: {
        type: Array,
        default: () => [],
      },
      level: {
        type: Number,
        default: 0,
      },
    },
    components: {},
    data() {
      this.UserStatusTag = UserStatusTag;
      return {
        options: [],
        currentPage: 1,
        total: 0,
        pageSize: 10,
        loading: false,
        tempSelected: [],
      };
    },
    computed: {
      category() {
        return {
          user: 1,
          group: 2,
        }[this.type];
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
      ColumnConfig() {
        return ColumnConfig[this.type];
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
          this.pageSize = 10;
          this.getOptions();
        },
        immediate: true,
      },
    },
    mounted() {},
    methods: {
      getOptions() {
        if (this.loading) return;
        this.loading = true;
        let params = {
          current_page: this.currentPage,
          page_size: this.pageSize,
        };
        this.dataFn(params).then(res => {
          this.options = res.content;
          this.total = res.total;
        });
      },
      handlePageChange(page) {
        this.currentPage = page;
        this.getOptions();
      },
      isDisabled(data) {
        return data.status != "ACTIVE" || this.getIsAdded(data);
      },
      handleConfirm() {
        this.$emit("close", false);
        this.$emit("change", this.tempSelected);
        this.tempSelected = [];
      },
      getIsAdded(row) {
        return this.selected.some(predicate => predicate[this.id] === row[this.id]);
      },
      cancel() {
        this.tempSelected = [];
        this.$emit("close", false);
      },
      close() {
        this.$emit("close");
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

<style scoped lang="scss"></style>
