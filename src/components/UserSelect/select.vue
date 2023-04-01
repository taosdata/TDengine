<template>
  <div class="user-select-wrapper">
    <el-select
      class="w100"
      filterable
      :allow-create="allowCreateAdd"
      :placeholder="placeholder"
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
  import { getOrganizationUser ,getAppUser,getGroupList} from "@/api/gateway/data/dbs";
  
  import { deepClone, loadPageData } from "@/utils";
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
        default: true,
      },
      placeholder: {
        type: String,
        default: "",
      },
      allowCreate: {
        type: Boolean,
        default: false,
      },
      valueField: {
        type: String,
        default: "",
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
      };
    },
    computed: {
      selectValue: {
        get() {
          return this.value;
        },
        set(val) {
          this.$emit("input", val);
        },
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
      displayField() {
        return {
          user: "email",
          group: "group_name",
        }[this.type];
      },
      id() {
        if (this.valueField) return this.valueField;
        return {
          user: "userId",
          group: "id",
        }[this.type];
      },
      dataFn() {
        return {
          user: [getOrganizationUser, getAppUser],
          group: [getGroupList, getGroupList],
        }[this.type][this.level];
      },
      addBtnShow() {
        return !!this.$store.state.currentServerLevel || this.total < 5;
      },
      allowCreateAdd() {
        if (this.type == "group" || !this.allowCreate) return false;
        return this.allowCreate && this.addBtnShow;
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
            this.total = data.length;
            this.allData = data.filter(item => item.status == "ACTIVE");
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
      handleConfirm() {
        const addList = [];
        const removeList = [];
        this.tempSelected.forEach(item => {
          if (!this.value.some(predicate => predicate[this.id] === item[this.id])) {
            addList.push(item);
          } else {
            removeList.push(item);
          }
        });
        this.$emit("update:dialog", false);
        this.$emit("input", this.tempSelected);
        this.$emit("change", this.tempSelected, addList, removeList);
        this.tempSelected = deepClone(this.value);
      },
      cancel() {
        this.tempSelected = deepClone(this.value);
        this.$emit("update:dialog", false);
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
