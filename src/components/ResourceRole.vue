<template>
  <el-tree class="custom-tree" accordion :indent="10" expand-on-click-node :data="options" node-key="id">
    <el-tooltip slot-scope="{ node, data }" class="item" :content="node.label" placement="right">
      <div class="flexBetween">
        <el-checkbox @change="change($event, data.value)" v-if="data.value" class="left" :value="isChecked(data.value)"></el-checkbox>
        <span class="center">{{ node.label }}</span>
        <span class="right" @click="currentPopover = node.id">
          <el-popover v-if="node.isLeaf" :value="currentPopover == node.id" placement="right" width="245" trigger="click">
            <UTCDATETIMEPICKER
              size="mini"
              :picker-options="$root.afterTimePickerOptions"
              v-model="data.value.expiration"
              type="datetime"
              popper-class="resource-expiration-popover"
              value-format="timestamp"
              :placeholder="$t('expiration')"
            >
            </UTCDATETIMEPICKER>
            <el-icon slot="reference" class="el-icon-date"></el-icon>
          </el-popover>
        </span>
      </div>
    </el-tooltip>
  </el-tree>
</template>

<script>
  import {  getUnGrantList } from "@/api/gateway/data/dbs";
  import UTCDATETIMEPICKER from "@/components/UTCDateTimePicker.vue";
  export default {
    props: {
      type: {
        type: String,
        default: "all",
      },
      level: {
        type: String,
        default: "organization",
      },
      defaultKeys: {
        type: Array,
        default: () => [],
      },
      value: {
        type: Array,
        default: () => [],
      },
      params: {
        type: Object,
        default: () => ({}),
      },
    },
    components: { UTCDATETIMEPICKER },
    data() {
      return {
        options: [],
        currentValue: [],
        expiration: {},
        currentPopover: null,
      };
    },
    computed: {
      dataFn() {
        return getUnGrantList;
      },
      aliasMap() {
        return this.$store.getters.instanceAliasMap;
      },
    },
    watch: {
      params: {
        handler(newval, oldval) {
          if (JSON.stringify(newval) != JSON.stringify(oldval)) {
            // this.getGrantList();
          }
        },
        deep: true,
      },
    },

    created() {
      // this.getGrantList();
    },
    mounted() {},
    methods: {
      change(status, value) {
        if (status) {
          this.value.push(value);
        } else {
          this.value.splice(
            this.value.findIndex(item => Object.keys(item).every(key => item[key] == value[key])),
            1
          );
        }
      },
      isChecked(val) {
        return this.value.some(item => Object.keys(item).every(key => item[key] == val[key]));
      },
      getGrantList() {
        this.dataFn(this.params)
          .then(data => {
            let result = [];
            const appId = this.params.app_id;
            const databaseId = this.params.databaseId;
            let database;
            data.forEach(item => {
              switch (item.resourceType) {
                case "Organization":
                  if (appId || databaseId) return;
                  result[0] = {
                    label: "Organization",
                    id: item.accountId,
                    children: item.roles?.map(ite => ({
                      label: ite.name,
                      id: ite.id,
                      value: {
                        role_id: ite.id,
                        expiration: null,
                      },
                      type: "Organization",
                      checked: false,
                    })),
                  };
                  break;
                case "Instance":
                  if (databaseId) return;
                  if (appId) {
                    if (appId != item.instanceId) return;
                    item.roles?.forEach(ite => {
                      result.push({
                        label: ite.name,
                        id: ite.id,
                        type: "Instance",
                        value: {
                          role_id: ite.id,
                          instance_id: item.instanceId,
                          expiration: null,
                        },
                        checked: false,
                      });
                    });
                    return;
                  }
                  if (!result[1]) {
                    result[1] = {
                      label: "Instance",
                      id: "instance",
                      children: [],
                    };
                  }
                  result[1].children.push({
                    label: this.aliasMap[item.instanceId],
                    id: item.instanceId,
                    children: item.roles?.map(ite => ({
                      label: ite.name,
                      id: ite.id,
                      type: "Instance",
                      value: {
                        role_id: ite.id,
                        instance_id: item.instanceId,
                        expiration: null,
                      },
                      checked: false,
                    })),
                  });
                  break;
                case "Database":
                  if (databaseId) {
                    if (databaseId != item.databaseId) return;
                    result = item.roles?.map(ite => ({
                      label: ite.name,
                      id: ite.id,
                      type: "Database",
                      value: {
                        role_id: ite.id,
                        instance_id: item.instanceId,
                        database_id: item.databaseId,
                        expiration: null,
                      },
                      checked: false,
                    }));
                    return;
                  }
                  if (!result[1] && !appId) {
                    result[1] = {
                      label: "Instance",
                      id: "Instance",
                      children: [],
                    };
                  }
                  if (appId) {
                    if (!result.find(ite => ite.id == "Database")) {
                      result.push({
                        label: "Database",
                        id: "Database",
                        children: [],
                      });
                    }
                    database = result.find(ite => ite.id == "Database").children;
                  } else {
                    if (!result[1].children.find(ite => ite.id == item.instanceId).children.find(ite => ite.id == "Database")) {
                      result[1].children
                        .find(ite => ite.id == item.instanceId)
                        .children.push({
                          label: "Database",
                          id: "Database",
                          children: [],
                        });
                    }
                    database = result[1].children.find(ite => ite.id == item.instanceId).children.find(ite => ite.id == "Database").children;
                  }
                  database.push({
                    label: item.resourceName,
                    id: item.databaseId,
                    children: item.roles?.map(ite => ({
                      label: ite.name,
                      id: ite.id,
                      type: "Database",
                      value: {
                        instance_id: item.instanceId,
                        role_id: ite.id,
                        database_id: item.databaseId,
                        expiration: null,
                      },
                      checked: false,
                    })),
                  });
                  break;
                default:
                  break;
              }
            });
            this.options = result.filter(predicate => predicate);
          })
          .catch(() => {
            this.options = [];
          });
      },
      reset() {
        this.expiration = {};
        this.currentValue = [];
        this.$emit("input", []);
        // this.getGrantList();
      },
    },
  };
</script>

<style scoped lang="scss">
  .custom-tree {
    max-height: 300px;
    overflow-y: auto;
  }

  .flexBetween {
    flex: 1;
    .right {
      flex-shrink: 0;
      margin: 0 5px;
    }
    .left {
      flex-shrink: 0;
      margin-right: 5px;
    }
    .center {
      flex: 1;
      text-align: left;
    }
  }
</style>
<style lang="scss">
  .resource-expiration-popover > .el-picker-panel__footer > button:nth-child(1) {
    display: none;
  }
</style>
