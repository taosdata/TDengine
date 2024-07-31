<template>
  <div>
    <template v-if="!isViewable">
      <template v-for="(child, index) in configData">
        <div :key="'child' + '-' + index">
          <el-form-item
            label-width="240px"
            :required="child.host.required"
            :rules="rules(child.host)"
            :class="[classMark(child.host.field)]"
            :prop="parent + child.host.field"
          >
            <template slot="label">
              <el-tooltip placement="top" effect="light" :open-delay="0">
                <template slot="content">
                  <DocsContent
                    :class="child.templateUrl ? 'noboder' : ''"
                    :content="child.host.description"
                  />
                </template>
                <span>
                  <span>{{ child.host.label }}</span>
                  <span style="margin-left: 1px">
                    <Icon name="label_info" class="info_icon_custom"></Icon>
                  </span>
                </span>
              </el-tooltip>
            </template>
            <div class="flexStart">
              <el-input
                v-model="data[child.host.field]"
                :id="parent + child.host.field"
                style="flex: 0 80%"
                class="mr20"
                :placeholder="child.host.placeholder"
                :disabled="isEdit"
              >
              </el-input>
              <el-button
                v-if="index"
                :disabled="!index || isEdit"
                style="width: 110px"
                type="primary"
                plain
                @click="remove(index, child.host.field, child.port.field)"
                >{{ $t("datasource.delBroker") }}</el-button
              >
            </div>
          </el-form-item>
          <el-form-item
            label-width="240px"
            :required="child.port.required"
            :rules="rules(child.port)"
            :class="[classMark(child.port.field)]"
            :prop="parent + child.port.field"
          >
            <template slot="label">
              <el-tooltip placement="top" effect="light" :open-delay="0">
                <template slot="content">
                  <DocsContent
                    :class="child.templateUrl ? 'noboder' : ''"
                    :content="child.port.description"
                  />
                </template>
                <span>
                  <span>{{ child.port.label }}</span>
                  <span style="margin-left: 1px">
                    <Icon name="label_info" class="info_icon_custom"></Icon>
                  </span>
                </span>
              </el-tooltip>
            </template>
            <div class="flexStart">
              <el-input
                v-model="data[child.port.field]"
                :id="parent + child.port.field"
                style="flex: 0 80%"
                class="mr20"
                :placeholder="child.port.placeholder"
                :disabled="isEdit"
              >
              </el-input>
            </div>
          </el-form-item>
        </div>
      </template>
      <div class="flexEnd">
        <el-button
          size="small"
          style="width: 110px"
          :disabled="isEdit"
          type="primary"
          plain
          @click="add"
          >{{ $t("datasource.addBroker") }}</el-button
        >
      </div>
    </template>
    <template v-else>
      <template v-for="(child, index) in configData">
        <div :key="'child' + '-' + index" class="descriptions">
          <div class="descItem">
            <span style="padding-right: 10px">{{ child.host.label }}:</span>
            <span>{{ child.host.value }}</span>
          </div>
          <div>
            <span style="padding-right: 10px">{{ child.port.label }}:</span>
            <span>{{ child.port.value }}</span>
          </div>
        </div>
      </template>
    </template>
  </div>
</template>

<script>
import { uuid } from "@/utils/util";
import { getDsnData, optionsField, getFieldClassMarkName } from "../utils";
import { jsonToObj, deepClone } from "@/utils";
import FormItem from "./formItem.vue";
import DocsContent from "@/views/support/components/editorContentDisplay.vue";

export default {
  props: {
    data: {
      type: Object,
      default: () => ({}),
    },
    config: {
      type: Array,
      default: () => [],
    },
    parentConfigList: {
      type: Array,
      default: () => [],
    },
    parent: {
      type: String,
      default: "",
    },
    isViewable: {
      type: Boolean,
    },
  },
  inject: ["sourceParent"],
  components: { FormItem, DocsContent },
  data() {
    return {
      loading: false,
      item: {},
      configData: this.config,
    };
  },
  computed: {
    isEdit() {
      return this.sourceParent.isEditable;
    },
    rules() {
      return (config) => {
        const requireRule = [
          {
            required: true,
            message: this.$t("required", [config.label ?? config.field]),
          },
        ];
        const patternRule = [
          {
            pattern: config.pattern,
            message: config.patternMsg,
            trigger: "blur",
          },
        ];

        return config.required
          ? config.pattern
            ? [...requireRule, ...patternRule]
            : requireRule
          : config.pattern
          ? [...patternRule]
          : [];
      };
    },
    classMark() {
      return (field) => {
        return getFieldClassMarkName(this.parent + field);
      };
    },
  },
  watch: {
    data: {
      deep: true,
      immediate: true,
      handler(data) {
        this.getResult();
      },
    },
  },
  created() {},
  mounted() {
    this.item = deepClone(this.config[0]);
  },
  methods: {
    add() {
      let item = deepClone(this.item);
      const key = uuid();
      item.host.field = "host_" + key;
      item.port.field = "port_" + key;
      item.host.required = false;
      item.port.required = false;
      item.host.value = "";
      item.port.value = "";
      this.configData = this.configData.concat(item);
    },
    remove(index, hostField, portField) {
      if (hostField) {
        delete this.data[hostField];
      }
      if (portField) {
        delete this.data[portField];
      }
      this.configData.splice(index, 1);
      this.getResult();
    },
    getResult() {
      let result = [];
      result = Object.keys(this.data)
        .filter((key) => key.startsWith("host_"))
        .map((hostKey) => {
          const id = hostKey.substring("host_".length); // 提取 host 后的唯一标识
          const portKey = `port_${id}`;
          const host = this.data[hostKey];
          const port = this.data[portKey];
          if ((host && !port) || (!host && port)) {
            this.mange(hostKey, portKey);
          }
          if (!host && !port) {
            this.mange(hostKey, portKey, true);
          }
          return host && port ? `${host}:${port}` : "";
        });
      this.data.endpoint = result.join(",");
    },
    mange(hostKey, portKey, empty) {
      this.configData = this.configData.map((config, index) => {
        if (config.host.field === hostKey && index) {
          config.port.required = !empty;
        }
        if (config.port.field === portKey && index) {
          config.host.required = !empty;
        }
        return config;
      });
    },
  },
};
</script>

<style scoped lang="scss">
.descriptions {
  font-size: 16px;
  display: grid;
  grid-template-columns: 1fr 1fr;
}
.descItem {
  padding: 0 5px 10px 0;
  > span {
    display: inline-block;
  }
}
</style>
