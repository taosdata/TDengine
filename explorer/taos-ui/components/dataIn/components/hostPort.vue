<template>
  <div>
    <template v-if="!isView">
      <template v-for="(child, index) in localConfig" :key="'child' + '-' + child.host.field">
        <div>
          <el-form-item
            label-width="240px"
            :required="child.host.required"
            :rules="rules(child.host)"
            :class="[classMark(child.host.field)]"
            :prop="parent + child.host.field"
          >
            <template #label>
              <el-tooltip placement="top" effect="light" :open-delay="0">
                <template #content>
                  <DocsContent :class="child.templateUrl ? 'noboder' : ''" :content="child.host.description" />
                </template>
                <span>
                  <span>{{ child.host.label }}</span>
                  <span style="margin-left: 1px">
                    <Icon name="label_info" class="info-icon-custom"></Icon>
                  </span>
                </span>
              </el-tooltip>
            </template>
            <div class="flex-between flex-1">
              <el-input
                :id="parent + child.host.field"
                v-model="localData[child.host.field]"
                style="flex: 0 80%"
                class="mr20"
                :placeholder="child.host.placeholder"
                :disabled="isEdit && !isCopy"
                @input="handlerConfig('host', child.host.field)"
              >
              </el-input>
              <el-button
                v-if="index"
                :disabled="isEdit && !isCopy"
                style="width: 110px"
                type="primary"
                plain
                @click="remove(index, child.host.field, child.port.field)"
                >{{ t('dataIn.delBroker') }}</el-button
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
            <template #label>
              <el-tooltip placement="top" effect="light" :open-delay="0">
                <template #content>
                  <DocsContent :class="child.templateUrl ? 'noboder' : ''" :content="child.port.description" />
                </template>
                <span>
                  <span>{{ child.port.label }}</span>
                  <span style="margin-left: 1px">
                    <Icon name="label_info" class="info-icon-custom"></Icon>
                  </span>
                </span>
              </el-tooltip>
            </template>
            <div class="flex-start flex-1">
              <el-input
                :id="parent + child.port.field"
                v-model="localData[child.port.field]"
                style="flex: 0 80%"
                class="mr20"
                :placeholder="child.port.placeholder"
                :disabled="isEdit && !isCopy"
                @input="handlerConfig('port', child.port.field)"
              >
              </el-input>
            </div>
          </el-form-item>
        </div>
      </template>
      <div class="flex-end">
        <el-button
          size="default"
          style="width: 110px"
          :disabled="isEdit && !isCopy"
          type="primary"
          plain
          @click="add"
          >{{ t('dataIn.addBroker') }}</el-button
        >
      </div>
    </template>
    <template v-else>
      <template v-for="(child, index) in localConfig" :key="'child' + '-' + index">
        <div class="descriptions">
          <div class="desc-item">
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

<script setup lang="ts">
import { getFieldClassMarkName } from '../model/util';
import { cloneDeep } from 'lodash-es';
import DocsContent from 'components/MdRender.vue';
import { currentPageType } from '../model/util';
import { t } from 'locales';

const props = withDefaults(
  defineProps<{
    config: Record<string, any>[];
    data: Record<string, any>;
    parentConfigList: Record<string, any>[];
    parent: string;
  }>(),
  {}
);

const localConfig = reactive(props.config);
const localData = reactive(props.data);

const isEdit = computed(() => currentPageType.value === 'edit');
const isCopy = computed(() => currentPageType.value === 'copy');
const isView = computed(() => currentPageType.value === 'view');

const rules = computed(() => {
  return (config: Recordable) => {
    const requireRule = [
      {
        required: true,
        message: t('common.requiredTemp', [config.label ?? config.field])
      }
    ];
    const patternRule = [
      {
        pattern: config.pattern,
        message: config.patternMsg,
        trigger: 'blur'
      }
    ];

    return config.required
      ? config.pattern
        ? [...requireRule, ...patternRule]
        : requireRule
      : config.pattern
        ? [...patternRule]
        : [];
  };
});
const classMark = (field: string) => {
  return getFieldClassMarkName(props.parent + field);
};
const emit = defineEmits(['update:data', 'update:config']);

watch(localData, newData => {
  getResult();
  emit('update:data', newData);
});
watch(
  localConfig,
  newData => {
    emit('update:config', newData);
  },
  {
    deep: true,
    immediate: true
  }
);

function add() {
  const item = cloneDeep(localConfig[0]);
  const key = new Date().getTime();
  item.host.field = 'host_' + key;
  item.port.field = 'port_' + key;
  item.host.required = false;
  item.port.required = false;
  item.host.value = '';
  item.port.value = '';
  localConfig.push(item);
}

function remove(index: number, hostField: string, portField: string) {
  if (hostField) {
    delete localData[hostField];
  }
  if (portField) {
    delete localData[portField];
  }
  localConfig.splice(index, 1);
  getResult();
}

function handlerConfig(type: string, field: string) {
  const isHostType = type === 'host';
  const key = Object.keys(localData).find(key => key === field);

  if (key) {
    const id = key.substring((isHostType ? 'host_' : 'port_').length);
    const hostKey = `host_${id}`;
    const portKey = `port_${id}`;
    const host = localData[hostKey];
    const port = localData[portKey];

    manage(type, hostKey, portKey, host, port);
  }
}

function getResult() {
  let result = [];
  result = Object.keys(localData)
    .filter(key => key.startsWith('host_'))
    .map(hostKey => {
      const id = hostKey.substring('host_'.length); // 提取 host 后的唯一标识
      const portKey = `port_${id}`;
      const host = localData[hostKey];
      const port = localData[portKey];
      return host && port ? `${host}:${port}` : '';
    });
  localData.endpoint = result
    .join(',')
    .replace(/(,{2,})/g, ',')
    .replace(/^,|,$/g, '');
}

function manage(type: string, hostKey: string, portKey: string, host: string, port: string) {
  localConfig.map((config, index: number) => {
    if (index === 0) return; // 跳过第一个元素

    const isHostType = type === 'host' && config.host.field === hostKey;
    const isPortType = type === 'port' && config.port.field === portKey;

    if (isHostType || isPortType) {
      if (host && !port) {
        config.port.required = true;
        config.port.defaultValue = '';
      } else if (!host && port) {
        config.host.required = true;
        config.host.defaultValue = '';
      } else if (!host && !port) {
        config.host.required = false;
        config.port.required = false;
        config.host.defaultValue = '';
        config.port.defaultValue = '';
      }
    }
  });
}
</script>

<style scoped lang="scss">
.descriptions {
  display: grid;
  grid-template-columns: 1fr 1fr;
  font-size: 16px;
}

.desc-item {
  padding: 0 5px 10px 0;

  > span {
    display: inline-block;
  }
}
</style>
