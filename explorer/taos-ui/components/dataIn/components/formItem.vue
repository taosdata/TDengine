<template>
  <div v-if="config && data">
    <template v-if="config.children && !config.hasValue">
      <FormItem
        v-for="item in config.children"
        :key="item.label"
        :config="item"
        :data="localData[field]"
        :parent="parent + field + '.'"
      />
    </template>
    <!-- 排除tmq中 dsn 自己带的参数，这类 param 特点就是 type=input,没有label  -->
    <el-form-item
      v-else-if="isFieldDisplay && (labelText || config.type !== 'input')"
      :label="labelText"
      :label-width="labelWidth"
      :required="isFieldRequired()"
      :class="[classMark, { 'hidden-required': !isFieldRequired() }]"
      :rules="timeFormats.includes(field) ? [...timeRules, ...rules] : rules"
      :prop="field.indexOf('.') >= 0 ? `${parent.slice(0, -1)}['${field}']` : parent + field"
    >
      <template #label>
        <el-tooltip v-if="doscShow && !dataSetDocsShow" placement="top" effect="light" :open-delay="0">
          <template #content>
            <DocsContent
              v-if="doscShow && !dataSetDocsShow"
              :style="docsStyle"
              :class="config.templateUrl ? 'noboder' : ''"
              :content="config.description"
            />
          </template>
          <span>
            <span>{{ labelText }}</span>
            <span
              v-if="doscShow && !dataSetDocsShow"
              style="display: inline-block; margin-left: 2px; vertical-align: middle"
            >
              <Icon name="label_info" class="info-icon-custom"></Icon>
            </span>
          </span>
        </el-tooltip>
      </template>
      <el-input
        v-if="inputType.includes(config.type)"
        :id="parent + field"
        v-model="localData[field]"
        :disabled="isFieldDisabled()"
        :type="config.type"
        :show-password="config.type == 'password'"
        :placeholder="config.placeholder"
        @blur="trimInput"
      ></el-input>
      <el-input-number
        v-if="config.type == 'number'"
        :id="parent + field"
        v-model="localData[field]"
        :disabled="isFieldDisabled()"
        :max="config.max"
        :min="config.min"
        :placeholder="config.placeholder"
      ></el-input-number>
      <el-select
        v-if="config.type == 'select'"
        :id="parent + field"
        v-model="localData[field]"
        v-bind="meta"
        class="ds-select"
        clearable
        :disabled="isFieldDisabled()"
        :placeholder="config.placeholder"
        :multiple="config.multiple"
      >
        <el-option
          v-for="item in getOptions()"
          :key="item.value"
          v-bind="item"
          :title="item.description"
          :disabled="item.disabled"
        ></el-option>
      </el-select>
      <el-switch
        v-if="config.type == 'switch'"
        :id="parent + field"
        v-model="localData[field]"
        :disabled="isFieldDisabled()"
        :placeholder="config.placeholder"
        @change="changeSwitch"
      ></el-switch>
      <component
        :is="currentDatePicker"
        v-if="config.type == 'time'"
        :id="parent + field"
        v-model="localData[field]"
        :disabled="isFieldDisabled()"
        :placeholder="config.placeholder"
        :type="config.dateType"
        style="width: 100%"
        value-format="YYYY-MM-DDTHH:mm:ssZ"
      ></component>
      <InputWithSelect
        v-if="config.type == 'compose'"
        ref="compose"
        :options="getOptions()"
        :config="config"
        :data="data"
      />
      <InputNumberWithSelect
        v-if="config.type == 'composeAppend'"
        ref="composeAppend"
        v-model="localData[field]"
        :options="getOptions()"
        :config="config"
      />
      <UploadCsv
        v-if="config.type == 'file'"
        v-model="localData[field]"
        :config="config"
        :btn-text="config.btnText"
        :disabled="isFieldDisabled()"
      >
      </UploadCsv>
      <Dataset v-if="config.type == 'dataset'" v-model="localData[field]" :config="config" :data="data" />
      <Bucket
        v-if="config.type == 'bucket'"
        ref="bucket"
        :config="config"
        :data="data"
        :parent-config-list="parentConfigList"
      />
      <PointName
        v-if="config.type == 'pattern'"
        ref="pattern"
        :config="config"
        :data="data"
        :parent-config-list="parentConfigList"
      />
      <CustomId
        v-if="config.type == 'customId'"
        ref="pattern"
        :config="config"
        :data="data"
        :parent-config-list="parentConfigList"
      />
      <Namespace
        v-if="config.type == 'namespace'"
        ref="namespace"
        :config="config"
        :data="data"
        :parent-config-list="parentConfigList"
      />
      <DocsContent
        v-if="dataSetDocsShow"
        :style="docsStyle"
        :class="config.templateUrl ? 'noboder' : ''"
        :content="config.description"
      />
    </el-form-item>
    <template v-else-if="nolabel">
      <DocsContent v-if="doscShow" :style="docsStyle" :content="config.description" />
    </template>
  </div>
</template>

<script setup lang="ts">
import DocsContent from 'components/MdRender.vue';
import UploadCsv from './uploadCsv.vue';
import Dataset from './dataset.vue';
import PointName from './pointName.vue';
import CustomId from './customId.vue';
import Bucket from './bucket.vue';
import InputWithSelect from './inputWithSelect.vue';
import InputNumberWithSelect from './inputNumberWithSelect.vue';
import Namespace from './namespace.vue';
import UTCDateTimePicker from 'components/UTCDateTimePicker.vue';
import TimezoneDatePicker from 'components/datePicker/index';
import { project } from 'config';
import { t } from 'locales';
import {
  sourceForm,
  currentPageType,
  TimeFormats,
  getFieldClassMarkName,
  checkJson,
  getNestedValue
} from '../model/util';

const components = {
  UTCDateTimePicker,
  TimezoneDatePicker
};
const currentDatePicker = computed(
  () => components[project.isCloud ? 'UTCDateTimePicker' : ('TimezoneDatePicker' as keyof typeof components)]
);

const fnMap = {
  checkJson: checkJson
};

const props = withDefaults(
  defineProps<{
    config: Record<string, any>;
    data: Record<string, any>;
    parser?: Record<string, any>;
    parent: string;
    parentConfigList?: Record<string, any>[];
    parentConfig?: Record<string, any>;
  }>(),
  {
    config: () => ({}),
    data: () => ({}),
    parser: () => ({}),
    parentConfigList: () => [],
    parentConfig: () => ({})
  }
);
const emit = defineEmits(['csv-enable', 'update:data', 'update:config', 'update:parent-config']);

const inputType = ['input', 'textarea', 'password'];
const noLabelType = ['tab', 'opcTable'];

const date1 = ref();
const date2 = ref();
const localData = reactive(props.data);
const localConfig = reactive(props.config);
const localParentConfig = reactive({ ...props.parentConfig });

watch(localData, newData => {
  emit('update:data', newData);
});

watch(localConfig, newConfig => {
  emit('update:config', newConfig);
});

watch(localParentConfig, newConfig => {
  emit('update:parent-config', newConfig);
});

const field = computed(() => props.config.valueField || props.config.field);
const labelWidth = computed(() => props.config.labelWidth || undefined);
const labelText = computed(() => (props.config.labelShow !== false ? props.config.label : ''));
const nolabel = computed(() => !props.config.type || noLabelType.includes(props.config.type));
const doscShow = computed(() => props.config.description && !props.config.info);
const dataSetDocsShow = computed(() => props.config.info2);
const docsStyle = computed(() => {
  const isTab = props.config.type == 'tab';
  const marginKey = isTab ? 'marginBottom' : 'marginTop';
  return {
    [marginKey]: '5px'
  };
});
const rules = computed(() => {
  const requireRule = [
    {
      required: true,
      message: t('common.requiredTemp', [props.config.label ?? props.config.field])
    }
  ];

  const patternRule = [
    {
      pattern: props.config.pattern,
      message: props.config.patternMsg,
      trigger: 'blur'
    }
  ];
  const validatorRule = [
    {
      validator: fnMap[props.config.validator],
      trigger: 'blur'
    }
  ];
  let rules: Recordable[] = [];
  if (props.config.pattern) {
    rules = rules.concat(patternRule);
  }

  if (props.config.validator) {
    rules = rules.concat(validatorRule);
  }

  return props.config.required ? rules.concat(requireRule) : rules;
});
const timeRules = computed(() => {
  return [{ validator: compareTime, trigger: 'blur' }];
});
const meta = computed(() => props.config.meta || {});

const isFieldDisplay = computed(() => {
  if (nolabel.value) return false;

  const { displayDependsOn, displayDependsOnValues, displayConditions, hasParentSwitch } = props.config;

  if (!displayDependsOn || !displayDependsOnValues) {
    return true; // 默认都展示
  }

  const checkDisplay = (dep: string) => {
    const deps = dep.split('/');
    const nestedValue = getNestedValue(sourceForm.data, dep);
    const display = displayDependsOnValues[deps[deps.length - 1]]?.includes(nestedValue);

    if (!hasParentSwitch) {
      localConfig['hide'] = !display;
    }

    if (!display) {
      // 隐藏时需要把当前的值清空或者设置为默认值
      const { defaultValue } = props.config;
      if (defaultValue !== undefined) {
        localData[field.value] = defaultValue;
      } else {
        delete localData[field.value];
      }
    }

    return display;
  };

  if (displayConditions === 'some') {
    return displayDependsOn.some(checkDisplay);
  } else {
    return displayDependsOn.every(checkDisplay);
  }
});

const timeFormats = computed(() => {
  return TimeFormats;
});
const classMark = computed(() => {
  return getFieldClassMarkName(props.parent + field.value);
});

// 检查字段是否满足依赖条件，返回是否禁用
const isFieldDisabled = () => {
  if (!props.config.disabledDependsOn || !props.config.disabledDependsOnValues) {
    switch (currentPageType.value) {
      case 'add':
        return props.config.disabled;
      case 'edit':
        return props.config.editDisabled !== undefined ? props.config.editDisabled : props.config.disabled;
      case 'copy':
        return props.config.copyDisabled !== undefined ? props.config.copyDisabled : props.config.disabled;
      default:
        return props.config.disabled;
    } // 没有依赖，直接显示
  }

  return props.config.disabledDependsOn.every((dep: string) => {
    const deps = dep.split('/');
    const nestedValue = getNestedValue(sourceForm.data, dep);
    const display = props.config.disabledDependsOnValues?.[deps[deps.length - 1]]?.includes(nestedValue);
    return display;
  });
};

// 检查字段是否满足依赖条件，返回是否必填
const isFieldRequired = () => {
  const { requiredDependsOn, requiredDependsOnValues, requiredConditions } = props.config;

  if (!requiredDependsOn || !requiredDependsOnValues) {
    switch (currentPageType.value) {
      case 'add':
        return props.config.required;
      case 'edit':
        return props.config.editRequired !== undefined ? props.config.editRequired : props.config.required;
      case 'copy':
        return props.config.copyRequired !== undefined ? props.config.copyRequired : props.config.required;
      default:
        return props.config.required;
    }
  }

  const checkRequired = (dep: string) => {
    const deps = dep.split('/');
    const nestedValue = getNestedValue(sourceForm.data, dep);
    const display = requiredDependsOnValues[deps[deps.length - 1]]?.includes(nestedValue);

    return display;
  };

  if (requiredConditions === 'some') {
    return requiredDependsOn.some(checkRequired);
  } else {
    return requiredDependsOn.every(checkRequired);
  }
};

const isFieldEmpty = () => {
  if (!props.config.emptyDependsOn || !props.config.emptyDependsOn) {
    return false;
  }

  return props.config.emptyDependsOn.every((dep: string) => {
    const deps = dep.split('/');
    const nestedValue = getNestedValue(sourceForm.data, dep);
    const display = props.config.emptyDependsOnValues?.[deps[deps.length - 1]]?.includes(nestedValue);
    return display;
  });
};

// 自动更新所有字段的显示状态
const updateFieldStates = () => {
  if (props.config.requiredDependsOn) {
    localConfig.required = isFieldRequired();
  }

  if (props.config.disabledDependsOn) {
    localConfig.disabled = isFieldDisabled();
  }

  if (props.config.emptyDependsOn && isFieldEmpty()) {
    localData[field.value] = '';
  }
};
watch(
  localData,
  () => {
    updateFieldStates();
  },
  { deep: true }
);
function changeSwitch() {
  if (props.config.field === 'use_csv_config') {
    emit('csv-enable', props.data[field.value]);
  }
}

function getOptions() {
  if (props.config.optionsDependsOn) {
    const value = getNestedValue(sourceForm.data, props.config.optionsDependsOn);
    const key = typeof value === 'string' ? value : '';

    // pi 设置 select 的值 临时解决 不是最终方案
    if (value.indexOf('AF') > 0) {
      localData[field.value + '_type'] = 'template';
    } else {
      localData[field.value + '_type'] = 'point';
    }

    return props.config.options[key];
  }

  return props.config.options;
}

function compareTime(_: any, _value: string, callback: AnyFunction) {
  const getDateValue = (key: string) => {
    return localData?.[key] ? new Date(localData[key]) : 0;
  };

  const fieldMap: Record<string, { start: string; end: string }> = {
    taos: { start: 'start', end: 'end' },
    postgres: { start: 'start', end: 'end' },
    mysql: { start: 'start', end: 'end' },
    oracle: { start: 'start', end: 'end' },
    mssql: { start: 'start', end: 'end' },
    mongodb: { start: 'start', end: 'end' },
    avevaHistorian: { start: 'beginDateTime', end: 'endDateTime' },
    influxdb: { start: 'beginTime', end: 'endTime' },
    opentsdb: { start: 'beginTime', end: 'endTime' },
    pibackfill: { start: 'BackfillStartTime', end: 'BackfillEndTime' }
  };

  const fields = fieldMap[sourceForm.type];

  if (fields) {
    date1.value = getDateValue(fields.start);
    date2.value = getDateValue(fields.end);
  }

  // Check if date1 is greater than date2
  if (date1.value && date2.value && date1.value > date2.value) {
    return callback(new Error(t('dataIn.timeTip')));
  } else {
    callback();
  }

  callback();
}

function trimInput() {
  // 在失去焦点时去除输入框值的前后空格
  localData[field.value] = props.data[field.value].toString().trim();
}
</script>

<style scoped lang="scss">
$color-description: rgb(137 130 130);

:deep(.markdown-body) {
  p {
    font-size: 14px;
  }

  color: $color-description;
}

.ds-select {
  width: 100%;
}

.noboder {
  :deep(p) {
    margin-bottom: 5px;
  }

  :deep(table) {
    font-size: 12px;
    border: none;

    th,
    tr,
    td {
      padding: 0;
      background-color: unset;
      border: none;
    }
  }
}
</style>
