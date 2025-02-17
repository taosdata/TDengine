<template>
  <div class="info">
    <el-form label-width="180px" label-position="left">
      <section class="info-content">
        <section class="left">
          <el-form-item v-for="item in leftField" :key="item" :label="item + ':'">
            {{ infoData[item] }}
          </el-form-item>
          <el-form-item v-if="infoType !== 'db'" label="tags:"></el-form-item>
          <el-table v-if="infoType !== 'db'" tooltip-effect="light" size="small" border :data="tags">
            <el-table-column :show-overflow-tooltip="true" min-width="100" label="name" prop="name"> </el-table-column>
            <el-table-column
              :show-overflow-tooltip="true"
              :label="infoType == 'stb' ? 'type' : 'value'"
              prop="value"
              :min-width="infoType == 'stb' ? 100 : 150"
            >
            </el-table-column>
          </el-table>
        </section>
        <section class="right">
          <el-form-item v-for="item in rightField" :key="item" :label="item + ':'">
            {{ infoData[item] }}
          </el-form-item>
          <el-form-item v-if="infoType == 'stb' || infoType == 'db'" label="columns:"></el-form-item>
          <el-table
            v-if="infoType !== 'db' && columns?.length > 0"
            tooltip-effect="light"
            size="small"
            border
            :data="columns"
          >
            <el-table-column :show-overflow-tooltip="true" min-width="100" label="name" prop="name"> </el-table-column>
            <el-table-column :show-overflow-tooltip="true" width="150" label="type" prop="value"> </el-table-column>
          </el-table>
        </section>
      </section>
      <el-form-item v-if="infoType === 'db'" :label="'DSN:'" class="tmp-label">
        <div class="nowrap tmp-dns">{{ tmqDNS }}</div>
        <div class="cp-btn">
          <el-tooltip effect="light" :content="t('explorer.copyTMQDSN')">
            <el-button link type="primary" size="small" icon="copy-document" @click.stop="copyDNS()">{{
              t('common.copy')
            }}</el-button>
          </el-tooltip>
        </div>
      </el-form-item>
    </el-form>
  </div>
</template>

<script lang="ts" setup>
import { copy } from 'utils';
import { DBCustomedFiled } from 'constants1/tdengine';
import { getCurrentInfoDataProvider } from './utils';
import { t } from 'locales';
import { getSubtbCurrentStruct, getStableStructReq } from '../../api';
import { instance, project } from 'config';

const displayMap = {
  stb: ['name', 'create_time'],
  tb: ['table_name', 'create_time', 'stable_name']
};
const columns = ref<Recordable[]>([]);
const tags = ref<Recordable[]>([]);
const currentInfoData = getCurrentInfoDataProvider();
const infoType = computed(() => currentInfoData.type);
const infoData = computed(() => currentInfoData[infoType.value]);
const infoField = computed(() =>
  infoType.value == 'db'
    ? Object.keys(infoData.value).filter((item: string) => !DBCustomedFiled.includes(item))
    : displayMap[infoType.value]
);
const leftField = computed(() => infoField.value.filter((_, index: number) => index % 2 == 0));
const rightField = computed(() => infoField.value.filter((_, index: number) => index % 2));
const tmqDNS = computed(() => {
  // return this.$store.state.app.current_cluster.urlPath;
  const gatewayURL = instance.gatewayUrl;
  const wsPrefix = gatewayURL.startsWith('https') ? 'wss' : 'ws';
  const uri = gatewayURL.replace(/https?:\/\//, '');
  const token = instance.token || '';
  const user = instance.user || '';
  const password = instance.password || '';
  const dbName = currentInfoData.db.name;
  if (project.isCloud && instance.token) {
    return `taos+${wsPrefix}://${uri}/${dbName}?token=${token}`;
  }
  return `taos+${wsPrefix}://${user}:${password}@${uri}/${dbName}`;
});
watch(
  () => infoData.value.name,
  () => {
    getStruct();
  }
);

function getStruct() {
  switch (infoType.value) {
    case 'stb':
      getStableStruct();
      break;
    case 'tb':
      getTableStruct();
      break;
    default:
      break;
  }
}
async function getStableStruct() {
  // 当为超级表的时候只需要获取结构的类型就可以了
  const data = await getStableStructReq(infoData.value.parent, infoData.value.name).catch(() => ({
    ts_field_name: '',
    columns: [],
    tags: []
  }));
  columns.value = processTagAndColumnData(data.columns);
  tags.value = processTagAndColumnData(data.tags);
}
async function getTableStruct() {
  const data = await getSubtbCurrentStruct(currentInfoData.db.name, '', infoData.value.name);
  tags.value = processTagAndColumnData(data.tags);
  columns.value = processTagAndColumnData(data.columns);
}

function processTagAndColumnData(data: Recordable[]) {
  return data.map((item: Recordable) => ({
    name: item.field,
    value: item.type
  }));
}
function copyDNS() {
  copy(tmqDNS.value);
}
</script>

<style lang="scss" scoped>
.info {
  height: 100%;

  .info-content {
    display: flex;
    justify-content: space-between;

    .left,
    .right {
      width: 50%;
      padding-left: 10px;
    }

    &:deep(.el-form-item) {
      margin-bottom: 8px;
    }
  }

  &:deep(.el-form-item__label) {
    font-size: 16px;
    line-height: 20px !important;
  }

  &:deep(.el-form-item__content) {
    font-size: 16px;
    line-height: 22px;
  }

  &:deep(.el-table) {
    margin-top: -6px;

    & th.el-table__cell {
      cursor: unset;
    }

    & th.el-table__cell > .cell {
      padding-left: 6px;
      font-size: 16px;
      font-weight: 500;
    }

    & td.el-table__cell > .cell {
      padding-left: 6px;
      font-size: 16px;
    }
  }
}

.tmp-label {
  &:deep(.el-form-item__label) {
    line-height: 40px !important;
  }

  &:deep(.el-form-item__content) {
    display: flex;
    align-items: center;
  }

  .tmp-dns {
    height: 40px;
    line-height: 40px;
  }
}
</style>
