<template>
  <div class="part">
    <div v-show="partActiveTab == 'sql'" class="sql-btn">
      <el-tooltip class="item" effect="light" placement="bottom-end">
        <template #content>
          <div class="flexCenter">
            <span>{{ t('explorer.runSqlTip') }}</span>
            <Icon class="icon-shift" name="shift" />+
            <Icon class="icon-shift" name="enter" />
          </div>
        </template>
        <el-button
          :disabled="!sqlStr || sqlExecuting"
          type="primary"
          icon="caretRight"
          :loading="sqlExecuting"
          size="small"
          @click="executeSql"
        >
          <span>{{ t('common.run') }}</span>
        </el-button>
      </el-tooltip>
      <el-button :disabled="!sqlStr || sqlExecuting" type="warning" icon="SetUp" size="small" @click="formatSql">
        <span>{{ t('common.format') }}</span>
      </el-button>
      <el-tooltip
        effect="light"
        :content="t('explorer.' + (favorited ? 'deleteCurrentSavedSql' : 'saveCurrentSqlAsFavorite'))"
      >
        <el-button :disabled="!sqlStr || sqlExecuting || favorited" type="success" size="small" @click="toggleFavorite">
          <template v-if="!favorited">
            <el-icon :size="14">
              <Star></Star>
            </el-icon>
            <span class="add_favorite_text">{{ t('explorer.favorite') }}</span>
          </template>
          <template v-else>
            <el-icon :size="14">
              <StarFilled />
            </el-icon>
            <span class="add_favorite_text">{{ t('status.saved') }}</span>
          </template>
        </el-button>
      </el-tooltip>
    </div>
    <el-tabs v-model="partActiveTab" tab-position="top" type="border-card">
      <el-tab-pane name="sql" label="SQL">
        <section class="sql-wrapper">
          <Sql id="sql" ref="sqlEditorRef"></Sql>
          <div id="bar" class="bar"></div>
          <PanelView></PanelView>
        </section>
      </el-tab-pane>
      <el-tab-pane v-if="tabName" name="detail" :label="tabName">
        <Detail>
          <slot name="detail"></slot>
        </Detail>
      </el-tab-pane>
    </el-tabs>
  </div>
</template>

<script lang="ts" setup>
import Detail from './detail.vue';
import Sql from './sqlEditor.vue';
import PanelView from './panel.vue';
import { getSqlProvider, getExplorerProps } from '../model/useExplorer';
import {
  currentDetailComponentConfig,
  favoriteData,
  updateFavoriteEvent,
  partActiveTab,
  panelActiveTab,
  favoriteParams,
  favoriteActiveTab
} from './utils';
import { ElMessage, ElMessageBox } from 'element-plus';
import { t } from 'locales';

const tabName = computed(() => currentDetailComponentConfig.name);
const { sqlStr, sqlExecuting } = getSqlProvider();
const { favorite, isCloud } = getExplorerProps();
const sqlEditorRef = ref<null | InstanceType<typeof Sql>>(null);
const unsubscribe = updateFavoriteEvent.on(() => getFavorites());
const favorited = computed<Recordable | null>(() => {
  const current = normalizeSql(sqlStr.value || '');
  // 在 personal 与 shared 中同时查找，避免 favoriteActiveTab 不同导致找不到
  const all = [...favoriteData.personal, ...favoriteData.shared];
  const match = all.find(item => {
    const itemNorm = normalizeSql(item.sql);
    // 调试输出
    // console.log('compare:', current, itemNorm, current === itemNorm, item.sql, sqlStr.value);
    return itemNorm === current;
  });
  return match || null;
});

function getFavorites() {
  return isCloud ? getCloudFavorites() : getEnterpriseFavorites();
}
function getCloudFavorites() {
  return Promise.all([
    favorite.api.getList().then((data: Recordable[]) => {
      favoriteData.personal.splice(0, favoriteData.personal.length, ...data);
    }),
    favorite.api.getSharedList().then((data: Recordable[]) => {
      favoriteData.shared.splice(0, favoriteData.shared.length, ...data);
    })
  ]);
}
function getEnterpriseFavorites() {
  if (favoriteActiveTab.value == 'personal') {
    return favorite.api.getList(favoriteParams).then((res: Recordable) => {
      favoriteData.personal.splice(0, favoriteData.personal.length, ...res.data.list);
      favoriteData.total = res.data.total;
    });
  } else {
    return favorite.api.getSharedList(favoriteParams).then((res: Recordable) => {
      favoriteData.shared.splice(0, favoriteData.shared.length, ...res.data.list);
      favoriteData.total = res.data.total;
    });
  }
}
onMounted(() => {
  dragChangeHeight('bar', 'sql');
});
onBeforeUnmount(() => {
  unsubscribe();
});
function executeSql() {
  sqlEditorRef.value?.handleExecute();
}
function formatSql() {
  sqlEditorRef.value?.handleFormat();
}
function dragChangeHeight(drag: string, panel: string) {
  const dragEl = document.getElementById(drag);
  const panelEl = document.getElementById(panel);
  if (!dragEl || !panelEl) return;
  dragEl.onmousedown = ev => {
    const disH = panelEl.offsetHeight;
    const disY = ev.clientY;
    document.onmousemove = ev => {
      panelEl.style.height = disH + (ev.clientY - disY) + 'px';
    };
    document.onmouseup = () => {
      document.onmousemove = document.onmouseup = null;
    };
    return false;
  };
}

async function toggleFavorite() {
  if (favorited.value) {
    // favorited 现在是对象
    await favorite.api
      .delete(favorited.value.id)
      .then(() => {
        ElMessage.success(t('msg.deleteSuccess'));
      })
      .catch(() => {});
    await getFavorites(); // 刷新收藏列表
  } else if (isCloud) {
    await favorite.api
      .add(normalizeSql(sqlStr.value))
      .then(() => {
        ElMessage.success(t('msg.addSuccess'));
      })
      .catch(() => {});
    await getFavorites();
  } else {
    await addDesc();
    await getFavorites();
  }
  panelActiveTab.value = 'favorites';
}

function normalizeSql(sql: string) {
  return sql.replace(/\s+/g, '').toLowerCase();
}

function addDesc() {
    return new Promise((resolve, reject) => {
    ElMessageBox.prompt('', t('explorer.addDesc'), {
      closeOnClickModal: false,
      confirmButtonText: t('common.confirm'),
      cancelButtonText: t('common.cancel'),
      inputPattern: /^.{0,20}$/,
      inputErrorMessage: t('explorer.characterLen', ['20']),
      inputPlaceholder: t('explorer.descPlaceholder', ['20'])
    })
      .then(({ value }) => {
        const params = {
          sql: normalizeSql(sqlStr.value),
          description: value
        };
        favorite.api
          .add(params)
          .then(() => {
            ElMessage.success(t('msg.addSuccess'));
            resolve(undefined);
          })
          .catch(err => {
            reject(err);
          });
      })
      .catch(err => {
        reject(err);
      });
  });
}
</script>

<style lang="scss" scoped>
$bar-color: #f5f5f5;
$bar-light-color: #dcdfe6;

.part {
  position: relative;
  flex: 1;
  height: 100%;
  overflow-x: hidden;

  &:deep(.el-tabs) {
    height: 100%;
  }

  &:deep(.el-tab-pane) {
    height: 100%;
  }

  &:deep(.el-tabs__content) {
    flex: 1;
    padding: 15px 0;
    overflow: auto;
  }

  &:deep(.el-tabs__header) {
    flex-shrink: 0;
  }
}

.sql-btn {
  position: absolute;
  top: 8px;
  right: 20px;
  z-index: 20;
}

.sql-wrapper {
  display: flex;
  flex-direction: column;
  height: 100%;
}

.icon-shift {
  width: 20px;
  height: 20px;
}

.bar {
  flex-shrink: 0;
  width: 100%;
  height: 10px;
  cursor: n-resize;
  background-color: $bar-color;
}

.bar:hover {
  background-color: $bar-light-color;
}
</style>
