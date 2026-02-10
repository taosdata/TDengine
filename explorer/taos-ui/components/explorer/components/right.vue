<template>
  <div class="part">
    <div id="part-top" class="part-top">
      <div v-show="partActiveTab == 'sql'" class="sql-btn">
        <el-tooltip class="item" effect="light" placement="bottom-end">
          <template #content>
            <div class="flex-center">
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
          </section>
        </el-tab-pane>

        <!-- 新增：Favorites tab -->
        <el-tab-pane name="favorites" :label="t('explorer.favorites')">
          <template #label>
            <div class="flex-center">
              <span>{{ t('explorer.favorites') }}</span>
            </div>
          </template>
          <div class="favorites-wrapper">
            <component :is="currentFavoriteComponent" v-if="currentFavoriteComponent"></component>
          </div>
        </el-tab-pane>

        <!-- 新增：Log tab -->
        <el-tab-pane name="log" :label="t('common.logs')">
          <template #label>
            <div class="flex-center">
              <span>{{ t('common.logs') }}</span>
            </div>
          </template>
          <LogView></LogView>
        </el-tab-pane>

        <el-tab-pane v-if="tabName" name="detail" :label="tabName">
          <Detail>
            <slot name="detail"></slot>
          </Detail>
        </el-tab-pane>
      </el-tabs>
    </div>
    <div id="bar" class="bar"></div>
    <div id="part-bottom" class="part-bottom">
      <PanelView></PanelView>
    </div>
  </div>
</template>

<script lang="ts" setup>
import Detail from './detail.vue';
import Sql from './sqlEditor.vue';
import PanelView from './panel.vue';
import LogView from './log.vue';
import FavoriteView from './favoriteList/cloud/index.vue';
import EnterpriseFavoriteView from './favoriteList/enterprise/index.vue';
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

import { t } from 'locales';

const tabName = computed(() => currentDetailComponentConfig.name);
const { sqlStr, sqlExecuting } = getSqlProvider();
const { favorite, isCloud } = getExplorerProps();
const sqlEditorRef = ref<null | InstanceType<typeof Sql>>(null);
const unsubscribe = updateFavoriteEvent.on(async () => await getFavorites());
const favorited = computed<Recordable | null>(() => {
  const current = normalizeSql(sqlStr.value || '');
  const all = [...favoriteData.personal, ...favoriteData.shared];
  const match = all.find(item => {
    const itemNorm = normalizeSql(item.sql);
    return itemNorm === current;
  });
  return match || null;
});

// 选择收藏组件
const currentFavoriteComponent = computed(() =>
  isCloud ? (FavoriteView as typeof FavoriteView) : (EnterpriseFavoriteView as typeof EnterpriseFavoriteView)
);

async function getFavorites() {
  return isCloud ? await getCloudFavorites() : await getEnterpriseFavorites();
}
async function getCloudFavorites() {
  return Promise.all([
    favorite.api.getList().then((data: Recordable[]) => {
      favoriteData.personal.splice(0, favoriteData.personal.length, ...data);
    }),
    favorite.api.getSharedList().then((data: Recordable[]) => {
      favoriteData.shared.splice(0, favoriteData.shared.length, ...data);
    })
  ]);
}
async function getEnterpriseFavorites() {
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
  dragChangeHeight('bar', 'part-top', 'part-bottom');
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
function dragChangeHeight(dragId: string, topId: string, bottomId: string) {
  const dragEl = document.getElementById(dragId);
  const topEl = document.getElementById(topId);
  const bottomEl = document.getElementById(bottomId);
  if (!dragEl || !topEl || !bottomEl) return;
  
  dragEl.onmousedown = ev => {
    const startY = ev.clientY;
    const startTopHeight = topEl.offsetHeight;
    const startBottomHeight = bottomEl.offsetHeight;
    
    document.onmousemove = ev => {
      const delta = ev.clientY - startY;
      const newTopH = startTopHeight + delta;
      const newBottomH = startBottomHeight - delta;
      
      // 限制最小高度
      if (newTopH < 150 || newBottomH < 150) return;
      
      // 使用实时的容器总高度计算 flex 比例
      const containerHeight = topEl.parentElement!.offsetHeight;
      const totalAvailableHeight = containerHeight - 8; // 减去 bar 的高度
      
      const topFlex = newTopH / totalAvailableHeight;
      const bottomFlex = newBottomH / totalAvailableHeight;
      
      topEl.style.flex = String(topFlex);
      topEl.style.height = 'auto';
      bottomEl.style.flex = String(bottomFlex);
      bottomEl.style.height = 'auto';
    };
    
    document.onmouseup = () => {
      document.onmousemove = document.onmouseup = null;
    };
    return false;
  };
}

async function toggleFavorite() {
  if (favorited.value) {
    await favorite.api
      .delete(favorited.value.id)
      .then(() => {
        ElMessage.success(t('msg.deleteSuccess'));
      })
      .catch(() => {});
    await getFavorites();
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
  return sql.trim();
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

.flex-center {
  height: 100%;
}

.part {
  display: flex;
  flex-direction: column;
  flex: 1;
  height: 100%;
  overflow: hidden;
  gap: 0;
  background-color: #f2f3f3;
}

.part-top {
  position: relative;
  flex: 1;
  min-height: 150px;
  overflow: hidden;
  border-radius: 6px;
  border: 1px solid #dcdfe6;
  background-color: #ffffff;

  &:deep(.el-tabs) {
    height: 100%;
    border-radius: 6px;
    border: none;
    background-color: #f2f3f3;
  }

  &:deep(.el-tabs__header) {
    flex-shrink: 0;
    border-radius: 6px 6px 0 0;
    background-color: #f2f3f3;
    margin: 0;
  }

  &:deep(.el-tabs--border-card) {
    border-radius: 6px;
    border: none;
    box-shadow: none;
  }

  // 修复左上角圆角
  &:deep(.el-tabs--border-card > .el-tabs__header .el-tabs__item:first-child) {
    border-top-left-radius: 6px;
  }

  &:deep(.el-tab-pane) {
    height: 100%;
  }

  &:deep(.el-tabs__content) {
    flex: 1;
    padding: 10px 0 0;
    overflow: auto;
    border-radius: 0;
    background-color: #ffffff;
  }

  &:deep(.el-tabs__header) {
    flex-shrink: 0;
  }
}

.part-bottom {
  flex: 1;
  min-height: 200px;
  overflow: hidden;
  border-radius: 6px;
  border: 1px solid #dcdfe6;
  background-color: #ffffff;
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
  padding: 0;
}

.icon-shift {
  width: 20px;
  height: 20px;
}

.bar {
  flex-shrink: 0;
  width: 100%;
  height: 8px;
  margin: 0;
  cursor: n-resize;
  background-color: #f2f3f3;
  transition: background-color 0.2s;
}

.bar:hover {
  background-color: $bar-light-color;
}

.favorites-wrapper {
  padding: 0px 5px 0px 5px;
  height: 100%;
  box-sizing: border-box;
  overflow: auto;

  &:deep(.el-input) {
    margin-left: 12px;
  }
}

.tab-icon {
  width: 19px;
  height: 19px;
  margin-right: 5px;
  cursor: pointer;
}

:deep(.el-tabs__item.is-active) {
  border: 1px solid #dcdfe6;
}

/* 前三个 tab 未选中时文字为黑色 */
:deep(.el-tabs--border-card > .el-tabs__header .el-tabs__item:not(.is-active)) {
  color: #303133 !important;
}

.log {
  border: none;
}
</style>
