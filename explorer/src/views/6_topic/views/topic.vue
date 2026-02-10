<template>
  <div>
    <div class="flex-end">
      <el-button
        plain
        type="primary"
        size="default"
        icon="Refresh"
        :disabled="requestIng"
        style="font-size: 14px"
        @click="refresh"
        >{{ $t('refresh') }}</el-button
      >
      <el-button class="big-button" plain type="primary" size="default" icon="Plus" @click="add">{{
        $t('topic.createTopic')
      }}</el-button>
    </div>
    <el-table style="margin-top: 20px" :data="topicList" size="small" row-key="topic_name">
      <el-table-column
        width="150"
        :label="$t('topic.topicName')"
        prop="topic_name"
        show-overflow-tooltip
      ></el-table-column>
      <el-table-column width="150" :label="$t('topic.DBName')" prop="db_name" show-overflow-tooltip></el-table-column>
      <el-table-column min-width="200" label="SQL" prop="sql">
        <template #default="scope">
          <el-tooltip placement="left-start" :content="scope.row.sql" popper-class="my-popper" :open-delay="1000">
            <span>
              <pre v-highlight class="nowrap sql-code pre-code">
            <code class="language-sql" style="overflow:hidden">{{ scope.row.sql }} </code>
          </pre>
            </span>
          </el-tooltip>
        </template>
      </el-table-column>
      <el-table-column width="90" :label="$t('getDsn')" prop="dsn">
        <template #default="scope">
          <el-button class="copy-btn" size="small" @click="copyDsn(scope.row.dsn)">
            <el-icon><CopyDocument /></el-icon>
            {{ $t('copy') }}
          </el-button>
        </template>
      </el-table-column>
      <el-table-column width="210" :label="$t('createTime')" prop="create_time" show-overflow-tooltip>
        <template #default="scope">
          <span>{{ parsinginZone(scope.row.create_time) }}</span>
        </template>
      </el-table-column>
      <el-table-column width="50" fixed="right">
        <template #default="scope">
          <div class="operations-wrapper">
            <el-dropdown trigger="hover">
              <el-button icon="MoreFilled" size="small" class="rotate-90!" text></el-button>
              <template #dropdown>
                <el-dropdown-menu>
                  <el-dropdown-item @click="document(scope.row)">
                    <el-icon><CopyDocument /></el-icon>
                    {{ $t('topic.sampleCode') }}
                  </el-dropdown-item>
                  <el-dropdown-item @click="manage(scope.row)">
                    <el-icon><Share /></el-icon>
                    {{ $t('topic.shareTopic') }}
                  </el-dropdown-item>
                  <el-dropdown-item @click="del(scope.row)">
                    <el-icon><Delete /></el-icon>
                    {{ $t('delete') }}
                  </el-dropdown-item>
                </el-dropdown-menu>
              </template>
            </el-dropdown>
          </div>
        </template>
      </el-table-column>
    </el-table>
    <el-pagination
      v-model:current-page="currentPage"
      class="pagination"
      layout="total, prev, pager, next"
      :page-size="pageSize"
      :hide-on-single-page="true"
      :total="total"
      @current-change="handlePageChange"
    >
    </el-pagination>
    <p v-if="!$IS_OEM" v-dompurify-html="learnMoreTip" class="default-tip"></p>
    <el-dialog
      v-model="dialog"
      align="center"
      :close-on-click-modal="false"
      :title="title"
      :width="width"
      :destroy-on-close="true"
      @close="closeDialog"
    >
      <component :is="dialogComp" v-bind="dialogParams" @close="close"></component>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { getTopics, delTopic } from '@/api/topic';
import { getDSN } from '@/utils/index';
import { parsinginZone, copy } from '@/utils';
import AddTopic from '../components/addTopic.vue';
const { $IS_OEM, $error } = inject('globalCustomProperties') as GlobalCustomProperties;

const { t } = useI18n();
const router = useRouter();

const requestIng = ref(false);
const dialog = ref(false);
const topicList = ref([]);
const currentPage = ref(1);
const pageSize = ref(10);
const total = ref(0);
const dialogType = ref('0');
let dialogParams = reactive({ topicList: [] });

const learnMoreTip = computed(() => {
  return t('topic.learnMoreTip').replace(/docsUrl/, `${t('urlPart')}/taos-sql/tmq/#create-a-topic`);
});
const dialogComp = computed(() => {
  return {
    0: AddTopic
  }[dialogType.value];
});
const title = computed(() => {
  return {
    0: t('topic.createTopic'),
    1: t('topic.manageTopic')
  }[dialogType.value];
});
const width = computed(() => {
  return {
    0: '750px',
    1: '380px'
  }[dialogType.value];
});

function closeDialog() {
  dialog.value = false;
}
function refresh() {
  getTopicsData();
}
async function getTopicsData() {
  if (requestIng.value) return;
  requestIng.value = true;
  [topicList.value, total.value] = await getTopics({
    currentPage: currentPage.value,
    pageSize: pageSize.value
  });
  topicList.value.forEach(item => {
    item.dsn = getDSN('tmq') + '/' + item.topic_name;
  });
  requestIng.value = false;
}
function del(data) {
  if (requestIng.value) return;
  ElMessageBox.confirm(t('topic.delTopic') + '：' + data.topic_name + '?', t('tips'), {
    confirmButtonText: t('confirm'),
    cancelButtonText: t('cancel'),
    type: 'warning'
  }).then(async () => {
    requestIng.value = true;
    await delTopic(data.topic_name)
      .then(() => {
        ElMessage.success(t('delSucc'));
      })
      .finally(() => {
        requestIng.value = false;
        currentPage.value = 1;
        getTopicsData();
      })
      .catch(res => {
        $error(res?.desc);
      });
  });
}
function handlePageChange() {
  getTopicsData();
}
function add() {
  dialogType.value = '0';
  dialogParams = { topicList: topicList.value };
  dialog.value = true;
}
function close() {
  dialog.value = false;
  getTopicsData();
}
function manage(data) {
  router.push({
    path: '/topic/share',
    query: {
      topicId: data.topicId
    }
  });
}
function document(data) {
  router.push({
    path: '/topic/example',
    query: {
      topicId: data.topicId
    }
  });
}
function copyDsn(dsn) {
  copy(dsn);
}
getTopicsData();
</script>

<style lang="scss" scoped>
.sql-code {
  position: relative;
  padding: 3px 0;
  font-size: 16px;
  text-align: left;
}

.language-sql {
  white-space: inherit !important;
}

.copy-btn {
  cursor: pointer;
}

.operations-wrapper {
  display: flex;
  justify-content: flex-end;
  align-items: center;
}
</style>

<style lang="scss">
.my-popper {
  max-width: 600px;
  max-height: 600px;
  overflow: hidden auto;
}
</style>
