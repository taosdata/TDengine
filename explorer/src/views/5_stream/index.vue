<template>
  <div class="page-wrapper">
    <!-- <PageHeader :title="$t('stream.pageTitle')"></PageHeader> -->
    <section class="content">
      <div>
        <div class="flex-end">
          <el-button
            v-permission
            class="big-button"
            plain
            type="primary"
            size="default"
            icon="Plus"
            @click="dialog = true"
            >{{ $t('stream.createStream') }}</el-button
          >
        </div>
        <el-table style="margin-top: 20px" size="default" :data="streamList">
          <el-table-column :label="$t('stream.streamName')" width="200" prop="stream_name">
            <template #default="scope">
              <el-tooltip :content="scope.row.stream_name" placement="top-start">
                <span class="nowrap">{{ scope.row.stream_name }}</span>
              </el-tooltip>
            </template>
          </el-table-column>
          <el-table-column :label="$t('createTime')" width="210" prop="create_time">
            <template #default="scope">
              <span>{{ parsinginZone(scope.row.create_time) }}</span>
            </template>
          </el-table-column>
          <el-table-column label="sql" min-width="200" prop="sql">
            <template #default="scope">
              <el-tooltip :content="scope.row.sql" placement="top-start">
                <pre v-highlight class="nowrap sql-code pre-code">
                  <code class="language-sql" style="overflow:hidden">{{ scope.row.sql }} </code>
                </pre>
              </el-tooltip>
            </template>
          </el-table-column>
          <el-table-column width="120" :label="$t('status')" prop="status">
            <template #default="scope">
              <el-tooltip :content="scope.row.status" placement="top-start">
                <span class="nowrap">{{ scope.row.status }}</span>
              </el-tooltip>
            </template>
          </el-table-column>
          <el-table-column width="120" :label="$t('stream.database')" prop="source_db">
            <template #default="scope">
              <el-tooltip :content="scope.row.db_name" placement="top-start">
                <span class="nowrap">{{ scope.row.db_name }}</span>
              </el-tooltip>
            </template>
          </el-table-column> 

          <el-table-column :label="$t('operate')" width="80">
            <template #default="scope">
              <el-button plain size="small" icon="Delete" @click="del(scope.row)"></el-button>
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
      </div>
    </section>
    <el-dialog
      v-model="dialog"
      :close-on-click-modal="false"
      align="center"
      :title="$t('stream.createStream')"
      width="800px"
      :destroy-on-close="true"
      @close="closeDialog"
    >
      <AddForm ref="stream" type="stream" :stream-list="streamList" @close="close" />
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import AddForm from './components/addStream.vue';
import { getStreams, delStream } from '@/api/stream.ts';
import { compareVersion, parsinginZone } from '@/utils';
const { $IS_OEM, $error } = inject('globalCustomProperties') as GlobalCustomProperties;
const { t } = useI18n();
const name = 'Stream';
provide('parentName', name);

const dialog = ref<boolean>(false);
const streamList = ref([]);
const requestIng = ref<boolean>(false);
const currentPage = ref(1);
const pageSize = ref(10);
const total = ref(0);

const TDVersion = localStorage.getItem('td_version') || '';

const learnMoreTip = computed(() => {
  return t('stream.learnMoreTip').replace(/docsUrl/, `${t('urlPart')}/develop/stream/`);
});

function closeDialog() {
  dialog.value = false;
}
async function getStreamsData() {
  if (requestIng.value) return;
  requestIng.value = true;
  [streamList.value, total.value] = await getStreams({ currentPage: currentPage.value, pageSize: pageSize.value });
  requestIng.value = false;
}
function del(data: any) {
  if (requestIng.value) return;
  ElMessageBox.confirm(t('stream.delStream') + '：' + data.stream_name + '?', t('tips'), {
    confirmButtonText: t('confirm'),
    cancelButtonText: t('cancel'),
    type: 'warning'
  }).then(async () => {
    requestIng.value = true;
    let streamName = `\`${data.stream_name}\``;
    if (compareVersion(TDVersion, '>=3.3.7.0')) {
      streamName = `\`${data.db_name}\`.\`${data.stream_name}\``;
    }
    await delStream(streamName)
      .then(() => {
        ElMessage.success(t('delSucc'));
      })
      .finally(() => {
        requestIng.value = false;
        currentPage.value = 1;
        getStreamsData();
      })
      .catch(res => {
        $error(res?.desc);
      });
  });
}
function handlePageChange() {
  getStreamsData();
}
function close() {
  getStreamsData();
  dialog.value = false;
}

getStreamsData();
</script>

<style lang="scss">
.sql-code {
  position: relative;
  padding: 3px 0;
  font-size: 16px;
  text-align: left;
}

</style>
