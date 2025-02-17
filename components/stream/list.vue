<template>
  <section>
    <div class="flex-end">
      <el-button :disabled="!props.isCanCreate" plain icon="plus" @click="add">{{
        t('stream.createNewStream')
      }}</el-button>
    </div>
    <el-table class="mt-20px" :data="streamList">
      <el-table-column show-overflow-tooltip :label="t('common.name')" width="200" prop="stream_name"></el-table-column>
      <el-table-column :label="t('date.createTime')" width="240" prop="create_time"> </el-table-column>
      <el-table-column label="sql" min-width="200" prop="sql" show-overflow-tooltip>
        <template #default="scope">
          <pre
            :key="scope.row.stream_name"
            v-highlight
            class="pre-code no-wrap"
          ><code class="language-sql">{{ scope.row.sql }} </code>
        </pre>
        </template>
      </el-table-column>
      <el-table-column width="100" :label="t('common.status')" prop="status"></el-table-column>
      <el-table-column show-overflow-tooltip width="120" :label="t('db.source')" prop="source_db"></el-table-column>
      <el-table-column show-overflow-tooltip width="120" :label="t('db.target')" prop="target_db"></el-table-column>
      <el-table-column
        show-overflow-tooltip
        width="120"
        :label="t('stb.targetTable')"
        prop="target_table"
      ></el-table-column>
      <el-table-column width="100" label="watermark" prop="watermark"></el-table-column>
      <el-table-column width="120" :label="t('stream.trigger')" prop="trigger"></el-table-column>

      <el-table-column :label="t('common.action')" width="80">
        <template #default="scope">
          <el-button plain size="small" icon="delete" @click="del(scope.row)"></el-button>
        </template>
      </el-table-column>
    </el-table>
    <p v-dompurify-html="learnMoreTip" class="default-tip"></p>
  </section>
</template>

<script lang="ts" setup>
import { getStreams, delStream, streamList } from './api';
import { TdDocsUrl } from 'config';
import { ElMessage, ElMessageBox } from 'element-plus';
import { t } from 'locales';
import { useRouter } from 'hooks/useCurrentRouter';

const props = withDefaults(
  defineProps<{
    isCanCreate?: boolean;
  }>(),
  {
    isCanCreate: true
  }
);
const loading = ref(false);
const router = useRouter();
const learnMoreTip = computed(() => t('stream.learnMoreTip').replace(/docsUrl/, TdDocsUrl.value + '/cloud/stream/'));

getData();
function getData() {
  loading.value = true;
  getStreams()
    .then(data => {
      streamList.value = data;
    })
    .finally(() => {
      loading.value = false;
    });
}

function add() {
  router.push('/stream/create');
}

function del(data: Recordable) {
  if (loading.value) return;
  ElMessageBox.confirm(t('stream.delTip', [data.stream_name]), t('status.warning'), {
    confirmButtonText: t('common.confirm'),
    cancelButtonText: t('common.cancel'),
    type: 'warning'
  }).then(async () => {
    loading.value = true;
    await delStream(data.stream_name)
      .then(() => {
        ElMessage.success(t('msg.deleteSuccess'));
      })
      .catch(err => {
        err.desc && ElMessage.error(err.desc);
      })
      .finally(() => {
        loading.value = false;
        getData();
      });
  });
}
</script>

<style lang="scss" scoped></style>
