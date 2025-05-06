<template>
  <div>
    <div class="flex-end">
      <el-button plain :disabled="requestIng" icon="refresh" @click="emits('update')">{{
        t('common.refresh')
      }}</el-button>
      <el-button v-permission:ins="['db:create']" plain icon="plus" @click="add">{{ t('topic.create') }}</el-button>
    </div>
    <el-table v-loading="requestIng" class="mt-20px" :data="props.topicList">
      <el-table-column show-overflow-tooltip width="120" :label="t('common.name')" prop="topicName"></el-table-column>
      <el-table-column show-overflow-tooltip width="120" :label="t('common.database')" prop="dbName"></el-table-column>

      <el-table-column show-overflow-tooltip min-width="150" label="SQL" prop="topicSql">
        <template #default="scope">
          <pre :key="scope.row.topicId" v-highlight class="no-wrap topic-sql pre-code">
          <code class="language-sql" style="overflow:hidden">{{ scope.row.topicSql }} </code>
        </pre>
        </template>
      </el-table-column>
      <el-table-column label="DSN" show-overflow-tooltip min-width="150">
        <template #default="{ row }">
          <TextCopy :text="dsn(row.topicName)" style="line-height: inherit" />
        </template>
      </el-table-column>
      <el-table-column width="166" :label="t('common.expiration')" prop="privilegeExpirationTime"></el-table-column>
      <el-table-column width="166" :label="t('date.createTime')" prop="createTime"> </el-table-column>
      <el-table-column width="150" :label="t('common.createBy')" show-overflow-tooltip prop="dbName">
        <template #default="{ row }">
          {{ t('common.usernameTemp', [row.createUserFirstName, row.createUserLastName]) }}
        </template>
      </el-table-column>
      <el-table-column :label="t('common.action')" width="140">
        <template #default="scope">
          <el-tooltip effect="light" :content="t('common.sampleCode')" placement="top">
            <el-button size="small" icon="document" plain @click="document(scope.row)"></el-button>
          </el-tooltip>
          <el-tooltip
            v-if="user.id == scope.row.createBy"
            effect="light"
            :content="t('topic.shareTopic')"
            placement="top"
          >
            <el-button icon="share" plain @click="manage(scope.row)"></el-button>
          </el-tooltip>
          <el-tooltip
            v-if="user.id == scope.row.createBy"
            effect="light"
            :content="t('topic.delTooltip')"
            placement="top"
          >
            <el-button icon="delete" plain @click="del(scope.row)"></el-button>
          </el-tooltip>
        </template>
      </el-table-column>
    </el-table>
    <p v-dompurify-html="learnMoreTip" class="default-tip"></p>
  </div>
</template>

<script lang="ts" setup>
import { user, instance } from 'config';
import { deleteTopic } from '../api';
import { TdDocsUrl } from 'config';
import { ElMessage, ElMessageBox } from 'element-plus';
import { t } from 'locales';
import { useRouter } from 'hooks/useCurrentRouter';

const props = defineProps<{
  topicList: Recordable[];
}>();
const router = useRouter();
const requestIng = ref(false);
const learnMoreTip = computed(() => {
  return t('topic.learnMoreTip').replace(/docsUrl/, TdDocsUrl + '/cloud/data-subscription/');
});
const emits = defineEmits(['update', 'add']);

function add() {
  emits('add');
}
function del(data: Recordable) {
  if (requestIng.value) return;
  ElMessageBox.confirm(t('topic.delTip', [data.topicName]), t('status.warning'), {
    confirmButtonText: t('common.confirm'),
    cancelButtonText: t('common.cancel'),
    type: 'warning'
  }).then(async () => {
    requestIng.value = true;
    await deleteTopic(data.topicId)
      .then(() => {
        ElMessage.success(t('msg.deleteSuccess'));
      })

      .finally(() => {
        requestIng.value = false;
        emits('update');
      });
  });
}

function manage(data: Recordable) {
  router.push({
    path: '/topic/share',
    query: {
      topicId: data.topicId
    }
  });
}
function document(data: Recordable) {
  router.push({
    path: '/topic/example',
    query: {
      topicId: data.topicId
    }
  });
}
function dsn(topicName: string) {
  const wsPrefix = instance.gatewayUrl.startsWith('https') ? 'wss' : 'ws';
  const uri = instance.gatewayUrl.replace(/https?:\/\//, '');
  return `tmq+${wsPrefix}://${uri}/${topicName}?token=${instance.token}`;
}
</script>

<style lang="scss"></style>
