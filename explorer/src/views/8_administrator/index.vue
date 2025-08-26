<template>
  <div class="page-wrapper">
    <!-- <PageHeader :title="$t('route.admin')"></PageHeader> -->
    <div class="content">
      <el-tabs v-model="activeName">
        <el-tab-pane name="user" :label="$t('taosuser.users')">
          <MgUser></MgUser>
        </el-tab-pane>
        <el-tab-pane v-if="getMetaShow('backup_restore')" name="backup" :label="$t('taosuser.backup')" lazy>
          <AdBackup></AdBackup>
        </el-tab-pane>
        <el-tab-pane
          v-if="isLessThan3_3_2_12 ? getMetaShow('td3.0') : getMetaShow('data_sync')"
          name="replication"
          :label="$t('taosuser.datareplication')"
          lazy
        >
          <AdReplication :is-less-than3_3_3_0="isLessThan3330"></AdReplication>
        </el-tab-pane>
        <el-tab-pane v-if="getMetaShow('dnodes')" name="cluster" :label="$t('route.cluster')" lazy>
          <Cluster></Cluster>
        </el-tab-pane>
        <el-tab-pane v-if="$IS_TSDBLITE || !$IS_COMMUNITY" name="license" :label="$t('topic.license')" lazy>
          <License></License>
        </el-tab-pane>
        <el-tab-pane v-if="getMetaShow('audit')" name="audit" :label="$t('topic.audit')" lazy>
          <Audit :active-name="activeName"></Audit>
        </el-tab-pane>
        <el-tab-pane v-if="!isLessThan3330" name="slowSql" :label="$t('topic.slowSql')" lazy>
          <SlowSql :active-name="activeName"></SlowSql>
        </el-tab-pane>
      </el-tabs>
      <el-alert
        v-if="$IS_COMMUNITY && (activeName == 'backup' || activeName == 'replication' || activeName == 'audit')"
        style="margin-top: 8px"
        class="my-alert"
        type="warning"
        :description="$t('communityDemoDataTip')"
        :closable="true"
        center
      />
    </div>
  </div>
</template>
<script setup lang="ts">
import MgUser from './views/user.vue';
import AdBackup from './views/backup.vue';
import AdReplication from './views/replication.vue';
import License from './views/license.vue';
import Cluster from './views/cluster.vue';
import Audit from './views/audit.vue';
import SlowSql from './views/slowSql.vue';
import { compareVersion } from '@/utils/index';
import useLicense from '@/hooks/useLicense.ts';

const globalCustomProperties: any = inject('globalCustomProperties');
const { $IS_COMMUNITY, $IS_TSDBLITE } = globalCustomProperties;
const router = useRouter();
const route = useRoute();
const { getMetaShow } = useLicense();

const activeName: Ref<string> = ref('user');

const TDengineVersion = localStorage.getItem('td_version') || '';

const isLessThan3_3_2_12 = computed(() => {
  return compareVersion(TDengineVersion, '<3.3.2.12');
});

const isLessThan3330 = computed(() => {
  return compareVersion(TDengineVersion, '<3.3.3.0');
});

watch(
  () => route,
  (val: any) => {
    activeName.value = val.name;
  },
  {
    deep: true
  }
);

watch(
  activeName,
  val => {
    router.push('/management/' + val);
  },
  {
    deep: true
  }
);
</script>
<style scoped lang="scss"></style>
