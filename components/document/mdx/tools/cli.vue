<template>
  <div>
    <p>{{ t('tools.cli.topdesc') }}</p>
    <tdclient></tdclient>
    <h2 id="connect">{{ t('tools.cli.step3') }}</h2>
    <el-tabs model-value="linux" group-id="sys">
      <el-tab-pane name="linux" label="Linux">
        <p>{{ t('tools.cli.step3desc') }}</p>
        <pre v-highlight><code>taos
</code></pre>
        <p>{{ t('tools.cli.step3desc1') }}</p>
        <pre v-highlight><code>taos -E ${{dsnKey}}
</code></pre>
      </el-tab-pane>
      <el-tab-pane name="windows" label="Windows">
        <p>{{ t('tools.cli.step3desc') }}</p>
        <pre v-highlight><code>taos
</code></pre>
        <p>{{ t('tools.cli.step3desc1') }}</p>
        <pre v-highlight><code>taos.exe -E %{{dsnKey}}%
</code></pre>
      </el-tab-pane>
      <el-tab-pane name="mac" label="Mac">
        <p>{{ t('tools.cli.step3desc') }}</p>
        <pre v-highlight><code>taos
</code></pre>
        <p>{{ t('tools.cli.step3desc1') }}</p>
        <pre v-highlight><code>taos -E ${{dsnKey}}
</code></pre>
      </el-tab-pane>
    </el-tabs>

    <h2 id="using-tdengine-cli">{{ t('tools.cli.step4') }}</h2>
    <p>{{ t('tools.cli.step4desc') }}</p>
    <pre v-highlight><code>Welcome to the TDengine shell from Linux, Client Version:{{ instance.version }}
Copyright (c) 2022 by TAOS Data, Inc. All rights reserved.

Successfully connect to cloud.tdengine.com:8085 in restful mode

taos&gt;
</code></pre>
    <p>
      {{ t('tools.cli.step4desc1') }}
      <a :href="`${TdDocsUrl}/reference/taos-shell#execute-sql-script-file`">{{ t('tools.cli.step4desc2') }}</a>
      {{ t('tools.cli.step4desc3') }}
    </p>
  </div>
</template>

<script lang="ts" setup>
import tdclient from './tdclient.vue';
import { t } from 'locales';
import { dsnKey } from '../utils';
import { TdDocsUrl, instance } from 'config';
</script>
