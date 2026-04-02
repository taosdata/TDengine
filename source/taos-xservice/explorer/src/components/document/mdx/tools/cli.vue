<template>
  <div>
    <p>{{ $t('docs.tool.cli.topdesc') }}</p>
    <h2 id="installation">{{ $t('docs.tool.cli.step1') }}</h2>
    <p>
      {{ $t('docs.tool.cli.step1desc') }}
      <b>{{ $t('docs.tool.cli.step1desc1') }}</b
      >&nbsp;{{ $t('docs.tool.cli.step1desc2') }}<a :href="installUrlLinux">Linux</a>{{ $t('docs.tool.cli.step1desc3')
      }}<a :href="installUrlWindows">Windows</a>{{ $t('docs.tool.cli.step1desc3') }}
      <a :href="installUrlMac">MacOS-x64</a>{{ $t('docs.tool.cli.step1desc3') }}
      <a :href="installUrlMacArm">MacOS-arm64</a>{{ $t('docs.tool.cli.step1desc4') }}
    </p>
    <h2 id="config">{{ $t('docs.tool.cli.step2') }}</h2>
    <el-tabs v-model="sysActivateTab" group-id="sys">
      <el-tab-pane name="linux" label="Linux">
        <p>{{ $t('docs.tool.cli.step2desc') }} <span class="docker-tip"></span></p>
        <p>
          <el-icon color="gold" :size="20">
            <Opportunity />
          </el-icon>
          <span class="docker-tip">{{ $t('dockerTip', [`${url.split('//')[1]}`]) }}</span>
        </p>
        <pre
          v-highlight="
            `export TDENGINE_DSN=&quot;${dsn}&quot;
`
          "
        ><code class="language-bash"></code></pre>
      </el-tab-pane>
      <el-tab-pane name="windows" label="Windows">
        <p>{{ $t('docs.tool.cli.step2desc1') }}</p>
        <p>
          <el-icon color="gold" :size="20">
            <Opportunity />
          </el-icon>
          <span class="docker-tip">{{ $t('dockerTip', [`${url.split('//')[1]}`]) }}</span>
        </p>
        <pre
          v-highlight="
            `set TDENGINE_DSN=${dsn}
`
          "
        ><code class="language-bash"></code></pre>
        <p>{{ $t('docs.tool.cli.step2desc2') }}</p>
        <p>
          <el-icon color="gold" :size="20">
            <Opportunity />
          </el-icon>
          <span class="docker-tip">{{ $t('dockerTip', [`${url.split('//')[1]}`]) }}</span>
        </p>
        <pre
          v-highlight="
            `$env:TDENGINE_DSN='${dsn}'
`
          "
        ><code class="language-bash"></code></pre>
      </el-tab-pane>
      <el-tab-pane name="mac" label="Mac" group-id="sys">
        <p>{{ $t('docs.tool.cli.step2desc3') }}</p>
        <p>
          <el-icon color="gold" :size="20">
            <Opportunity />
          </el-icon>
          <span class="docker-tip">{{ $t('dockerTip', [`${url.split('//')[1]}`]) }}</span>
        </p>
        <pre
          v-highlight="
            `export TDENGINE_DSN=&quot;${dsn}&quot;
`
          "
        ><code class="language-bash"></code></pre>
      </el-tab-pane>
    </el-tabs>

    <h2 id="connect">{{ $t('docs.tool.cli.step3') }}</h2>
    <el-tabs model-value="linux" group-id="sys">
      <el-tab-pane name="linux" label="Linux">
        <p>{{ $t('docs.tool.cli.step3desc1') }}</p>
        <pre v-highlight><code>taos -E $TDENGINE_DSN
</code></pre>
      </el-tab-pane>
      <el-tab-pane name="windows" label="Windows">
        <p>{{ $t('docs.tool.cli.step3desc1') }}</p>
        <pre v-highlight><code>taos.exe -E %TDENGINE_DSN%
</code></pre>
      </el-tab-pane>
      <el-tab-pane name="mac" label="Mac">
        <p>{{ $t('docs.tool.cli.step3desc1') }}</p>
        <pre v-highlight><code>taos -E $TDENGINE_DSN
</code></pre>
      </el-tab-pane>
    </el-tabs>

    <h2 id="using-tdengine-cli">{{ $t('docs.tool.cli.step4') }}</h2>
    <p>{{ $t('docs.tool.cli.step4desc') }}</p>
    <pre v-highlight><code>{{ code }}
</code></pre>
    <p>{{ $t('docs.tool.cli.step4desc1') }}&nbsp;</p>
  </div>
</template>

<script setup lang="ts">
import { DocsProps } from '../utils';

defineProps<DocsProps>();
const sysActivateTab = ref('linux');
const version = localStorage.getItem('td_version');
const code = ref(`Welcome to the TDengine Command Line Interface, Client Version:${version}
Copyright (c) 2023 by TDengine, Inc. all rights reserved.

taos>`);
</script>
