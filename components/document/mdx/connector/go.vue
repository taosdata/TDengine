<template>
  <div>
    <h2 id="initialize-module">{{ t('connector.go.step1') }}</h2>
    <p>{{ t('connector.go.step1desc') }}</p>
    <pre v-highlight><code>go mod init tdengine.com/example</code></pre>
    <h2 id="add-dependency">{{ t('connector.go.step2') }}</h2>
    <p>{{ t('connector.go.step2desc') }}</p>
    <pre v-highlight><code class="language-go-mod">module tdengine.com/example

go 1.17

require github.com/taosdata/driver-go/v3 latest
</code></pre>
    <doc-config
      :url="endpoint"
      :need-token="false"
      url-key="TDENGINE_GO_DSN"
      :url-des="t('docsConfig.dsn')"
    ></doc-config>

    <h2 id="connect">{{ t('connector.go.step4') }}</h2>
    <p>{{ t('connector.go.step4desc') }}</p>
    <pre v-highlight><code class="language-go">package main

import (
  &quot;database/sql&quot;
  &quot;fmt&quot;
  &quot;os&quot;

  _ &quot;github.com/taosdata/driver-go/v3/taosRestful&quot;
)

func main() {
  dsn := os.Getenv(&quot;TDENGINE_GO_DSN&quot;)
 
  taos, err := sql.Open(&quot;taosRestful&quot;, dsn)
  if err != nil {
      fmt.Println(err)
      return
  }
  defer taos.Close()
  rows, err := taos.Query(&quot;show databases&quot;)
  if err != nil {
      fmt.Println(err)
      return
  }
  rows.Close()
  fmt.Println(&quot;connect success&quot;)
}
</code></pre>
    <p>{{ t('connector.go.step4desc1') }}</p>
    <pre v-highlight><code>go mod tidy
</code></pre>
    <p>{{ t('connector.go.step4desc2') }}</p>
    <pre v-highlight><code>go run main.go
</code></pre>
    <p>
      {{ t('connector.bottom2') }}
      <a :href="`${docs.urlPrefix}/programming/insert/`">{{ `${docs.urlPrefix}/programming/insert/` }}</a>
      {{ t('connector.bottomand') }}
      <a :href="`${docs.urlPrefix}/programming/query/`">{{ `${docs.urlPrefix}/programming/query/` }}</a
      >{{ t('connector.bottom3end') }}
    </p>
    <p>
      {{ t('connector.bottom3') }}
      <a :href="`${docs.urlPrefix}/programming/connect/rest-api/`">REST API</a>{{ t('connector.bottom3end') }}
    </p>
  </div>
</template>

<script lang="ts" setup>
import DocConfig from '../configTabs.vue';
import { t } from 'locales';
import { dsn } from '../utils';
import { project, docs } from 'config';

const endpoint = computed(() => getEndpoint());

function getEndpoint() {
  if (!project.isCloud) return dsn.value;
  if (dsn.value.startsWith('https')) {
    return dsn.value.replace(/\?token/, ':443?token');
  } else {
    return dsn.value;
  }
}
</script>
