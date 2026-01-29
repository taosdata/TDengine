<template>
  <div>
    <p>{{ $t("docs.connector.rest.desc") }}</p>
    <h2 id="config">{{ $t("docs.connector.rest.step1") }}</h2>
    <p>
      {{
        $t("docs.docConfig.content", [
          " Token " + $t("docs.connector.bottomand") + " URL ",
        ])
      }}
      <span class="docker-tip">{{ $t("dockerTip")}}</span>
    </p>
    <p>
           <el-icon color="gold" :size="20">
        <Opportunity/>
      </el-icon>
      <span class="docker-tip">{{ $t("dockerTip", [`${url.split('//')[1]}`] )}}</span>
    </p>
    <el-tabs model-value="bash">
      <el-tab-pane name="bash" label="Bash">
        <pre
          v-highlight="
            `export TDENGINE_TOKEN=&quot;${token}&quot;
export TDENGINE_URL=&quot;${url}&quot;
`
          "
        ><code class="language-bash"></code></pre>
      </el-tab-pane>
      <el-tab-pane name="cmd" label="CMD">
        <pre
          v-highlight="
            `set TDENGINE_TOKEN=&quot;${token}&quot;
set TDENGINE_URL=&quot;${url}&quot;
`
          "
        ><code class="language-bash"></code></pre>
      </el-tab-pane>
      <el-tab-pane name="powershell" label="Powershell">
        <pre
          v-highlight="
            `$env:TDENGINE_TOKEN=&quot;${token}&quot;
$env:TDENGINE_URL=&quot;${url}&quot;
`
          "
        ><code class="language-powershell"></code></pre>
      </el-tab-pane>
    </el-tabs>

    <h2 id="insert">{{ $t("docs.connector.rest.step2") }}</h2>
    <p>{{ $t("docs.connector.rest.step2desc") }}</p>
    <pre
      v-highlight="
        `curl -L \
  -d &quot;INSERT INTO d1001 VALUES (1538548685000, 10.3, 219, 0.31)&quot; \
  $TDENGINE_URL/rest/sql/test?token=$TDENGINE_TOKEN
`
      "
    ><code class="language-bash"></code></pre>

    <h2 id="query">{{ $t("docs.connector.rest.step3") }}</h2>
    <p>{{ $t("docs.connector.rest.step3desc") }}</p>
    <pre
      v-highlight="
        `curl -L \
  -d &quot;select name, ntables, status from information_schema.ins_databases;&quot; \
  $TDENGINE_URL/rest/sql/test?token=$TDENGINE_TOKEN
`
      "
    ><code class="language-bash"></code></pre>
    <p>
      {{ $t("docs.connector.bottom3") }}
      <a
        :href="`${$t('urlPart')}/${restApi}`"
        >REST API</a
      >{{ $t("docs.connector.bottom3end") }}
    </p>
  </div>
</template>

<script setup lang="ts">
import { isEn } from '@/const';
import { DocsProps } from '../utils'

defineProps<DocsProps>()
const restApi = computed(() => isEn.value ? 'tdengine-reference/client-libraries/rest-api/' : 'reference/connector/rest-api/');
</script>
