<template>
  <div class="agent-docs markdown-body">
    <h2>{{ $t('dataIn.agentUsageDocs') }}</h2>
    <ol>
      <li v-dompurify-html="$t('docs.taosxAgent.1', downloadUrl)"></li>
      <li>
        <span v-dompurify-html="$t('docs.taosxAgent.2')"></span>
        <pre v-highlight><code>endpoint="{{ taoxAddress }}"
token="&lt;token&gt;"</code></pre>
      </li>
      <li
        >{{ $t('docs.taosxAgent.3') }}
        <pre v-highlight><code>taosx-agent -c agent.toml</code></pre>
      </li>
      <li
        >{{ $t('docs.taosxAgent.4') }}
        <pre v-highlight><code>taosx-agent -c agent.toml</code></pre>
      </li>
    </ol>
  </div>
</template>

<script>
import 'github-markdown-css/github-markdown-light.css';
import { OfficialSite } from '@/const';
export default {
  props: {},
  components: {},
  data() {
    return {};
  },
  computed: {
    downloadUrl() {
      const assetsUrl = OfficialSite + `/assets-download/3.0/taosx-agent-${localStorage.getItem('agent_version')}-`;
      return {
        linuxDL: assetsUrl + 'linux-x64.tar.gz',
        windowDL: assetsUrl + 'windows-x64-installer.exe'
      };
    },
    taoxAddress() {
      return this.$store.state.dataIn.taoxAddress?.[0]?.address ?? '';
    },
    
  },
  watch: {},
  created() {},
  mounted() {},
  methods: {}
};
</script>

<style scoped lang="scss">
.agent-docs {
  padding: 10px 0;
  &:deep(ol, ul) {
    padding-left: 0;
  }
}
</style>
