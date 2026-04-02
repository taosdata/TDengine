<template>
  <div>
    <p v-dompurify-html="$t('docs.connectorTip')" class="primary-tip"></p>
    <DocsList v-bind="docsMap.connector" />
  </div>
</template>

<script>
import DocsList from '@/components/document/list.vue';
import { connector } from '@/components/document/index';
export default {
  components: { DocsList },
  data() {
    return {
      docsMap: {
        connector: {
          docsList: connector,
          parentUrl: '/programming',
          urlPre: '/docs/connector/'
        }
      }
    };
  },
  computed: {
    toolName() {
      return this.$route.path.slice(1).split('/')[1] || '';
    }
  },
  created() {
    console.log(connector, 'connector');
  }
};
</script>

<style lang="scss" scoped>
.icon-topic {
  width: 20px;
  height: 20px;
  margin-right: 10px;
}
</style>
