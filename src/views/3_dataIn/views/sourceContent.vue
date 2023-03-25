<template>
  <div class="source-cont">
    <component :is="currentComp"></component>
  </div>
</template>

<script>
import InfluxDB from "@/utils/config/mdx/influxdb.vue";
import OpenTSDB from "@/utils/config/mdx/opentsdb.vue";
import OPC from "@/utils/config/mdx/opc.vue";
import Kafka from "@/utils/config/mdx/kafka.vue";
export default {
  name: "SourceContent",
  components: {
    InfluxDB,
    OpenTSDB,
    OPC,
    Kafka,
  },
  props: {
    url: {
      type: String,
      default: "",
    },
  },
  computed: {
    currentComp() {
      return this.$route.params.sourceName;
    },
  },
  created() {
    console.log(this.$route.params.sourceName, "===");
  },
};
</script>

<style lang="scss" scoped>
.source-cont {
  display: flex;
  flex: 1;
}
</style>