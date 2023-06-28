<template>
  <div>
    <h2 id="init">{{ $t('docs.topic.createProject') }}</h2>
    <p>{{ $t('docs.topic.step1desc', ['Java']) }}</p>
    <pre v-highlight="`mvn archetype:generate -DgroupId=com.taos -DartifactId=consumer -Dversion=1.0.0 -DarchetypeArtifactId=maven-archetype-quickstart`"><code class="language-bash"></code></pre>
    <p>{{ $t('docs.topic.step1desc1', ['consumer/pom.xml']) }}</p>
    <pre
      v-highlight="
        `<properties>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <maven.compiler.source>1.8</maven.compiler.source>
    <maven.compiler.target>1.8</maven.compiler.target>
 </properties>
  
<dependency>
    <groupId>com.taosdata.jdbc</groupId>
    <artifactId>taos-jdbcdriver</artifactId>
    <version>3.2.1</version>
</dependency>`
      "
    ><code class="language-xml">
    
 </code></pre>
    <doc-config
      :id="'config'"
      :url="tmq"
      :need-token="false"
      :url-key="'TDENGINE_JDBC_URL'"
      :url-des="$t('component.docConfig.tmq')"
    ></doc-config>
    <!-- <pre v-highlight><code class="language-bash">export TDENGINE_JDBC_URL="jdbc:TAOS-RS://gw.us-east-1.aws.cloud.tdengine.com?useSSL=true&token=6363827614de80e382473d2b2febd642b0bae37e"</code></pre> -->
    <h2 id="create-consumer">{{ $t('docs.topic.step3') }}</h2>
    <p>{{ $t('docs.topic.step3desc') }}</p>
    <pre
      v-highlight="
        `String url = System.getenv(&quot;TDENGINE_JDBC_URL&quot;);

Properties properties = new Properties();
properties.setProperty(TMQConstants.CONNECT_TYPE, &quot;ws&quot;);
properties.setProperty(TMQConstants.CONNECT_URL, url);
properties.setProperty(TMQConstants.CONNECT_TIMEOUT, &quot;10000&quot;);
properties.setProperty(TMQConstants.CONNECT_MESSAGE_TIMEOUT, &quot;10000&quot;);
properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, &quot;true&quot;);
properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, &quot;true&quot;);
properties.setProperty(TMQConstants.GROUP_ID, &quot;gId&quot;);
properties.setProperty(TMQConstants.VALUE_DESERIALIZER, &quot;com.taosdata.jdbc.tmq.MapDeserializer&quot;);

TaosConsumer<Map<String, Object>> consumer = new TaosConsumer<>(properties));`
      "
    ><code class="language-java"></code></pre>
    <h2 id="subscribe-topic">{{ $t('docs.topic.step4') }}</h2>
    <p>{{ $t('docs.topic.step4desc', [topicName]) }}</p>
    <pre
      v-highlight="
        `consumer.subscribe(Collections.singletonList(&quot;${topicName}&quot;));\n            for (int i = 0; i < 10; i++) {\n                ConsumerRecords<Map<String, Object>> consumerRecords = consumer.poll(Duration.ofMillis(100));\n                for (ConsumerRecord<Map<String, Object>> r : consumerRecords) {\n                    Map<String, Object> bean = r.value();\n                    bean.forEach((k, v) -> {\n                        System.out.print(k + &quot; : &quot; + v + &quot; &quot;);\n                    });\n                    System.out.println();\n                }\n            }`
      "
    ><code></code></pre>
    <h2 class="close-consumer">{{ $t('docs.topic.step5') }}</h2>
    <p>{{ $t('docs.topic.step5desc', [topicName]) }}</p>
    <pre v-highlight><code class="language-java">consumer.unsubscribe();
consumer.close();</code></pre>
    <h2 class="full-example">{{ $t('docs.topic.step6') }}</h2>
    <p>{{ $t('docs.topic.step6desc', [topicName]) }}</p>
    <pre
      v-highlight="
        `package com.taos;\n\nimport com.taosdata.jdbc.tmq.ConsumerRecord;\nimport com.taosdata.jdbc.tmq.ConsumerRecords;\nimport com.taosdata.jdbc.tmq.TMQConstants;\nimport com.taosdata.jdbc.tmq.TaosConsumer;\n\nimport java.sql.SQLException;\nimport java.time.Duration;\nimport java.util.Collections;\nimport java.util.Map;\nimport java.util.Properties;\n\npublic class Consumer {\n    public static void main(String[] args) throws SQLException {\n\n        String url = System.getenv(&quot;TDENGINE_JDBC_URL&quot;);\n\n        Properties properties = new Properties();\n        properties.setProperty(TMQConstants.CONNECT_TYPE, &quot;ws&quot;);\n        properties.setProperty(TMQConstants.CONNECT_URL, url);\n        properties.setProperty(TMQConstants.CONNECT_TIMEOUT, &quot;10000&quot;);\n        properties.setProperty(TMQConstants.CONNECT_MESSAGE_TIMEOUT, &quot;10000&quot;);\n        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, &quot;true&quot;);\n        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, &quot;true&quot;);\n        properties.setProperty(TMQConstants.GROUP_ID, &quot;gId&quot;);\n        properties.setProperty(TMQConstants.VALUE_DESERIALIZER, &quot;com.taosdata.jdbc.tmq.MapDeserializer&quot;);\n\n        try (TaosConsumer<Map<String, Object>> consumer = new TaosConsumer<>(properties)) {\n            consumer.subscribe(Collections.singletonList(&quot;${topicName}&quot;));\n            for (int i = 0; i < 10; i++) {\n                ConsumerRecords<Map<String, Object>> consumerRecords = consumer.poll(Duration.ofMillis(100));\n                for (ConsumerRecord<Map<String, Object>> r : consumerRecords) {\n                    Map<String, Object> bean = r.value();\n                    bean.forEach((k, v) -> {\n                        System.out.print(k + &quot; : &quot; + v + &quot; &quot;);\n                    });\n                    System.out.println();\n                }\n            }\n        }\n    }\n}`
      "
    ><code class="language-java"></code></pre>
  </div>
</template>

<script>
import DocConfig from '@/components/DocConfig/index.vue';
export default {
  components: { DocConfig },
  props: {
    token: {
      type: String,
      default: ''
    },
    url: {
      type: String,
      default: ''
    },
    topic: {
      type: String,
      default: ''
    }
  },
  computed: {
    tmq() {
      const wsPrefix = this.url.startsWith('https') ? 'wss' : 'ws';
      const uri = this.url.replace(/https?:\/\//, '');
      const tokenStr = this.token;
      return `${wsPrefix}://${uri}/rest/tmq?token=${tokenStr}`;
    },
    topicName() {
      return this.topic ? this.topic : this.$t('docs.topic.defaultTopic');
    }
  }
};
</script>
