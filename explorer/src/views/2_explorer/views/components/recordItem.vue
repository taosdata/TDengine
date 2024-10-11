<template>
  <div class="recordItem">
    <div class="firstRow">
      <span class="dbAndArrow">{{ this.record.database }}></span>
      <span style="color: #1652f0; cursor: pointer" v-html="parsedSQL" @click="addSql"></span>
    </div>
    <div class="secondRow">
      <template v-if="record.type">
        [ {{ parsedTime }} ]
        <span v-if="record.rows">{{ record.rows }} rows retrieved</span>
        <span v-else>{{ record.message }}</span>
      </template>
      <template v-else>
        [ {{ parsedTime }} ]
        <span style="color: #b22222">{{ $t("error") }}: {{ record.message }}</span>
      </template>
      <span class="total"> {{ $t("total") }}: {{ record.time }}ms </span>
    </div>
  </div>
</template>

<script>
  import { parseTime } from "@/utils";

  export default {
    props: ["record"],
    computed: {
      parsedTime() {
        return parseTime(this.record.createdAt, "YYYY-MM-DD kk:mm:ss");
      },
      parsedSQL() {
        return this.record.sql.replace(/\n/g, "<br/>").replace(/\s/g, "&ensp;");
      },
    },
    data() {
      return {};
    },
    methods: {
      addSql() {
        const newline = this.$store.state.console.sqlStr ? "\n" : "";
        this.$store.commit("console/ADD_SQLSTR", newline + this.record.sql);
      },
    }
  };
</script>

<style scoped>
  .recordItem {
    font-size: 15px;
    margin-bottom: 13px;
  }

  .firstRow {
    display: flex;
    flex-direction: row;
    align-items: center;
  }

  .secondRow {
    margin-top: 6px;
    color: #666;
  }

  .dbAndArrow {
    color: #33b169;
    margin-right: 8px;
  }
</style>
