<template>
  <div class="">
    <ParserTable
      v-model="primaryKey"
      :data="tableData"
    />
    <el-form-item
      :label="$t('datasource.name')"
      prop='data.parser.model.name'
      :rules="[{ required: true, message: $t('required', [$t('datasource.name')]) }]" 
    >
      <template slot="label">
        <span
          >{{ $t('datasource.name') }}
          <el-tooltip
            class="item"
            effect="light"
            :content="$t('datasource.createsubtbtip')"
            placement="top-start"
          >
            <el-icon
              style="margin-left: 5px; vertical-align: middle"
              class="el-icon-info info-icon"
            ></el-icon> </el-tooltip
        ></span>
      </template>
      <el-input v-model="data.model.name"></el-input>
    </el-form-item>
    <el-form-item
      :label="$t('data.stableName')"
      prop='data.parser.model.using'
      :rules="[{ required: true, message: $t('required', [$t('data.stableName')]) }]" 
    >
      <el-input v-model="data.model.using"></el-input>
    </el-form-item>
  </div>
</template>

<script>
import ParserTable from './parserTable.vue';
import { sendSQLReq } from '@/api/gateway/console';
import { getGroupsObj } from '../utils'

const dbPrecisionMap = {};
export default {
  props: {
    data: {
      type: Object,
      default: () => ({})
    },
    fields: {
      type: Array,
      default: () => []
    },
    parent: {
      type: String,
      default: ''
    }
  },
  components: { ParserTable },
  inject: ['sourceParent'],
  data() {
    return {
      primaryKey: 'DateTime',
      tableData: [],
      dbPrecision: '',
      displayfiledNameArr: []
    };
  },
  computed: {
    subtableNamingRules: {
      get() {
        return this.data.model.name;
      },
      set(val) {
        return (this.data.model.name = val);
      }
    },
    stableName: {
      get() {
        return this.data.model.using;
      },
      set(val) {
        return (this.data.model.using = val);
      }
    },
    isKafkaOrMongoDB() {
      return this.sourceParent.sourceForm.type == 'kafka' || this.sourceParent.sourceForm.type == 'mongodb';
    },
    jsonParentField() {
      return this.isKafkaOrMongoDB ? 'value' : 'payload';
    },
    jsonArray() {
      return this.data.parse?.[this.jsonParentField]?.json ?? [];
    },
    filedNameArr() {
      return this.fields.map(item => {
        return {
          name: item.name,
          cast: item.type
        }
      });
    },
    targetDB() {
      return this.$store.state.app.currentDBName;
    },
    collectTable() {
      return getGroupsObj(this.sourceParent.sourceForm.data)?.table;
    },
    isEdit() {
      return this.sourceParent.isEditable;
    }
  },
  watch: {
    tableData: {
      handler(val) {
        this.generateJsonArray(val);
        this.setColumnAndTag(val);
      },
      deep: true
    },
    targetDB: {
      handler(val) {
        // if (!this.isKafka) return;
        if (dbPrecisionMap[val]) {
          this.dbPrecision = dbPrecisionMap[val];
        } else {
          val && this.getDbPercesion();
        }
      },
      immediate: true
    },
    dbPrecision: {
      handler(val) {
        // if (!this.isKafka) return;
        // this.data.parse.ts = { as: `timestamp(${val})` };
        this.data.parse.DateTime = { as: `timestamp(${val})` };
        if (this.data.parse.StartDateTime) {
          this.data.parse.StartDateTime = { as: `timestamp(${val})` };
        }
      },
      immediate: true
    },
    collectTable: {
      handler(val) {
        let arr = val === 'Runtime.dbo.Live' ? ['wwResolution', 'StartDateTime'] : ['OPCQuality']
        this.displayfiledNameArr = this.filedNameArr.filter(item => !arr.includes(item.name))
        this.generateTableData();
      },
      immediate: true
    }
  },
  created() {
    // this.generateTableData();
  },
  mounted() {},
  methods: {
    generateTableData() {
      const { columns = [], tags = [] } = this.data.model;
      
      const result = this.displayfiledNameArr.map(item => {
        const config = {
          usageType: 0,
          field: item.name,
          type: 'system',
          cast: item.cast
        };
        if (this.isEdit) {
          if (columns.includes(item.name)) {
            config.usageType = 1;
          } 
          if (tags.includes(item.name)) {
            config.usageType = 2;
          } 
        } else {
          config.usageType =  item.name == 'TagName' ? 2 : 1;
        }
        return config;
      });
      this.tableData = result
      // this.tableData = result.concat(
      //   this.getCustomeField(
      //     columns.filter(item => !this.filedNameArr.includes(item.name)),
      //     1
      //   ),
      //   this.getCustomeField(
      //     tags.filter(item => !this.filedNameArr.includes(item.name)),
      //     2
      //   )
      // );
    },
    getCustomeField(data, usageType = 1) {
      if (!data.length || !this.jsonArray.length) return [];
      return data.map(item => {
        const config = {
          usageType,
          field: item,
          type: 'custome'
        };
        const jsonConfig = this.jsonArray.find(jsonItem => jsonItem.name == item);
        config.targetField = jsonConfig?.alias ?? item;
        config.dataType = jsonConfig?.cast ?? 'INT';
        return config;
      });
    },
    generateJsonArray(data) {
      // if (!data.length) return (this.data.parse[this.jsonParentField].json = []);
      // this.data.parse[this.jsonParentField].json = data
      //   .filter(item => item.type == 'custome' && item.field && item.targetField)
      //   .map(item => {
      //     const config = {
      //       name: item.field,
      //       alias: item.targetField,
      //       cast: item.dataType
      //     };
      //     return config;
      //   });
      if (!data.length) return this.data.parse = {}
      let config = {}
       data
        .filter(item => item.type == 'system' && item.field)
        .map(item => {
          if (item.cast == 'timestamp') {
            config[item.field] = { as: `${item.cast}(${this.dbPrecision})`}
          } else {
            config[item.field] = { as: item.cast}
          }
        });
        this.data.parse = config
    },
    setColumnAndTag(data) {
      const columns = data.filter(item => item.usageType == 1).map(item => item.field);
      const tags = data.filter(item => item.usageType == 2).map(item => item.field);
      this.data.model.columns = columns;
      this.data.model.tags = tags;
    },
    getDbPercesion() {
      sendSQLReq(`select \`precision\` from information_schema.ins_databases where name = '${this.targetDB}';`, true).then(data => {
        this.dbPrecision = data?.[0]?.precision ?? '';
        dbPrecisionMap[this.targetDB] = this.dbPrecision;
      });
    }
  }
};
</script>

<style scoped lang="scss"></style>
