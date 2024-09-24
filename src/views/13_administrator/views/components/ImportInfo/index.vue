<template>
  <div>
    <el-form
      :model="ruleForm"
      :rules="rules"
      ref="ruleForm"
      size="mini"
      label-width="auto"
      class="demo-ruleForm"
    >
      <el-form-item :label="$t('taosuser.server')" prop="server">
        <el-input v-model.trim="ruleForm.server" autocomplete="off" placeholder="http://localhost:6041 / http://127.0.0.1:6041 "></el-input>
      </el-form-item>
      <el-form-item :label="$t('taosuser.password')" prop="pwd">
        <el-input
          autocomplete="new-password" 
          clear
          v-model.trim="ruleForm.pwd"
          :show-password="true"
        ></el-input>
      </el-form-item>
      <el-form-item
        :label="$t('taosuser.items')"
        class="database-item"
        prop="selectedItems"
      >
        <el-checkbox-group
          v-model="ruleForm.selectedItems"
          class="db-pri"
        >
          <el-checkbox label="passwords">{{ $t('taosuser.userItem') }}</el-checkbox>
          <el-checkbox label="privileges">{{ $t('taosuser.privilegesItem') }}</el-checkbox>
          <el-checkbox label="whitelist">{{ $t('taosuser.whitelistItem') }}</el-checkbox>
        </el-checkbox-group>
      </el-form-item>
        <el-alert 
          type="info"
          class='reason'
          @close="showAlert=false"
          v-if="showAlert">
          <p>
            <span v-if="importReason?.success?.passwords">{{importReason?.success?.passwords}} {{$t('taosuser.succ1')}} </span>
            <span v-if="importReason?.success?.passwords && importReason?.success?.privileges">、</span>
            <span v-if="importReason?.success?.privileges">{{importReason?.success?.privileges}} {{$t('taosuser.succ2')}}</span>
            <span v-if="importReason?.success?.passwords || importReason?.success?.privileges">{{ $i18n.locale.includes('en') ? ',': '，'}}</span>
            <span v-if="importReason?.fails?.passwords?.length > 0 || importReason?.fails?.privileges?.length > 0" class="fail">{{$t('taosuser.fail1')}}</span>
          </p>
          <ul>
            <li v-for="(item,index) in importReason?.fails?.passwords" :key="'pwd'+index">{{ $t('taosuser.user') }} {{ item.user}} {{$t('taosuser.fail2')}} {{ item.reason }}</li>
            <li v-for="(item,index) in importReason?.fails?.privileges" :key="'pri'+index">{{ $t('taosuser.user') }} {{ item.user }} {{ $t('taosuser.privilegesItem') }}(`{{ item.privilege }}`){{$t('taosuser.fail2')}} {{ item.reason }}</li>
          </ul>
          <br/>
          <span v-if="importReason?.fails?.passwords?.length > 0 || importReason?.fails?.privileges?.length > 0">{{$t('taosuser.fail3')}}</span>
        </el-alert>
     
    </el-form>

    <el-row style="margin-top: 20px">
      <el-col :span="5" :offset="6">
        <el-button size="small" @click="cancel" class="w100">{{
          $t("cancel")
        }}</el-button>
      </el-col>
      <el-col :span="5" :push="4">
        <el-button
          size="small"
          :disabled="confirmStatus"
          @click="createUser"
          class="w100"
          type="primary"
          :loading="loading"
          >{{ $t("confirm") }}</el-button
        >
      </el-col>
    </el-row>
  </div>
</template>

<script>
import { sendSQLReq } from "@/api/gateway/console";
import { importTaosInfo } from "@/api/explorer/login";
import { Message } from "element-ui";
export default {
  props: {
    close: {
      type: Function,
      default: () => {},
    },
    status: {
      type: Boolean,
      default: false,
    },
  },
  async created() {
  },
  watch: {
    "ruleForm.selectedItems": {
      deep: true,
      handler(items) {
        if (items) {
          // 当选中白名单时默认勾选 Passwords
          if (items.includes('whitelist') && !items.includes('passwords')) {
            this.ruleForm.selectedItems.push('passwords')
          }
        }
      },
    },
  },
  data() {
    return {
      ruleForm: {
        server: "",
        pwd: '',
        selectedItems: ['passwords','privileges'],
        passwords: false,
        privileges: false,
        whitelist: false,
      },
      rules: {
        server: [
          {
            required: true,
            message: this.$t("taosuser.server") + this.$t("requiredMessage"),
          },
          {
            pattern:/^https?:\/\/((\d{1,3}\.){3}\d{1,3}|[a-zA-Z0-9.-]+)(:\d{1,5})$/,
            message: this.$t('taosuser.formatError'),
          }
        ],
        pwd: [
          {
            required: true,
            message: this.$t("taosuser.password") + this.$t("requiredMessage"),
          }
        ],
        selectedItems: [
        {
            required: true,
            message: this.$t("taosuser.items") + this.$t("requiredMessage"),
          }
        ]
      },
      confirmStatus: false,
      // selectedItems: ['passwords','privileges'],
      loading: false,
      showAlert: false,
      importReason: {
        // "success": {
        //   "passwords": 1,
        //   "privileges": 1,
        // },
        // "fails": {
        //   "passwords": [
        //     { 
        //       "user": 'root',
        //       "reason": 'succ' 
        //     }
        //   ],
        //   "privileges": [
        //     { 
        //       "user": 'String',
        //       "privilege": 'String',
        //       "reason": 'Contact the TDengine customer success team to get the activation code' 
        //     }
        //   ]
        // }
      }
    };
  },
  methods: {
    getServer() {
      // http://user:pwd@host:6041
      let url = ''
      const { server, pwd } = this.ruleForm
      try {
        let parsed_url = new URL(server);
        const { protocol, host } = parsed_url;
        url = protocol + "//" + 'root:' + pwd + '@' + host    
      } catch (error) {
        console.log('error');
      }
      return url || server
    },
    getSelectItem(item) {
      return this.ruleForm.selectedItems.includes(item)
    },
    createUser() {
      this.$refs["ruleForm"].validate(async(valid) => {
        if (valid) {
          this.loading = true;
          let params = {
            server: this.getServer(),
            passwords: this.getSelectItem('passwords'),
            privileges: this.getSelectItem('privileges'),
            whitelist: this.getSelectItem('whitelist'),
          }
          try {
            let res = await importTaosInfo(params)
            if (res && Object.hasOwnProperty.call(res,'code')) {
              this.loading = false;
              this.$error(res?.message);
              return
            }
            this.importReason = res
            this.showAlert = true
            this.loading = false;
            this.$emit("refresh")
            Message.success(this.$t("operateSucc"));
          } catch (error) {
            this.loading = false;
            console.log(error);
          }
        } else {
          return false;
        }
      });
    },
    cancel() {
      this.$emit("close");
    }
  },
};
</script>

<style lang="scss" scoped>
.db-label {
  display: inline-block;
  margin-right: 30px;
  width: 240px;
  text-align: left;
}

.db-pri {
  display: flex;
  text-align: left;
  flex-wrap: wrap;
  flex-direction: column;
}
.database-item {
  ::v-deep .el-form-item__content {
    padding-top: 5px;
  }
}
.reason {
  text-align: left;
  font-weight: 500;
  ::v-deep .el-alert__description {
    font-size: 14px;
  }
  ul{
    color: red;
    max-height: 400px;
    overflow-y: scroll;
  }
  
}
</style>