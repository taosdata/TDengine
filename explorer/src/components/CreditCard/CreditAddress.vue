<template>
  <el-form ref="addrForm" :model="info" size="mini">
    <el-form-item :label="$t('billing.streetAddress')" prop="addressDetail" required>
      <el-input type="tel" :placeholder="$t('billing.addressLine')" v-model="info.addressDetail"></el-input>
    </el-form-item>
    <el-row>
      <el-col :span="11">
        <el-form-item :label="$t('billing.city')" prop="addressCity" required>
          <el-input v-model="info.addressCity" :placeholder="$t('billing.city')"></el-input>
        </el-form-item>
      </el-col>
      <el-col :offset="2" :span="11" required prop="addressState">
        <el-form-item :label="$t('billing.stateProvince')">
          <el-input v-model="info.addressState" :placeholder="$t('billing.stateProvince')"></el-input>
        </el-form-item>
      </el-col>
    </el-row>
    <el-row>
      <el-col :span="11">
        <el-form-item :label="$t('billing.postalCode')" prop="postalCode" required>
          <el-input v-model="info.postalCode" :placeholder="$t('billing.postalCode')" type="number"></el-input>
        </el-form-item>
      </el-col>
      <el-col :offset="2" :span="11">
        <el-form-item :label="$t('country')" prop="addressCountry" required>
          <el-select style="width: 100%" filterable :placeholder="$t('choose')" v-model="info.addressCountry">
            <el-option v-for="item in country" :key="item.value" :label="item.label" :value="item.value">
              <span style="float: left; line-height: 34px">{{ item.label }}</span
              ><span style="float: right; line-height: 34px">{{ item.value }}</span>
            </el-option>
          </el-select>
        </el-form-item>
      </el-col>
    </el-row>
    <el-row style="text-align: left">
      <el-button @click="$emit('back')" icon="el-icon-back" type="text" size="small">{{ $t("backToPrev") }}</el-button>
    </el-row>
    <el-row>
      <el-col :span="11">
        <el-form-item>
          <el-button @click="$emit('cancel')" :disabled="requestIng" size="small" class="w100">{{ $t("cancel") }}</el-button>
        </el-form-item>
      </el-col>
      <el-col :offset="2" :span="11">
        <el-form-item>
          <el-button @click="save" :loading="requestIng" :disabled="requestIng" size="small" type="primary" class="w100">{{ $t("save") }}</el-button>
        </el-form-item>
      </el-col>
    </el-row>
  </el-form>
</template>

<script>
  export default {
    props: {
      requestIng: {
        type: Boolean,
        default: false,
      },
    },
    data() {
      return {
        info: {
          addressDetail: "",
          addressCity: "",
          addressCountry: "",
          addressState: "",
          postalCode: "",
        },
      };
    },
    computed: {
      country() {
        return this.$store.state.profile.countrys;
      },
    },
    watch: {
      "$store.state.billing.creditCardInfo": {
        handler(val) {
          this.info = {
            addressDetail: val.addressDetail || "",
            addressCity: val.addressCity || "",
            addressCountry: val.addressCountry || "US",
            addressState: val.addressState || "",
            postalCode: val.postalCode || "",
          };
        },
        deep: true,
        immediate: true,
      },
    },
    methods: {
      save() {
        this.$refs.addrForm.validate(valid => {
          if (valid) {
            this.$emit("save", this.info);
          }
        });
      },
    },
  };
</script>

<style></style>
