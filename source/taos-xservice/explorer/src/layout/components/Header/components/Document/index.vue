<template>
  <div class="doc-block">
    <el-dropdown trigger="hover" placement="bottom">
      <div class="avatar_block">
        <span>?</span>
      </div>
      <template #dropdown>
        <el-dropdown-menu>
          <el-dropdown-item style="display: flex; align-items: center">
            <a class="drop-block" target="_blank" :href="docsUrl">
              <div style="display: flex; align-items: center">
                <Icon
                  name="document"
                  class="dropdown_icon"
                  style="width: 20px; height: 20px; margin-left: 2px"
                ></Icon>
                <span style="color: #4259ce; margin-left: 6px">{{
                  $t("document")
                }}</span>
              </div>
            </a>
          </el-dropdown-item>
        </el-dropdown-menu>
      </template>
    </el-dropdown>
  </div>
</template>

<script setup lang="ts">
import { getLocalLang } from '@/utils'
const { OEM_NAME } = inject("globalCustomProperties") as GlobalCustomProperties;

const docsUrl = computed(() => {
  if (OEM_NAME.includes("TDengine")) {
    return getLocalLang().includes("zh") ? "/docs/" : "/docs-en/";
  } else {
    return "/docs/";
  }
})

</script>

<style lang="scss" scoped>
.doc-block {
  border-color: #4259ce;
  color: #4259ce;
  margin-right: 20px;
  margin-left: 15px;
  box-sizing: border-box;
  cursor: pointer;
  position: relative;
}
.avatar_block {
  margin-top: 4px;
  display: flex;
  align-items: center;
  justify-content: center;
  width: 26px;
  height: 26px;
  border: 1px solid $color-primary;
  border-radius: 50%;
  color: $color-primary;
}
.el-dropdown-menu__item {
  display: flex;
  align-items: center;
}
</style>
