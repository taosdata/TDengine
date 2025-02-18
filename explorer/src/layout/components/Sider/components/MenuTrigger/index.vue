<template>
  <div class="menuTrigger" @click="triggerClick">
    <Icon
      name="right"
      class="menuTrigger_svg"
      :class="{ 'is-active': opened }"
    ></Icon>
  </div>
</template>

<script setup lang="ts">
import Icon from "@/components/Icon/index.vue";
import { useStore } from "vuex";

const store = useStore()
const opened = computed(() => store.state.sidebar.opened)

function triggerClick() {
  store.commit("sidebar/TOGGLE_SIDEBAR");
}

</script>

<style scoped>
.menuTrigger {
  width: 20px;
  height: 20px;
  cursor: pointer;
  display: flex;
  flex-direction: row;
  align-items: center;
  justify-content: center;
  border-radius: 50%;
  background: white;
  box-shadow: rgb(0 0 0 , 10%) 2px 0px 9px;
  z-index: 2;
}

.menuTrigger_svg {
  width: 16px;
  height: 16px;
}

.is-active {
  transform: rotate(180deg);
}
</style>
