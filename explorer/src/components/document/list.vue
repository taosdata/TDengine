<template>
  <div class="connector">
    <ul class="client-list">
      <li v-for="(item, index) in localDocList" :key="index" :title="item.name">
        <router-link class="client-item" :to="getUrl(item.name, item.icon, item.path)">
          <h2 class="title">
            <Icon class="image" :name="getImg(item.name, item.icon)"></Icon>
            <span>{{ item.title || item.name }}</span>
          </h2>
          <p class="desc nowrap">
            {{ item.desc }}
          </p>
        </router-link>
      </li>
    </ul>
  </div>
</template>

<script setup lang="ts">
import { getLocalLang } from '@/utils/index';

const props = withDefaults(
  defineProps<{
    parentUrl: string;
    urlPre: string;
    docsList: AnyFunction;
  }>(),
  {}
);
const localDocList = ref<Record<string, any>>([]);
const language = computed(() => getLocalLang());
watch(
  language,
  () => {
    localDocList.value = props.docsList();
  },
  {
    immediate: true
  }
);

function getUrl(name: string, _: any, path: string) {
  return props.parentUrl + props.urlPre + encodeURIComponent(path ?? name);
}
function getImg(name: string, icon: string) {
  return icon || name;
}
</script>
<style lang="scss" scoped>
.connector {
  $item-width: 150px;
  $margin-size: 20px;

  .client-list {
    display: flex;
    flex-wrap: wrap;
    margin-bottom: 30px;

    li {
      width: calc((100% - #{$margin-size} * 3) / 3);
      margin-top: $margin-size;
      margin-right: $margin-size;
      color: rgb(96 103 112);
      border: 1px solid $item-border-color;
      border-radius: 15px;

      .client-item {
        display: block;
        padding: 30px;
      }

      $img-size: 30px;

      h2 {
        font-size: 20px;
        font-weight: bold;
        line-height: $img-size;

        span {
          margin-left: 10px;
        }
      }

      .image {
        width: $img-size;
        height: $img-size;
        vertical-align: middle;
        object-fit: contain;
      }

      .desc {
        font-size: 13px;
        line-height: 22px;
      }

      &:hover {
        border: 1px solid $color-primary;
        box-shadow: rgb(0 0 0 / 5%) 0 -9px 9px;
      }
    }
  }
}
</style>
