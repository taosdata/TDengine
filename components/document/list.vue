<template>
  <ul class="client-list">
    <li v-for="(item, index) in docsList" :key="index" :title="item.name">
      <router-link class="client-item" :to="getUrl(item.name)">
        <h2 class="title">
          <Icon class="image" :name="item.icon || item.name" />
          <span>{{ item.name }}</span>
        </h2>
        <p class="desc nowrap">
          {{ item.desc }}
        </p>
      </router-link>
    </li>
  </ul>
</template>

<script lang="ts" setup>
const props = withDefaults(
  defineProps<{
    urlPrefix: string;
    docsList: Recordable[];
  }>(),
  {
    urlPrefix: '',
    docsList: () => []
  }
);

function getUrl(name: string) {
  return props.urlPrefix + encodeURIComponent(name);
}
</script>
<style lang="scss" scoped>
.client-list {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  grid-auto-rows: 1fr;
  grid-gap: 20px;
  margin-bottom: 30px;

  li {
    color: rgb(96 103 112);
    border: 1px solid #e3e4e6;
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
      border: 1px solid rgb(25.8789% 34.8999% 80.7785%);
      box-shadow: 0 3px 6px 0 rgb(0 0 0 / 20%);
    }
  }
}
</style>
