<template>
  <div class="github_wrapper">
    <a href="https://github.com/taosdata/TDengine" target="_blank" class="githubLink">
      <Icon name="github" class="github_svg"></Icon>
    </a>
  </div>
</template>

<script>
import Icon from "@/components/Icon";

export default {
  name: 'github',
  components: { Icon },
  data() {
    return {};
  }
};
</script>

<style scoped>
.github_wrapper{
  cursor: pointer;
  display: flex;
  flex-direction: row;
  align-items: center;
}

.github_svg{
  width: 25px;
  height: 25px;
}

.githubLink{
  margin-top: 4px;
}


</style>
