<template>
  <el-row class="favorites_wrapper" :gutter="20">
    <el-col :span="12">
      <el-card shadow="always">
        <div slot="header">{{ $t("console.sharedFavorites") }}</div>
        <el-empty
          :image-size="imageSize"
          v-if="favorites&&favorites.length === 0"
        ></el-empty>
        <template v-else>
          <RecordItem
            v-for="record in favorites"
            :key="record.id"
            :record="record"
          ></RecordItem>
        </template>
      </el-card>
    </el-col>
    <!-- <el-col :span="12">
      <el-card shadow="always">
        <div slot="header">{{ $t("console.sharedFavorites") }}</div>
        <el-empty
          :image-size="imageSize"
          v-if="sharedFavorites&&sharedFavorites.length === 0"
        ></el-empty>
        <template v-else>
          <RecordItem
            v-for="record in sharedFavorites"
            :isShared="true"
            :key="record.id"
            :record="record"
          ></RecordItem>
        </template>
      </el-card>
    </el-col> -->
  </el-row>
</template>

<script>
import { mapState } from "vuex";
import RecordItem from "./components/RecordItem";
import { copy } from "@/utils";
export default {
  components: { RecordItem },
  computed: {
    ...mapState({
      favorites: state => state.console.favorites,
      selected_record: (state) => state.console.selected_record,
      sharedFavorites: state => state.console.sharedFavorites,
    }),
    // favorites() {
    //   return JSON.parse(localStorage.getItem("favorite_record"));
    // },
    // sharedFavorites() {
    //   return JSON.parse(localStorage.getItem("shared_favorites"));
    // },
  },
  data() {
    return {
      imageSize: Math.floor(window.innerHeight / 5),
    };
  },
  methods: {
    pasteSQL(sql) {
      copy(sql);
    },
  },
  mounted() {
    this.$store.commit("console/SET_FAVORITE",JSON.parse(localStorage.getItem('favorite_record')));
    this.$store.commit("console/SET_SHAREDFAVOURTIE",JSON.parse(localStorage.getItem('shared_favorites')));
  },
};
</script>

<style lang="scss" scoped>
.favorites_wrapper {
  height: 100%;
  &:deep(.el-col) {
    height: 100%;
  }
  &:deep(.el-card) {
    display: flex;
    flex-direction: column;
    height: 100%;
  }
  &:deep(.el-card__body) {
    overflow-y: auto;
    flex: 1;
  }
}

.content_wrapper {
  padding-top: 5px;
  /* display: flex; */
  /* flex-direction: row; */
  overflow-y: auto;
}

.favorite_list {
  width: 35%;
  height: 100%;
  border: 1px solid #efefef;
  margin-right: 10px;
  overflow-y: auto;
}

.favorite_detail {
  width: 65%;
  height: 100%;
  border: 1px solid #efefef;
  font-size: 16px;
  font-weight: 600;
  color: #333;
  padding: 10px;
}

.favorite_footer {
  width: 100%;
  display: flex;
  flex-direction: row;
  justify-content: flex-end;
  margin-top: 10px;
  font-size: 18px;
}
</style>
