<template>
  <div
    :class="['rule-block-card', { 'rule-block-card--active': active }]"
    role="button"
    tabindex="0"
    @click="$emit('select', rule.id)"
    @keydown.enter.stop.prevent="$emit('select', rule.id)"
    @keydown.space="onCardSpace"
  >
    <div class="rule-block-card__header">
      <div class="rule-block-card__title">{{ t('dataIn.transformer.ruleTitle', [index + 1]) }}</div>
      <div class="rule-block-card__actions">
        <button
          data-test="rule-move-up-button"
          type="button"
          aria-label="Move rule up"
          class="rule-block-card__button"
          :disabled="!canMoveUp"
          @click.stop="$emit('move-up', rule.id)"
        >
          <el-icon><Top /></el-icon>
        </button>
        <button
          data-test="rule-move-down-button"
          type="button"
          aria-label="Move rule down"
          class="rule-block-card__button"
          :disabled="!canMoveDown"
          @click.stop="$emit('move-down', rule.id)"
        >
          <el-icon><Bottom /></el-icon>
        </button>
        <button
          data-test="rule-remove-button"
          type="button"
          class="rule-block-card__button rule-block-card__button--danger"
          @click.stop="$emit('remove', rule.id)"
        >
          {{ t('dataIn.transformer.removeRule') }}
        </button>
      </div>
    </div>
    <div class="rule-block-card__field">
      <span class="rule-block-card__label">{{ t('dataIn.transformer.ruleMatches') }}</span>
      <div class="rule-block-card__matches-row">
        <input
          :value="rule.matches.expr"
          data-test="rule-matches-input"
          class="rule-block-card__input"
          type="text"
          @input="onMatchesInput"
        />
        <el-button
          data-test="rule-preview-matches-button"
          class="rule-block-card__preview-btn"
          @click.stop="$emit('preview-matches', rule.id)"
        >
          <Icon name="PREVIEW" style="width: 16px; height: 16px"></Icon>
        </el-button>
      </div>
    </div>
    <slot></slot>
  </div>
</template>

<script setup lang="ts">
import { Bottom, Top } from '@element-plus/icons-vue';
import type { TransformRuleState } from './type';
import { t } from 'locales';

const props = defineProps<{
  rule: TransformRuleState;
  index: number;
  active: boolean;
  canMoveUp: boolean;
  canMoveDown: boolean;
}>();

const emit = defineEmits(['update:rule', 'remove', 'move-up', 'move-down', 'select', 'preview-matches']);

function onMatchesInput(event: Event) {
  const target = event.target as HTMLInputElement;
  emit('update:rule', {
    ...props.rule,
    matches: {
      expr: target.value
    }
  });
}

function onCardSpace(event: KeyboardEvent) {
  if ((event.target as HTMLElement).tagName === 'INPUT') return;
  event.stopPropagation();
  event.preventDefault();
  emit('select', props.rule.id);
}
</script>

<style lang="scss" scoped>
.rule-block-card {
  display: flex;
  flex-direction: column;
  gap: 12px;
  padding: 16px;
  border: 1px solid #dcdfe6;
  border-radius: 8px;
  background: #fff;
  cursor: pointer;

  &--active {
    border-color: #409eff;
    box-shadow: 0 0 0 1px rgb(64 158 255 / 20%);
  }

  &:focus-visible {
    outline: 2px solid #409eff;
    outline-offset: 2px;
  }

  &__header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
  }

  &__title {
    font-weight: 600;
    color: #303133;
  }

  &__actions {
    display: flex;
    gap: 8px;
  }

  &__button {
    padding: 4px 10px;
    border: 1px solid #dcdfe6;
    border-radius: 6px;
    background: #fff;
    color: #606266;

    &:disabled {
      cursor: not-allowed;
      opacity: 0.5;
    }

    &--danger {
      color: #f56c6c;
      border-color: #fbc4c4;
    }
  }

  &__matches-row {
    display: flex;
    gap: 8px;
    align-items: center;
  }

  .rule-block-card__preview-btn.el-button {
    flex-shrink: 0;
    width: 32px;
    height: 32px;
    padding: 2px;
    border: none;
    box-shadow: none;
    border-radius: 6px;
    margin: 0;
  }

  &__field {
    display: flex;
    flex-direction: column;
    gap: 6px;
  }

  &__label {
    font-size: 12px;
    color: #909399;
  }

  &__input {
    width: 100%;
    min-height: 32px;
    padding: 6px 10px;
    border: 1px solid #dcdfe6;
    border-radius: 6px;
  }
}
</style>
