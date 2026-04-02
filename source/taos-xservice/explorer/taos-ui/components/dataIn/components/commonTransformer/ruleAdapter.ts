import { cloneDeep } from 'lodash-es';
import type { ConditionExpr, TransformCapabilities, TransformConfig, TransformFormState, TransformRuleState } from './type';

const DEFAULT_RULE_MATCHES = { expr: 'true' } satisfies ConditionExpr;

function buildRuleId(index: number) {
  return `rule-${index + 1}`;
}

export function normalizeConditionExpr(expr?: ConditionExpr | string | null): ConditionExpr {
  if (typeof expr === 'string') {
    return { expr };
  }

  if (expr?.expr) {
    return typeof expr.null_if_error === 'boolean'
      ? { expr: expr.expr, null_if_error: expr.null_if_error }
      : { expr: expr.expr };
  }

  return cloneDeep(DEFAULT_RULE_MATCHES);
}

export function normalizeMutateList(mutate: Recordable[] = []) {
  return cloneDeep(mutate).map(item => {
    if (!item.filter) {
      return item;
    }

    return {
      ...item,
      filter: normalizeConditionExpr(item.filter)
    };
  });
}

function toRuleState(rule: Recordable, index: number): TransformRuleState {
  return {
    id: rule.id || buildRuleId(index),
    matches: normalizeConditionExpr(rule.matches),
    mutate: normalizeMutateList(rule.mutate || []),
    model: cloneDeep(rule.model || {})
  };
}

export function getTransformCapabilities(sourceType: string): TransformCapabilities {
  if (sourceType === 'kafka') {
    return {
      supportsRuleBlocks: true,
      supportsMultipleRules: true
    };
  }

  return {
    supportsRuleBlocks: false,
    supportsMultipleRules: false
  };
}

export function toRuleFormState(config: TransformConfig, sourceType: string): TransformFormState {
  const state = cloneDeep(config) as TransformFormState;
  const capabilities = getTransformCapabilities(sourceType);

  if (!capabilities.supportsRuleBlocks) {
    return state;
  }

  const parser = state.parser || {};
  if (parser.rules?.length) {
    parser.rules = parser.rules.map((rule, index) => toRuleState(rule, index));
    state.parser = parser;
    return state;
  }

  const hasLegacyRule = parser.model || parser.mutate?.length;
  parser.rules = hasLegacyRule
    ? [
        toRuleState(
          {
            matches: DEFAULT_RULE_MATCHES,
            mutate: normalizeMutateList(parser.mutate || []),
            model: parser.model || {}
          },
          0
        )
      ]
    : [];
  state.parser = parser;
  return state;
}

export function toBackendPayload(state: TransformFormState, sourceType: string): TransformConfig {
  const payload = cloneDeep(state) as TransformConfig;
  const parser = payload.parser || {};
  const capabilities = getTransformCapabilities(sourceType);

  if (capabilities.supportsRuleBlocks) {
    parser.rules = (parser.rules || []).map(rule => {
      const nextRule = { ...rule };
      delete nextRule.id;
      return nextRule;
    });
    delete parser.model;
    delete parser.mutate;
    payload.parser = parser;
    return payload;
  }

  const [firstRule] = parser.rules || [];
  if (firstRule) {
    parser.model = cloneDeep(firstRule.model);
    parser.mutate = cloneDeep(firstRule.mutate);
  }
  delete parser.rules;
  payload.parser = parser;
  return payload;
}
