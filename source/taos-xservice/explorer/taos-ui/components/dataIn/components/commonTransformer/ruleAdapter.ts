import { cloneDeep } from 'lodash-es';
import type {
  ConditionExpr,
  TransformCapabilities,
  TransformConfig,
  TransformFormState,
  TransformRuleState
} from './type';

const DEFAULT_RULE_MATCHES = { expr: 'true' } satisfies ConditionExpr;
const PARSER_RUNTIME_KEYS = new Set(['early_break']);
const PARSER_OPTION_KEYS = new Set(['depth', 'keep', ...PARSER_RUNTIME_KEYS]);

type SerializedConditionExprWrapper = {
  expr: ConditionExprInput;
  null_if_error?: boolean;
};
type SerializedFilterEnumWrapper = {
  Expr?: ConditionExprInput;
};
type ConditionExprInput =
  | ConditionExpr
  | SerializedConditionExprWrapper
  | SerializedFilterEnumWrapper
  | string
  | null
  | undefined
  | ConditionExprInput[];

function buildRuleId(index: number) {
  return `rule-${index + 1}`;
}

function stringifyParserExpression(value: unknown): string {
  if (value == null) {
    return '';
  }

  if (typeof value === 'string') {
    return value;
  }

  if (Array.isArray(value)) {
    return value.toString();
  }

  if (typeof value === 'number' || typeof value === 'boolean') {
    return String(value);
  }

  return JSON.stringify(value);
}

export function stripParserRuntimeOptions(config?: Recordable) {
  if (!config) {
    return;
  }

  PARSER_RUNTIME_KEYS.forEach(key => {
    delete config[key];
  });
}

export function getParserDisplayConfig(config?: Recordable) {
  if (!config) {
    return {
      type: '',
      expression: ''
    };
  }

  if (config.plugin_type) {
    return {
      type: String(config.plugin_type),
      expression: stringifyParserExpression(config.plugin_params)
    };
  }

  const type = Object.keys(config).filter(key => !PARSER_OPTION_KEYS.has(key))[0] || '';
  return {
    type,
    expression: stringifyParserExpression(config[type])
  };
}

export function normalizeConditionExpr(expr?: ConditionExprInput): ConditionExpr {
  if (Array.isArray(expr)) {
    return normalizeConditionExpr(expr[0]);
  }

  if (typeof expr === 'string') {
    return { expr };
  }

  if (expr?.Expr) {
    return normalizeConditionExpr(expr.Expr);
  }

  if (typeof expr?.expr === 'object') {
    const nested = normalizeConditionExpr(expr.expr);
    return typeof expr.null_if_error === 'boolean' && typeof nested.null_if_error !== 'boolean'
      ? { ...nested, null_if_error: expr.null_if_error }
      : nested;
  }

  if (typeof expr?.expr === 'string' && expr.expr) {
    return typeof expr.null_if_error === 'boolean'
      ? { expr: expr.expr, null_if_error: expr.null_if_error }
      : { expr: expr.expr };
  }

  return cloneDeep(DEFAULT_RULE_MATCHES);
}

export function getConditionExprText(expr?: ConditionExprInput): string {
  if (Array.isArray(expr)) {
    return getConditionExprText(expr[0]);
  }

  if (typeof expr === 'string') {
    return expr;
  }

  if (expr?.Expr) {
    return getConditionExprText(expr.Expr);
  }

  if (typeof expr?.expr === 'object') {
    return getConditionExprText(expr.expr);
  }

  return typeof expr?.expr === 'string' ? expr.expr : '';
}

export function updateConditionExprText(expr: ConditionExprInput, text: string): ConditionExpr {
  if (Array.isArray(expr)) {
    return updateConditionExprText(expr[0], text);
  }

  if (expr?.Expr) {
    return updateConditionExprText(expr.Expr, text);
  }

  if (typeof expr?.expr === 'object') {
    const nested = updateConditionExprText(expr.expr, text);
    return typeof expr.null_if_error === 'boolean' && typeof nested.null_if_error !== 'boolean'
      ? { ...nested, null_if_error: expr.null_if_error }
      : nested;
  }

  if (typeof expr === 'object' && expr !== null) {
    return {
      ...expr,
      expr: text
    };
  }

  return { expr: text };
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
