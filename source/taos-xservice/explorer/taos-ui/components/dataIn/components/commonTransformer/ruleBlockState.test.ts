import { describe, expect, it } from 'vitest';
import { resolveActiveRuleIdAfterRemoval } from './ruleBlockState';

describe('resolveActiveRuleIdAfterRemoval', () => {
  it('keeps the current active rule when removing an inactive rule', () => {
    expect(
      resolveActiveRuleIdAfterRemoval(
        ['rule-1', 'rule-2', 'rule-3'],
        'rule-1',
        'rule-3'
      )
    ).toBe('rule-3');
  });

  it('activates the next rule when removing the active rule', () => {
    expect(
      resolveActiveRuleIdAfterRemoval(
        ['rule-1', 'rule-2', 'rule-3'],
        'rule-2',
        'rule-2'
      )
    ).toBe('rule-3');
  });

  it('falls back to the previous rule when removing the last active rule', () => {
    expect(
      resolveActiveRuleIdAfterRemoval(
        ['rule-1', 'rule-2', 'rule-3'],
        'rule-3',
        'rule-3'
      )
    ).toBe('rule-2');
  });
});
