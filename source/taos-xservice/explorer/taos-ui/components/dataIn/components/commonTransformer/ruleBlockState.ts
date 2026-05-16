export function resolveActiveRuleIdAfterRemoval(
  ruleIds: string[],
  removedRuleId: string,
  activeRuleId: string
) {
  const removedIndex = ruleIds.indexOf(removedRuleId);
  if (removedIndex < 0) {
    return activeRuleId || ruleIds[0] || null;
  }

  const remainingRuleIds = ruleIds.filter(ruleId => ruleId !== removedRuleId);
  if (!remainingRuleIds.length) {
    return null;
  }

  if (removedRuleId !== activeRuleId && remainingRuleIds.includes(activeRuleId)) {
    return activeRuleId;
  }

  return remainingRuleIds[Math.min(removedIndex, remainingRuleIds.length - 1)];
}
