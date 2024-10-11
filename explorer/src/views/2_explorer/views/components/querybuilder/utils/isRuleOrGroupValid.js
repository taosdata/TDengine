
import { isPojo } from './misc';

export const isValidationResult = (vr) =>
  isPojo(vr) && typeof vr.valid === 'boolean';

export const isRuleOrGroupValid = (
  rg,
  validationResult,
  validator
) => {
  if (typeof validationResult === 'boolean') {
    return validationResult;
  }
  if (isValidationResult(validationResult)) {
    return validationResult.valid;
  }
  if (typeof validator === 'function' && !('rules' in rg)) {
    const vr = validator(rg);
    if (typeof vr === 'boolean') {
      return vr;
    }
    // istanbul ignore else
    if (isValidationResult(vr)) {
      return vr.valid;
    }
  }
  return true;
};
