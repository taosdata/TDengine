import { defaultValueProcessorByRule } from './defaultValueProcessorByRule';
import { defaultRuleProcessorSQL } from './defaultRuleProcessorSQL';
import { isRuleOrGroupValid } from './isRuleOrGroupValid';
import { defaultPlaceholderFieldName, defaultPlaceholderOperatorName } from './defaults';


function formatQuery (ruleGroup) {
  let format = 'sql'
  let valueProcessorInternal = defaultValueProcessorByRule;
  let ruleProcessorInternal = null;
  let quoteFieldNamesWith = ['', ''];
  let validator = () => true;
  let fields = [];
  let validationMap= {};
  let fallbackExpression = '';
  let paramPrefix = ':';
  let parseNumbers = false;
  let placeholderFieldName = defaultPlaceholderFieldName;
  let placeholderOperatorName = defaultPlaceholderOperatorName;

  if (typeof validator === 'function') {
    const validationResult = validator(ruleGroup);
    if (typeof validationResult === 'boolean') {
      if (validationResult === false) {
        return format === 'parameterized'
          ? false
          : fallbackExpression;
      }
    } else {
      validationMap = validationResult;
    }
  }

  const validateRule = (rule) => {
    let validationResult;
    let fieldValidator;
    if (rule.id) {
      validationResult = validationMap[rule.id];
    }
    if (fields.length) {
      const fieldArr = fields.filter(f => f.name === rule.field);
      if (fieldArr.length) {
        const field = fieldArr[0];
        // istanbul ignore else
        if (typeof field.validator === 'function') {
          fieldValidator = field.validator;
        }
      }
    }
    return [validationResult, fieldValidator] ;
  };
  
  const processRuleGroup = (rg, outermost) => {
    if (!isRuleOrGroupValid(rg, validationMap[rg.id = ''])) {
      return outermost ? fallbackExpression : '';
    }
    
    const processedRules = rg.rules.map(rule => {
      // Independent combinators
      if (typeof rule === 'string') {
        return rule;
      }

      // Groups
      if ('rules' in rule) {
        return processRuleGroup(rule);
      }

      // Basic rule validation
      const [validationResult, fieldValidator] = validateRule(rule);
      if (
        !isRuleOrGroupValid(rule, validationResult, fieldValidator) ||
        rule.field === placeholderFieldName ||
        rule.operator === placeholderOperatorName
      ) {
        return '';
      }

      const escapeQuotes = (rule.valueSource ?? 'value') === 'value';

      // Use custom rule processor if provided...
      if (typeof ruleProcessorInternal === 'function') {
        return ruleProcessorInternal(rule, { parseNumbers, escapeQuotes, quoteFieldNamesWith });
      }
      // ...otherwise use default rule processor and pass in the value
      // processor (which may be custom)
      return defaultRuleProcessorSQL(rule, {
        parseNumbers,
        escapeQuotes,
        valueProcessor: valueProcessorInternal,
        quoteFieldNamesWith,
      });
    });

    if (processedRules.length === 0) {
      return fallbackExpression;
    }

    // return `${rg.not ? 'NOT ' : ''}(${processedRules
    //   .filter(Boolean)
    //   .join('combinator' in rg ? ` ${rg.combinator} ` : ' ')})`;
    return `(${processedRules
      .filter(Boolean)
      .filter(item => item !='()')
      .filter(item => item !="('')")
      .join('combinator' in rg ? ` ${rg.combinator} ` : ' ')})`;
  };

  return processRuleGroup(ruleGroup, true);
}

export default formatQuery