import { describe, expect, it } from 'vitest';
import { checkParseData } from './util';
import { getTransformCapabilities, toBackendPayload, toRuleFormState } from './ruleAdapter';

describe('ruleAdapter', () => {
  it('wraps legacy parser config into one default Kafka rule block', () => {
    const legacyConfig = {
      parser: {
        global: { timezone: 'UTC' },
        parse: { value: { json: ['$.temp'] } },
        model: {
          name: '${device}',
          using: 'meters',
          tags: ['site'],
          columns: ['temperature']
        },
        mutate: [
          {
            extract: {
              payload: {
                json: ['$.payload.temperature']
              }
            }
          },
          {
            filter: 'temperature > 10'
          }
        ]
      },
      input: [{ value: '{"payload":{"temperature":42}}' }],
      format: {
        pageCount: 1,
        pageSize: 20,
        currentPage: 1
      }
    };

    const state = toRuleFormState(legacyConfig, 'kafka');

    expect(state.parser.rules).toHaveLength(1);
    expect(state.parser.rules?.[0]).toEqual({
      id: expect.any(String),
      matches: { expr: 'true' },
      mutate: [
        legacyConfig.parser.mutate?.[0],
        {
          filter: { expr: 'temperature > 10' }
        }
      ],
      model: legacyConfig.parser.model
    });
    expect(state.parser.parse).toEqual(legacyConfig.parser.parse);
    expect(state.parser.global).toEqual(legacyConfig.parser.global);
    expect(state.input).toEqual(legacyConfig.input);
    expect(toBackendPayload(state, 'kafka')).toEqual({
      ...legacyConfig,
      parser: {
        global: legacyConfig.parser.global,
        parse: legacyConfig.parser.parse,
        rules: [
          {
            matches: { expr: 'true' },
            mutate: [
              legacyConfig.parser.mutate?.[0],
              {
                filter: { expr: 'temperature > 10' }
              }
            ],
            model: legacyConfig.parser.model
          }
        ]
      }
    });
  });

  it('enables rule blocks for Kafka and keeps MQTT on the legacy path', () => {
    expect(getTransformCapabilities('kafka')).toEqual({
      supportsRuleBlocks: true,
      supportsMultipleRules: true
    });
    expect(getTransformCapabilities('mqtt')).toEqual({
      supportsRuleBlocks: false,
      supportsMultipleRules: false
    });
  });

  it('preserves Kafka rule order during serialization', () => {
    const state = {
      parser: {
        parse: { value: { json: ['$.temp'] } },
        rules: [
          {
            id: 'rule-2',
            matches: { expr: 'topic == "b"' },
            mutate: [{ filter: { expr: 'b > 1' } }],
            model: { name: 'b', using: 'meters_b', tags: [], columns: ['value_b'] }
          },
          {
            id: 'rule-1',
            matches: { expr: 'topic == "a"' },
            mutate: [{ filter: { expr: 'a > 1' } }],
            model: { name: 'a', using: 'meters_a', tags: [], columns: ['value_a'] }
          }
        ]
      },
      input: [{ value: '{"temp":42}' }]
    };

    const payload = toBackendPayload(state, 'kafka');

    expect(payload.parser.rules?.map(rule => rule.matches)).toEqual([{ expr: 'topic == "b"' }, { expr: 'topic == "a"' }]);
  });

  it('validates nested mutate rules through the live checkParseData helper', () => {
    const invalidRuleConfig = {
      parser: {
        rules: [
          {
            matches: { expr: 'true' },
            mutate: [
              {
                extract: {
                  '': {
                    json: ['$.payload.temperature']
                  }
                }
              }
            ],
            model: {
              name: '${device}',
              using: 'meters',
              tags: ['site'],
              columns: ['temperature']
            }
          }
        ]
      }
    };

    expect(checkParseData(invalidRuleConfig)).toBe('datasource.transformer.extractrule.nofield');
  });

  it('keeps structured condition objects for Kafka rules', () => {
    const state = toRuleFormState(
      {
        parser: {
          rules: [
            {
              matches: { expr: 'topic == "meters"' },
              mutate: [{ filter: { expr: 'value > 1' } }],
              model: { name: 'meters', using: 'meters', tags: [], columns: ['value'] }
            }
          ]
        }
      },
      'kafka'
    );

    expect(state.parser.rules?.[0].matches).toEqual({ expr: 'topic == "meters"' });
    expect(state.parser.rules?.[0].mutate).toEqual([{ filter: { expr: 'value > 1' } }]);
    expect(toBackendPayload(state, 'kafka').parser.rules?.[0]).toMatchObject({
      matches: { expr: 'topic == "meters"' },
      mutate: [{ filter: { expr: 'value > 1' } }]
    });
  });

  it('preserves null_if_error in structured condition expressions', () => {
    const state = toRuleFormState(
      {
        parser: {
          rules: [
            {
              matches: { expr: 'topic == "meters"', null_if_error: true },
              mutate: [{ filter: { expr: 'value > 1', null_if_error: false } }],
              model: { name: 'meters', using: 'meters', tags: [], columns: ['value'] }
            }
          ]
        }
      },
      'kafka'
    );

    expect(state.parser.rules?.[0].matches).toEqual({
      expr: 'topic == "meters"',
      null_if_error: true
    });
    expect(state.parser.rules?.[0].mutate).toEqual([
      { filter: { expr: 'value > 1', null_if_error: false } }
    ]);
    expect(toBackendPayload(state, 'kafka').parser.rules?.[0]).toMatchObject({
      matches: { expr: 'topic == "meters"', null_if_error: true },
      mutate: [{ filter: { expr: 'value > 1', null_if_error: false } }]
    });
  });
});
